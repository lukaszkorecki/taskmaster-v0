(ns taskmaster.operation
  (:require
    [clojure.tools.logging :as log]
    [utility-belt.sql.conv]
    [utility-belt.sql.helpers :as sql]
    ;; load coercions
    [utility-belt.sql.model :as model])
  (:import
    (com.zaxxer.hikari
      HikariDataSource)
    (com.zaxxer.hikari.pool
      HikariProxyConnection)
    (org.postgresql
      PGConnection)
    (org.postgresql.core
      Notification)))


(declare ping*
         create-jobs-table*!
         drop-jobs-table*!
         unlock-dead-consumers*!
         put*!
         put-many*!
         get-by-ids*
         queue-stats*
         listen*
         find-jobs*
         lock*!
         unlock*!
         delete-jobs*!
         delete-all*!
         queue-size*
         setup-triggers*!)


(model/load-sql-file "taskmaster.sql" {:mode :kebab-maps})
(model/load-sql-file-vec-fns "taskmaster.sql")

(def ^{:dynamic true :doc "Default name of the table to store job records"} *job-table-name* "taskmaster_jobs")


(defn create-jobs-table!
  "Creates the jobs table, also will setup necessary triggers on the jobs table"
  [conn]
  (create-jobs-table*! conn {:table-name *job-table-name*})
  (setup-triggers*! conn {:table-name *job-table-name*}))


(defn drop-jobs-table!
  "Drop the jobs table, along with its triggers"
  [conn]
  (drop-jobs-table*! conn {:table-name *job-table-name*}))


(defn unlock-dead-consumers!
  "Free all locks, which are held by consumers which no longer exist (due to a crash etc)"
  [conn]
  (unlock-dead-consumers*! conn {:table-name *job-table-name*}))


(defn find-failed-jobs
  "Find all failed jobs in given queue"
  [conn {:keys [queue-name]}]
  (find-jobs* conn {:table-name *job-table-name* :queue-name queue-name :run-count 1}))


(defn put!
  "Schedule a job for execution"
  [conn {:keys [queue-name payload]}]
  (put*! conn {:table-name *job-table-name* :queue-name queue-name :payload payload}))


(defn lock!
  "Lock a table row using advisory locks and SKIP locked,
  returns the whole job row (with the ID, payload and so on"
  [conn {:keys [queue-name]}]
  (lock*! conn {:table-name *job-table-name*
                :table-name-id (str *job-table-name* ".id")
                :queue-name queue-name}))


(defn unlock!
  "Release the lock for given job ID"
  [conn {:keys [id]}]
  (unlock*! conn {:table-name *job-table-name* :id id}))


(defn delete-job!
  "Launches a missle 🚀"
  [conn {:keys [id]}]
  (delete-jobs*! conn {:table-name *job-table-name* :id [id]}))


(defn delete-all!
  "Launches a nuclear missle and nukes all of the jobs for given queue"
  [conn {:keys [queue-name]}]
  (delete-all*! conn {:table-name *job-table-name* :queue-name queue-name}))


(defn queue-size
  "How many jobs we have, for the whole queue - only pending ones."
  [conn {:keys [queue-name]}]
  (queue-size* conn {:table-name *job-table-name* :queue-name queue-name}))


(defn queue-stats
  "Return number of jobs per queue in the jobs table"
  [conn]
  (let [res (queue-stats* conn {:table-name *job-table-name*})]
    (->> res
         (group-by :queue-name)
         (map (fn [[gr res]]
                {:queue-name gr
                 :total (reduce + (map :count res))
                 :failed (or (:count (first (filter :is-failed res))) 0)
                 :pending (or (:count (first (remove :is-failed res))) 0)})))))


(defn requeue!
  "Requeues the job, without changing the payload and deletes old ones"
  [conn {:keys [id]}]
  {:pre [(every? number? id)]}
  (sql/with-transaction [tx conn]
    (let [jobs
          (->> (get-by-ids* tx {:table-name *job-table-name*
                                :min-run-count 1
                                :id id})
               (map #(vector (:queue-name %) (:payload %))))]
      (when (seq jobs)
        (let [new-ids (put-many*! tx {:table-name *job-table-name* :payloads jobs})]
          (delete-jobs*! conn {:table-name *job-table-name* :id id})

          new-ids)))))


(defn- get-notifications
  "LOW LEVEL BITS ALERT!
  Dig out all of the pending notifications from the current PG connection"
  [^PGConnection pg-conn]
  (when-not (.isClosed ^PGConnection pg-conn)
    (map #(.getParameter ^Notification %)
         (.getNotifications ^PGConnection pg-conn))))


(defn listen-and-notify
  "LOW LEVEL BITS ALERT!
  The core of the job listener for a given queue:
  - unpacks the raw connection from the connection pool
  - pings the trigger
  - pulls all pending jobs and passes them to the handler

  Notifications are polled at a 500ms interval, or can be adjusted via `interval-ms` argument
  `handler` receives multiple notification job payloads"
  [{:keys [datasource] :as _conn}
   {:keys [queue-name handler interval-ms]}]
  (try
    (let [raw-conn (.getConnection ^HikariDataSource datasource)
          pg-conn (.unwrap ^HikariProxyConnection  raw-conn PGConnection)]
      (ping* pg-conn)
      (listen* pg-conn {:queue-name queue-name})
      (while true
        (handler (get-notifications pg-conn))
        (Thread/sleep (or interval-ms 500))))
    (catch InterruptedException _
      ::interrupt) ; noop
    (catch Exception e
      (log/error e "Listen notify error: \n"))))


(defn wrap-handler
  "LOW LEVEL BITS ALERT!
  Wrap the handler function in such a way, that it deletes the jobs
  once they're processed successfully. All of the processing happens in a transaction"
  [conn {:keys [queue-name handler]}]
  (fn consumer-handler []
    (sql/with-transaction [tx conn]
      (let [jobs (lock! tx {:queue-name queue-name})]
        (log/debugf "jobs-count=%s" (count jobs))
        (mapv (fn run-job [{:keys [id] :as job}]
                (log/debugf "job-id=%s start" id)
                (let [res (handler job)]
                  (log/debugf "job-id=%s result=%s" id res)
                  ;; FIXME - raise if keyword is not qualified to taskmaster.operation?
                  (when (= ::ack res)
                    (let [del-res (delete-job! tx {:id id})]
                      (log/debugf "job-id=%s ack delete=%s" id del-res)))
                  (let [unlock-res (unlock! tx {:id id})]
                    (log/debugf "job-id=%s unlock %s" id unlock-res))))
              jobs)))))
