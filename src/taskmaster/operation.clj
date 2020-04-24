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
    PGConnection
    PGNotification)
   (org.postgresql.core
    Notification)))


(declare ping*
         create-jobs-table*!
         drop-jobs-table*!
         unlock-dead-consumers*!
         put*!
         lock*!
         unlock*!
         delete-job*!
         delete-all*!
         queue-size*)


(model/load-sql-file "taskmaster.sql" {:mode :kebab-maps})

(def ^:dynamic *job-table-name* "taskmaster_jobs")


(defn create-jobs-table! [conn]
  (create-jobs-table*! conn {:table-name *job-table-name*})
  (setup-triggers*! conn {:table-name *job-table-name*}))


(defn drop-jobs-table! [conn]
  (drop-jobs-table*! conn {:table-name *job-table-name*}))


(defn unlock-dead-consumers! [conn]
  (unlock-dead-consumers*! conn {:table-name *job-table-name*}))

(defn find-pending-jobs [conn {:keys [queue-name]}]
  (find-pending-jobs* conn {:table-name *job-table-name* :queue-name queue-name}))


(defn put! [conn {:keys [queue-name payload]}]
  (put*! conn {:table-name *job-table-name* :queue-name queue-name :payload payload}))


(defn lock! [conn {:keys [queue-name]}]
  (lock*! conn {:table-name *job-table-name*
                :table-name-id (str *job-table-name* ".id")
                :queue-name queue-name}))


(defn unlock! [conn {:keys [id]}]
  (unlock*! conn {:table-name *job-table-name* :id id}))


(defn delete-job! [conn {:keys [id]}]
  (delete-job*! conn {:table-name *job-table-name* :id id}))


(defn delete-all! [conn {:keys [queue-name]}]
  (delete-all*! conn {:table-name *job-table-name* :queue-name queue-name}))


(defn queue-size [conn {:keys [queue-name]}]
  (queue-size* conn {:table-name *job-table-name* :queue-name queue-name}))


(defn- get-notifications [^PGConnection pg-conn]
  (when-not (.isClosed ^PGConnection pg-conn)
    (map #(.getParameter ^Notification %)
         (.getNotifications ^PGConnection pg-conn))))

(defn listen-and-notify [{:keys [datasource] :as conn}
                         {:keys [queue-name handler on-error interval-ms] :as opts}]
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
  "Wrap the handler function in such a way, that it deletes the jobs
  once they're processed successfully"
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
