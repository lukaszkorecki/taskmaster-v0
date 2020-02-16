(ns taskmaster.operation
  (:require [utility-belt.sql.conv] ;; load coercions
            [utility-belt.sql.model :as model]
            [clojure.tools.logging :as log])
  (:import (org.postgresql PGConnection)))

(declare
  create-jobs-table*!
  unlock-dead-workers*!
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

(defn unlock-dead-workers! [conn]
  (unlock-dead-workers*! conn {:table-name *job-table-name*}))


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

(defn listen-and-notify [conn {:keys [queue-name callback on-error interval-ms] :as opts}]
  (try
    (while true
      (let [raw-conn (-> conn :datasource .getConnection)
            pg-conn (.unwrap raw-conn PGConnection)]
        (listen* raw-conn {:queue-name queue-name})
        (->> pg-conn
             .getNotifications
             (map #(.getParameter %))
             callback)
        (.close raw-conn)
        (Thread/sleep (or interval-ms 1000))))
    (catch Exception e
      (log/error "Listen notify error: \n" e)
      (if on-error
        (on-error {:queue-name queue-name :error e})
        (throw e))))
  (recur conn opts))
