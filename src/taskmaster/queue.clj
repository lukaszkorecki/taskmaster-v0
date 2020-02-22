(ns taskmaster.queue
  (:require [clojure.tools.logging :as log]
            [utility-belt.sql.helpers :as sql]
            [clojure.string :as str]
            [taskmaster.operation :as op])
  (:import (java.util.concurrent Executors TimeUnit)))

(defn with-worker-callback
  "Wrap the callback function in such a way, that it deletes the jobs
  once they're processed successfully"
  [conn {:keys [queue-name callback]}]
  (fn worker-callback [notification]
    (if (empty? notification)
      (log/debug "state=waiting")
      (sql/with-transaction [tx conn]
        (let [jobs (op/lock! tx {:queue-name queue-name})]
          (log/debug "notification=%s" notification)
          (mapv (fn run-job [{:keys [id] :as job}]
                  (log/infof "job-id=%s start" id)
                  (let [res (callback job)]
                    (log/infof "job-id=%s result=%s" id res)
                    (when (= ::ack res)
                      (let [del-res (op/delete-job! tx {:id id})]
                        (log/debugf "job-id=%s ack delete=%s" id del-res)))
                    (let [unlock-res (op/unlock! tx {:id id})]
                      (log/debugf "job-id=%s unlock %s" id unlock-res))))
                jobs))))))


(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))

(defn start! [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (fn? callback)
         (valid-queue-name? queue-name)]}
  (let [pool (Executors/newFixedThreadPool concurrency)]
    (log/info "starting queue=%s" queue-name)
  (mapv (fn [i]
          (let [name (str "taskmaster-" queue-name "-" i)
                cb (with-worker-callback conn {:queue-name queue-name
                                               :callback callback})]
            (log/info "thread=%s" name)
            (.submit pool ^Runnable #(op/listen-and-notify conn {:queue-name queue-name :callback cb}))))
        (range 0 concurrency))
  pool))

(defn stop! [pool]
  (log/infof "pool=%s stopping" pool)
  (.awaitTermination pool 5 TimeUnit/SECONDS)
  (.shutdownNow pool))

(defn put! [conn {:keys [queue-name payload]}]
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
