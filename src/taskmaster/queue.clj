(ns taskmaster.queue
  (:require [clojure.tools.logging :as log]
            [utility-belt.sql.helpers :as sql]
            [clojure.string :as str]
            [taskmaster.operation :as op]))

(defn with-worker-callback [conn {:keys [queue-name callback]}]
  (fn worker-callback [notification]
    (if (empty? notification)
      (log/debug "state=noop")
      (sql/with-transaction [tx conn]
        (let [jobs (op/lock! tx {:queue-name queue-name})]
          (mapv (fn run-job [{:keys [id] :as job}]
                  (log/debugf "job-id=%s start" id)
                  (when (= ::ack (callback job))
                    (log/debugf "job-id=%s ack" id)
                    (op/delete-job! tx {:id id}))
                  (log/debugf "job-id=%s unlock" id)
                  (op/unlock! tx {:id id}))
                jobs))))))


(defn create-worker-thread [conn {:keys [name queue-name callback]}]
  (Thread.
   #(op/listen-and-notify conn {:queue-name queue-name
                                :callback callback})
   name))

(defn- start-worker! [worker-thread]
  (.start ^Thread worker-thread))

(defn- stop-worker! [worker-thread]
  (.stop ^Thread worker-thread))

(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))

(defn create-worker-pool [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (valid-queue-name? queue-name)]}
  (mapv (fn [i]
        (create-worker-thread conn {:queue-name queue-name
                               :callback callback
                               :name (str "taskmaster-" queue-name "-" i)}))
        (range 0 concurrency)))

(defn start! [workers]
  (mapv start-worker! workers))

(defn stop! [workers]
  (mapv stop-worker! workers))


(defn put! [conn {:keys [queue-name payload]}]
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
