(ns taskmaster.queue
  (:require [clojure.tools.logging :as log]
            [utility-belt.sql.helpers :as sql]
            [clojure.string :as str]
            [taskmaster.operation :as op]))

(defn with-worker-callback [conn {:keys [queue-name callback]}]
  (fn worker-callback [notification]
    (if (empty? notification)
      (log/debug "state=waiting")
      (sql/with-transaction [tx conn]
        (let [jobs (op/lock! tx {:queue-name queue-name})]
          (mapv (fn run-job [{:keys [id] :as job}]
                  (log/infof "job-id=%s start" id)
                  (let [res (callback job)]
                    (log/infof "job-id=%s result=%s" id res)
                    (when (= ::ack res)
                      (let [del-res (op/delete-job! tx {:id id})]
                        (log/debugf "job-id=%s ack delete=%s" id del-res)))
                    (let [unlock-res (op/unlock! tx {:id id})]
                      (log/debugf "job-id=%s unlock %s" id unlock-res)))
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
          (let [cb (with-worker-callback conn {:queue-name queue-name
                                               :callback callback})]
        (create-worker-thread conn {:queue-name queue-name
                                    :callback cb
                                    :name (str "taskmaster-" queue-name "-" i)})))
        (range 0 concurrency)))

(defn start! [workers]
  (mapv start-worker! workers))

(defn stop! [workers]
  (mapv stop-worker! workers))


(defn put! [conn {:keys [queue-name payload]}]
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
