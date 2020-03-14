(ns taskmaster.queue
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [taskmaster.operation :as op])
  (:import
   (java.io
    Closeable)
   (java.util.concurrent
    ConcurrentLinkedQueue)))


(defrecord Consumer [queue-name listener pool]
  Closeable
  (close [this]
    (log/warnf "listener=stop queue-name=%s" queue-name)
    (mapv #(.stop ^Thread %) pool)
    (future-cancel listener)))


(defn stop! [consumer]
  (.close ^Closeable consumer))


(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))


(defn start! [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (fn? callback)
         (valid-queue-name? queue-name)]}
  (let [_ (log/warnf "unlocking queue-name=%s"  queue-name)
        _ (op/unlock-dead-consumers! conn)
        queue (ConcurrentLinkedQueue.)
        conveyor (fn conveyor [notification] (mapv #(.offer ^ConcurrentLinkedQueue queue %) notification))
        _ (log/infof "pool=starting queue-name=%s concurrency=%s" queue-name concurrency)
        ;; main listener, will notify other threads in the pool when something happens
        ;; then they will wake up and use locking semantics to pull jobs from the queue table
        ;; and do whatever they must. Therefore at minimum the pool will have 2 threads per queue:
        ;; 1 listener
        ;; 1 (at least) consumer, receiving messages from the blocking queue
        ;; The pool is a simple collection of threads polling the shared Concurrentlinkedqueue
        on-error (fn [{:keys [queue-name error]}]
                   (log/errorf "%s fail" queue-name)
                   (log/error error))
        listener-thread (future-call #(op/listen-and-notify conn {:queue-name queue-name
                                                                  :callback conveyor
                                                                  :on-error on-error}))
        pool (future (mapv (fn [i]
                             (let [name (str "taskmaster-" queue-name "-" i)
                                   wrapped-callback (op/wrap-callback conn {:queue-name queue-name
                                                                            :callback callback})
                                   thr (Thread. (fn processor []
                                                  (log/infof "procesor=start name=%s" name)
                                                  (while true
                                                    (when-let [el (.poll ^ConcurrentLinkedQueue queue)]
                                                      (wrapped-callback))
                                                    (Thread/sleep 25)))
                                                name)]
                               (.start ^Thread thr)
                               thr))
                           (vec (range 0 concurrency))))]
    ;; catchup on pending jobs
    (.offer queue "ping")
    ;; return the consumer record, along with the "thread pool" and the listener
    (->Consumer queue-name listener-thread @pool)))


(defn put! [conn {:keys [queue-name payload]}]
  {:pre [(valid-queue-name? queue-name)
         (map? payload)]}
  (log/debugf "put queue=%s job=%s" queue-name payload)
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
