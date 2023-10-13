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


(defn stop!
  "Stops the consumer, publishers are unstoppable"
  [consumer]
  (.close ^Closeable consumer))


(defn valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))


(defn start!
  "Core job consuming and processing logic.
  Resturns a Consumer record ,which holds the following
  - listener thread which pulls notifications from PG, and then finds next suitable record (or records) to process
    for given `queue-name` option
  - a ConcurrentLinkedQueue instance which is used as an internal queue representation to
    farily distribute individual payloads to multiple consumer threads (number is controlled by `concurrency` option
  - each job is processed by a `handler` function, which receives a map of `{id payload queue-name}`
  Consumer can be closed with `with-close` etc"
  [conn {:keys [queue-name handler concurrency]}]
  {:pre [(pos? concurrency)
         (fn? handler)
         (valid-queue-name? queue-name)]}
  (let [_ (log/warnf "unlocking queue-name=%s"  queue-name)
        _ (op/unlock-dead-consumers! conn)
        queue (ConcurrentLinkedQueue.)
        conveyor (fn conveyor [notification] (mapv #(.offer ^ConcurrentLinkedQueue queue %) notification))
        _ (log/infof "pool=starting queue-name=%s concurrency=%s" queue-name concurrency)
        ;; main listener, will notify other threads in the pool when something happens
        ;; then they will wake up and use locking semantics to pull jobs from the queue table
        ;; and do whatever they must. Therefore at minimum the Consumer record instance will
        ;; have 2 threads per queue:
        ;; 1 listener
        ;; 1 (at least) consumer, receiving messages from the blocking queue
        ;; Note: it's not a "real" thread pool, like j.u.c.ScheduledExecutorThreadPool - just a collection of
        ;; threads, nothing more, nothing less
        on-error (fn [{:keys [queue-name error]}]
                   (log/errorf "%s fail" queue-name)
                   (log/error error))
        listener-thread (future-call #(op/listen-and-notify conn {:queue-name queue-name
                                                                  :handler conveyor
                                                                  :on-error on-error}))
        pool (future (mapv (fn [i]
                             (let [name (str "taskmaster-" queue-name "-" i)
                                   wrapped-handler (op/wrap-handler conn {:queue-name queue-name
                                                                          :handler handler})
                                   thr (Thread. (fn processor []
                                                  (log/infof "procesor=start name=%s" name)
                                                  (while true
                                                    (when-let [_item (.poll ^ConcurrentLinkedQueue queue)]
                                                      (wrapped-handler))
                                                    (Thread/sleep 25)))
                                                name)]
                               (.start ^Thread thr)
                               thr))
                           (vec (range 0 concurrency))))]
    ;; catchup on pending jobs
    (.offer queue "ping")
    ;; return the consumer record, along with the "thread pool" and the listener
    (->Consumer queue-name listener-thread @pool)))


(defn put!
  "Put a job on a queue - payload has to be JSON-able, or a string"
  [conn {:keys [queue-name payload]}]
  {:pre [(valid-queue-name? queue-name)
         (map? payload)]}
  (log/debugf "put queue=%s job=%s" queue-name payload)
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
