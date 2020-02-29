(ns taskmaster.queue
  (:require
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [taskmaster.operation :as op])
  (:import
   (java.util.concurrent
    ConcurrentLinkedQueue)))

(defprotocol Closable
  (close [this]))

(defrecord Consumer [queue-name listener pool]
  Closable
  (close [this]
    (log/infof "listener=stop queue-name=%s" queue-name)
    (mapv #(.stop ^Thread %) pool)
    (future-cancel listener)))

(defn stop! [consumer]
  (close consumer))

(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))

(defn start! [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (fn? callback)
         (valid-queue-name? queue-name)]}
  (let [_ (log/infof "unlocking=%s queue-name=%s" (op/unlock-dead-consumers! conn) queue-name)
        ;; pool (Executors/newFixedThreadPool concurrency)
        queue (ConcurrentLinkedQueue.)
        conveyor (fn conveyor [notification] (mapv #(.offer ^Concurrentlinkedqueue queue %) notification))
        _ (log/infof "pool=starting queue-name=%s concurrency=%s" queue-name concurrency)
        ;; main listener, will notify other threads in the pool when something happens
        ;; then they will wake up and use locking semantics to pull jobs from the queue table
        ;; and do whatever they must. Therefore at minimum the pool will have 2 threads per queue:
        ;; 1 listener
        ;; 1 (at least) consumer, receiving messages from the blocking queue
        ;; The pool is a simple collection of threads polling the shared Concurrentlinkedqueue
        listener-thread (future-call #(op/listen-and-notify conn {:queue-name queue-name :callback conveyor}))
        pool (future (mapv (fn [i]
                             (let [name (str "taskmaster-" queue-name "-" i)
                                   _ (log/info name)
                                   wrapped-callback (op/wrap-callback conn {:queue-name queue-name :callback callback})
                                   thr (Thread. (fn processor []
                                                  (log/info "procesor=start name=%s" name)
                                                  (while true
                                                    (when-let [el (.poll ^Concurrentlinkedqueue queue)]
                                                      (wrapped-callback))
                                                    (Thread/sleep 25)))
                                                name)]
                               (.start ^Thread thr)
                               thr))
                           (vec (range 0 concurrency))))]
    (->Consumer queue-name listener-thread @pool)))

(defn put! [conn {:keys [queue-name payload]}]
  {:pre [(valid-queue-name? queue-name)]}
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
