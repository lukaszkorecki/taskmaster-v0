(ns taskmaster.queue
  (:require [clojure.tools.logging :as log]
            [clojure.string :as str]
            [taskmaster.operation :as op])
  (:import (java.util.concurrent Executors TimeUnit ArrayBlockingQueue ConcurrentLinkedQueue)
           (java.util ArrayList)))



(defprotocol Closable
  (close [this]))

(defrecord Worker [queue-name listener pool]
  Closable
  (close [this]
    (log/infof "listener=stop queue-name=%s" queue-name)
    (mapv #(.stop %) pool)
    (future-cancel listener)))


(defn stop! [worker]
  (close worker))

(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))

(defn start! [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (fn? callback)
         (valid-queue-name? queue-name)]}
  (let [_ (log/infof "unlocking=%s queue-name=%s" (op/unlock-dead-workers! conn) queue-name)
        ;; pool (Executors/newFixedThreadPool concurrency)
        queue (ConcurrentLinkedQueue.)
        conveyor (fn conveyor [notification]
                   (mapv #(.offer queue %) notification))
        listener (fn listener [processor]
                   (log/infof "listener=start queue=%s" queue-name)
                   (while true
                     (when-let [el (.poll queue)]
                       (log/info "got something" el)
                       (processor))
                     (Thread/sleep 10)
                     ))
        _ (log/infof "pool=starting queue-name=%s concurrency=%s" queue-name concurrency)
        ;; main listener, will notify other threads in the pool when something happens
        ;; then they will wake up and use locking semantics to pull jobs from the queue table
        ;; and do whatever they must. Therefore at minimum the pool will have 2 threads per queue:
        ;; 1 listener
        ;; 1 (at least) consumer, receiving messages from the blocking queue
        listener-thread (future-call #(op/listen-and-notify conn {:queue-name queue-name :callback conveyor}))
    pool (future (mapv (fn [i]
            (let [name (str "taskmaster-" queue-name "-" i)
                  _ (log/info name)
                  processor (listener (op/wrap-callback conn {:queue-name queue-name :callback callback}))
                  thr (Thread. processor name)]
              (log/infof "starging %s %s %s" name (.start thr) thr)
              thr))

               (vec (range 0 concurrency))))

]
    (->Worker queue-name listener-thread @pool)))

(defn put! [conn {:keys [queue-name payload]}]
  {:pre [(valid-queue-name? queue-name)]}
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
