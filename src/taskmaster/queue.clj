(ns taskmaster.queue
  (:require [clojure.tools.logging :as log]
            [utility-belt.sql.helpers :as sql]
            [clojure.string :as str]
            [taskmaster.operation :as op])
  (:import (java.util.concurrent Executors TimeUnit ArrayBlockingQueue)
           (java.util ArrayList)))

(defn- valid-queue-name?
  "Ensure there is a queue name and that it doesn't contain . in the name"
  [q]
  (and (not (str/blank? q))
       (not (re-find #"\." q))))

(defn start! [conn {:keys [queue-name callback concurrency]}]
  {:pre [(pos? concurrency)
         (fn? callback)
         (valid-queue-name? queue-name)]}
  (let [pool (Executors/newFixedThreadPool concurrency)
        queue (ArrayBlockingQueue. 10) ; max in-flight messages
        conveyor (fn conveyor [notification]
                   (log/infof "conv=%s" (vec notification))
                   (mapv #(.offer queue %) notification))
        listener (fn listener [processor]
                   (log/info "listener hi")
                   (let [a (ArrayList.)]
                     (while true
                       (.drainTo queue a) ;; will block if queue is empty
                       (mapv (fn [i]
                               (log/infof "in-listener %s" i)
                               (processor)) (seq a))
                       (.clear a))))]

    (log/infof "starting consumer=%s" queue-name)
    ;; main listener, will notify other threads in the pool when something happens
    ;; then they will wake up and use locking semantics to pull jobs from the queue table
    ;; and do whatever they must. Therefore at minimum the pool will have 2 threads
    (.submit pool (Thread. #(op/listen-and-notify conn {:queue-name queue-name :callback conveyor})
                           (str queue-name "_listener")))
    (mapv (fn [i]
            (let [name (str "taskmaster-" queue-name "-" i)
                  processor (listener (op/wrap-callback conn {:queue-name queue-name
                                                              :callback callback}))]
              (log/infof "starting thread=%s" name)
              (.submit pool (Thread. processor name))))
          (range 0 concurrency))
    pool))

(defn stop! [pool]
  (log/infof "pool=%s stopping" pool)
  (.awaitTermination pool 5 TimeUnit/SECONDS)
  (.shutdownNow pool))

(defn put! [conn {:keys [queue-name payload]}]
  {:pre [(valid-queue-name? queue-name)]}
  (op/put! conn {:queue-name queue-name
                 :payload payload}))
