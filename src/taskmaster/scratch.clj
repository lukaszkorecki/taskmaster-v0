(require '[taskmaster.queue :as queue]
         '[taskmaster.connection :as c]
         '[clojure.tools.logging :as log] :reload)


(def c1 (.start (c/make-one)))

(.stop c2)
(def c2 (.start (c/make-one)))


(def id-log (atom []))

(defn callback [job]
  (log/infof "got-job %s" job)
  (swap! id-log conj (:id job))
  (if (and (:some-number (:payload job)) (even? (:some-number (:payload job))))
    :taskmaster.queue/ack
    :taskmaster.queue/reject))

(def work-pool (queue/create-worker-pool
                c1
                {:queue-name "test_queue_1"
                 :concurrency 2
                 :callback (queue/with-worker-callback c2 {:queue-name "test_queue_1"
                                                           :callback callback})}))

(def work-pool-2 (queue/create-worker-pool
                c1
                {:queue-name "test_queue_2"
                 :concurrency 1
                 :callback (queue/with-worker-callback c2 {:queue-name "test_queue_2"
                                                           :callback callback})}))

(queue/start! work-pool)
(queue/start! work-pool-2)
(queue/stop! work-pool)
(queue/stop! work-pool-2)

(queue/put! c2 {:queue-name "test_queue_1" :payload { :some-number 200 :msg :hello}})
(queue/put! c2 {:queue-name "test_queue_2" :payload { :some-number 400 :msg :hello}})

(first (deref id-log))
(count (set (deref id-log)))

(reset! id-log [])
