(require '[taskmaster.queue :as queue]
         '[taskmaster.connection :as c]
         '[clojure.tools.logging :as log])


(def c1 (.start (c/make-one)))
(def c2 (.start (c/make-one)))


(def id-log (atom []))

(defn callback [job]
  (log/infof "got-job %s" job)
  (swap! id-log conj (:id job))
  (if (and (:some-number job) (even? (:some-number job)))
    :taskmaster.queue/ack
    :taskmaster.queue/reject))

(def work-pool (queue/create-worker-pool c1 {:queue-name "test_queue_1"
                                             :concurrency 2
                                             :callback (queue/with-worker-callback c2 {:queue-name "test_queue_1"
                                                                                       :callback callback})}))

(queue/start! work-pool)
(queue/stop! work-pool)

(queue/put! c2 {:queue-name "test_queue_1" :payload { :some-number 200 :msg :hello}})

(first (deref id-log))
(count (set (deref id-log)))

(reset! id-log [])
