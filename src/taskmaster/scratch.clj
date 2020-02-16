(require '[taskmaster.scratch :as s]
         '[clojure.tools.logging :as log]
         '[utility-belt.sql.helpers :as sql]
         '[taskmaster.core :as q] :reload)

(def c1 (.start (s/make-conn)))
(def c2 (.start (s/make-conn)))


(q/create-jobs-table! c1)

(def listener
  (Thread.
   (fn []
     (q/listen c1 {:queue-name "test_queue"
                   :callback (fn [args]
                               (log/info (q/queue-size c2 {:queue-name "test_queue"}))
                               (if (empty? args)
                                 (log/info "no jobs")

                                 (let [jobs (q/lock! c2 {:queue-name "test_queue"})]
                                   (mapv (fn [job]
                                           (log/infof "job %s" job)
                                           (q/delete-job! c2 {:id (:id job)})
                                           (q/unlock! c2 {:id (:id job)})) jobs))))}))))



(def listener2
  (Thread.
   (fn []
     (q/listen c1 {:queue-name "test_queue"
                   :callback (fn [args]
                               (log/info (q/queue-size c2 {:queue-name "test_queue"}))
                               (if (empty? args)
                                 (log/info "no jobs")

                                 (let [jobs (q/lock! c2 {:queue-name "test_queue"})]
                                   (mapv (fn [job]
                                           (log/infof "job %s" job)
                                           (q/delete-job! c2 {:id (:id job)})
                                           (q/unlock! c2 {:id (:id job)})) jobs))))}))))

(.stop listener)
(q/put! c2 {:queue-name "test_queue" :payload {:hi :there :time (str (java.time.LocalDateTime/now))}})
(.start listener)
(.start listener2)

(.stop listener2)
