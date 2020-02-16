(require '[taskmaster.connection :as c]
         '[clojure.tools.logging :as log]
         '[utility-belt.sql.helpers :as sql]
         '[taskmaster.core :as q] :reload)

(def c1 (.start (c/make-one)))
(def c2 (.start (c/make-one)))


(q/create-jobs-table! c1)

(defn notify-callback [args]
  (log/info (q/queue-size c2 {:queue-name "test_queue"}))
  (if (empty? args)
    (log/warn "no jobs")
    (let [jobs (q/lock! c2 {:queue-name "test_queue"})]
      (mapv (fn [job]
              (log/infof "job %s" (:id job))
              (q/delete-job! c2 {:id (:id job)})
              (q/unlock! c2 {:id (:id job)})) jobs))))

(def listener
  (Thread.
   (fn []
     (q/listen c1 {:queue-name "test_queue"
                   :callback notify-callback}))
   "listener-1"))



(def listener2
  (Thread.
   (fn []
     (q/listen c1 {:queue-name "test_queue"
                   :callback notify-callback }))
   "listener-2"
   ))

(.stop listener)
(.stop listener2)
(q/put! c2 {:queue-name "test_queue" :payload {:hi :there :time (str (java.time.LocalDateTime/now))}})
(.start listener)
(.start listener2)

(.stop listener2)


(q/delete-all! c2 {:queue-name "test_queue"})
