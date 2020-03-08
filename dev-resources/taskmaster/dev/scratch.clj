(require '[taskmaster.dev.connection :as c]
         '[taskmaster.queue :as queue]
         '[clojure.tools.logging :as log]
         )

(def qs (atom []))

(defn callback [{:keys [id queue-name payload] :as job}]
  (log/infof "got-job t=%s q=%s %s" (.getName (Thread/currentThread)) queue-name payload)
  (swap! qs conj id)
  (log/info (count (set @qs)))
  (let [res   (if (and (:some-number payload) (even? (:some-number payload)))
                :taskmaster.operation/ack
                :taskmaster.operation/reject)]
    (log/info res)
    res))

(def c (.start (c/make-one)))

(def w
  (queue/start! c {:queue-name "t1"
                   :concurrency 2
                   :callback callback}))



(def w2
  (queue/start! c {:queue-name "t2" :concurrency 3 :callback callback}))

(queue/put! c {:queue-name "t1" :payload {:some-number 2 :msg "hi"}})

(set (deref qs))
(queue/stop! w2)
(queue/stop! w)
