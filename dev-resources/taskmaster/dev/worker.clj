(require '[taskmaster.queue :as queue]
         '[taskmaster.dev.connection :as c]
         '[utility-belt.lifecycle :as lc]
         '[clojure.tools.logging :as log] :reload)


(def c1 (.start (c/make-one)))

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

(def work-pool (queue/start!
                c1
                {:queue-name "test_queue_1"
                 :concurrency 8
                 :callback callback}))

(lc/register-shutdown-hook :stop-worker #(queue/stop! work-pool))
(lc/install-shutdown-hooks!)
