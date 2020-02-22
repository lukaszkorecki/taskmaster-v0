(require '[taskmaster.queue :as queue]
         '[taskmaster.dev.connection :as c]
         '[utility-belt.lifecycle :as lc]
         '[clojure.tools.logging :as log] :reload)


(def c1 (.start (c/make-one)))

(defn callback [{:keys [id queue-name payload] :as job}]
  (log/infof "got-job q=%s %s" queue-name id)
  (let [res   (if (and (:some-number payload) (even? (:some-number payload)))
                :taskmaster.queue/ack
                :taskmaster.queue/reject)]
    (log/info res)
    res))

(def work-pool (queue/create-worker-pool
                c1
                {:queue-name "test_queue_1"
                 :concurrency 2
                 :callback callback}))





(lc/register-shutdown-hook :stop-worker #(queue/stop! work-pool))
(lc/install-shutdown-hooks!)


(queue/start! work-pool)
