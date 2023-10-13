(require '[taskmaster.queue :as queue]
         '[taskmaster.dev.connection :as c]
         '[clojure.string]
         '[clojure.tools.logging :as log] :reload)


(def c1 (.start (c/make-one)))


(let [args (mapv #(Integer/parseInt %) (clojure.string/split (System/getenv "PAYLOAD") #","))]
  (log/infof "n: %s" (count args))
  (mapv (fn [arg]

          (log/info "pushing " arg)
          (queue/put! c1 {:queue-name "test_queue_1" :payload {:some-number arg :msg :hello}})) args))


(.stop c1)
