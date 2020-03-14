(require '[taskmaster.dev.connection :as c]
         '[taskmaster.component :as com]
         '[clojure.tools.logging :as log]
         '[com.stuartsierra.component :as component]
         )

(def qs (atom []))
               (defn callback [{:keys [id queue-name payload component] :as job}]

  (log/infof "got-job t=%s q=%s %s" component queue-name payload)
  (swap! qs conj id)
  (log/info (count (set @qs)))
  (let [res   (if (and (:some-number payload) (even? (:some-number payload)))
                :taskmaster.operation/ack
                :taskmaster.operation/reject)]
    (log/info res)
    res))


(def system
  {:db-conn (c/make-one)
   :consumer (component/using
              (com/create-consumer {:queue-name "t3"
                                    :callback callback
                                    :concurrency 2})
              [:db-conn :some-thing]
              )
   :some-thing {:some :thing}
   :publisher (component/using
               (com/create-publisher)
               [:db-conn])})
(def SYS
  (component/start-system (component/map->SystemMap system)))
