(ns user
  (:require
    [:reload]
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [taskmaster.component :as com]
    [taskmaster.dev.connection :as c]
    [taskmaster.operation :as op]))


(def ^{:doc "job log"} qs (atom []))


(defn handler
  "Sample handler: if theres a number in :some-number key in the payload
  and its even, ACK the job, otherwise fail it"
  [{:keys [id queue-name payload component] :as _job}]
  (log/infof "got-job t=%s q=%s %s" component queue-name payload)
  (swap! qs conj id)
  (log/info (count (set @qs)))
  (let [res   (if (and (:some-number payload) (even? (:some-number payload)))
                :taskmaster.operation/ack
                :taskmaster.operation/reject)]
    (log/info res)
    res))


(def ^{:doc "sample system"} system
  {:db-conn (c/make-one)
   :consumer (component/using
               (com/create-consumer {:queue-name "t3"
                                     :handler handler
                                     :concurrency 2})
               [:db-conn :some-thing])
   :some-thing {:some :thing}
   :publisher (component/using
                (com/create-publisher)
                [:db-conn])})


(def system-publisher-only
  {:db-conn (c/make-one)
   #_ :consumer  #_ (component/using
               (com/create-consumer {:queue-name "t3"
                                     :handler handler
                                     :concurrency 2})
               [:db-conn :some-thing])
   :publisher (component/using
                (com/create-publisher)
                [:db-conn])})


(def SYS
  (component/start-system (component/map->SystemMap system)))


(def PUBSYS (component/start-system (component/map->SystemMap system-publisher-only)))

(com/put! (:publisher SYS) {:queue-name "t3" :payload {:some-number 1}})


(taskmaster.operation/requeue! (:db-conn SYS)
                               {:queue-name "t3" :id [46]})
