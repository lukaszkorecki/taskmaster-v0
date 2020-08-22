(ns user ; scratch
  (:require
    [clojure.tools.logging :as log]
    [com.stuartsierra.component :as component]
    [taskmaster.component :as com]
    [taskmaster.operation :as op]
    [taskmaster.dev.connection :as c]))





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


(do ; comment
  (def SYS
    (component/start-system (component/map->SystemMap system)))
  (op/create-jobs-table! (:db-conn SYS))
  (com/put! (:publisher SYS) {:queue-name "t3" :payload {:some-number 2}})
  (component/stop SYS))


(def SYS
  (component/start-system (component/map->SystemMap system)))


(def PUBSYS (component/start-system (component/map->SystemMap system-publisher-only)))

(com/put! (:publisher PUBSYS) {:queue-name "t3" :payload {:some-number 1}})


(require '[taskmaster.operation] :reload)
(taskmaster.operation/queue-stats (:db-conn PUBSYS) #_ {:table-name "taskmaster_jobs"})

(utility-belt.sql.helpers/execute (:db-conn PUBSYS) ["select queue_name, count(run_count), run_count > 0 as is_failed from ? group by queue_name, run_count", "taskmaster_jobs"])


(component/stop SYS)


(R/refresh-all)
(component/stop PUBSYS)

(R/refresh-all)


(->> [{:queue-name "t3", :count 7, :is-failed true} {:queue-name "t3", :count 7, :is-failed false} {:queue-name "t1", :count 1, :is-failed false}]
     (group-by :queue-name)
     (map (fn [[gr res]]
            {:queue-name gr
             :total (reduce + (map :count res))
             :failed (or (:count (first (filter :is-failed res))) 0)
             :pending (or (:count (first (remove :is-failed res))) 0)}))
     )
