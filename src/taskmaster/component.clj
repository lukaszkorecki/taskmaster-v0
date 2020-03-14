(ns taskmaster.component
  (:require
    [clojure.tools.logging :as log]
    [taskmaster.queue :as queue]))


(def has-component?
  (try
    (require '[com.stuartsierra.component :as component])
    true
    (catch Exception _
      (log/error "component not found")
      false)))


(if has-component?
  (do
    (defrecord Consumer
      [queue-name config concurrency callback
     ;; dependencies
       db-conn
     ;; internal state
       consumer-pool]
      component/Lifecycle
      (start [this]
        (let [callback-with-dependencies (fn callback-with-dependencies [payload]
                                           (callback (assoc payload :component this)))
              consumer-pool (queue/start! db-conn {:queue-name queue-name
                                                   :callback callback-with-dependencies
                                                   :concurrency concurrency})]
          (assoc this :consumer-pool consumer-pool)))
      (stop [this]
        (stop! consumer-pool)
        (assoc this :consumer-pool nil)))

    (defprotocol Publish
      (put! [this payload-map] "Put a message on a queue"))

    (defrecord Publisher [db-conn]
               (put! [this payload-map]
                 (queue/put! db-conn payload-map)))


    (defn create-consumer [config]
      (map->Consumer config))

    (defn create-publisher []
      (map->Publisher {})))

  (log/warn "Component not found, ignoring"))
