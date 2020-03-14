(ns taskmaster.component
  (:require
   [clojure.tools.logging :as log]
   [taskmaster.queue :as queue]))

;; Doesn't depend on the lifecycle, e.g. is stateless really
;; so doesn't depend on the Component Lifecycle protocol
(defprotocol Publish
  (put! [this payload-map]))


(defrecord Publisher [db-conn]
  Publish
  (put! [this payload-map]
    (queue/put! db-conn payload-map)))


(def has-component?
  (try
    (require '[com.stuartsierra.component :as component])
    true
    (catch Exception _
      (log/error "component not found")
      false)))


(defn create-publisher []
  (map->Publisher {}))


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
        (if (:consumer-pool this)
          this
          (let [callback-with-dependencies (fn callback-with-dependencies [payload]
                                             (callback (assoc payload :component this)))
                consumer-pool (queue/start! db-conn {:queue-name queue-name
                                                     :callback callback-with-dependencies
                                                     :concurrency concurrency})]
            (assoc this :consumer-pool consumer-pool))))
      (stop [this]
        (when (:conumser-ppol this)
          (queue/stop! consumer-pool))
        (assoc this :consumer-pool nil)))

    (defn create-consumer [config]
      (map->Consumer config)))

  (log/warn "Component not found, ignoring"))
