(ns taskmaster.component
  (:require
    [clojure.tools.logging :as log]
    [taskmaster.queue :as queue]))

;; Doesn't depend on the lifecycle, e.g. is stateless really
;; so doesn't depend on the Component Lifecycle protocol
(defprotocol Publish
  :extend-via-metadata true
  (put! [this payload-map]))


(defrecord Publisher [db-conn]
  Publish
  (put! [this payload-map]
    (queue/put! db-conn payload-map)))


(defn create-publisher
  "Utility function, creates a publisher component"
  []
  (map->Publisher {}))


(defn create-consumer
  "Returns a consumer component - but only if com.stuartsierra.component is added as a dependency.
  Otherwise it will return an empty map.
  Depends on:
  - db-conn - as PG connection, backed by a HikariCP connection pool"
  [{:keys [handler concurrency queue-name] :as _config}]
  {:pre [(fn? handler)
         (pos? concurrency)
         (queue/valid-queue-name? queue-name)]}
  (with-meta {}
    {'com.stuartsierra.component/start (fn [this]
                                         (if (:consumer-pool this)
                                           this
                                           (let [_ (log/infof "creating consumer=%s concurrency=%s" queue-name concurrency)
                                                 _ (assert (:db-conn this) "db-conn is required!")
                                                 handler-with-dependencies (fn handler-with-dependencies [payload]
                                                                             (handler (assoc payload :component this)))
                                                 consumer-pool (queue/start! (:db-conn this) {:queue-name queue-name
                                                                                              :handler handler-with-dependencies
                                                                                              :concurrency concurrency})]
                                             (assoc this :consumer-pool consumer-pool))))
     'com.stuartsierra.component/stop (fn [this]
                                        (when (:conumser-ppol this)
                                          (log/warnf "stoppping consumer=%s" queue-name)
                                          (queue/stop! (:consumer-pool this))
                                          (assoc this :consumer-pool nil)))}))
