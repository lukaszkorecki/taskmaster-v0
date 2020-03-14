(ns taskmaster.dev.connection
  (:require
    [utility-belt.sql.component.connection-pool :as cp]))


(def config
  {:auto-commit true
   :pool-name  "taskmaster"
   :adapter "postgresql"
   :username "taskmaster"
   :password  "password"
   :server-name  (or (System/getenv "PG_HOST") "127.0.0.1")
   :port-number  (or (System/getenv "PG_PORT") "5438")
   :maximum-pool-size 8
   :database-name (or (System/getenv "PG_NAME") "taskmaster")})


(defn make-one []
  (cp/create config))
