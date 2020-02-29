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
   :port-number  "5438"
   :maximum-pool-size 8
   :database-name "taskmaster"})


(defn make-one []
  (cp/create config))
