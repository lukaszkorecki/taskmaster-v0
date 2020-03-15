(ns taskmaster.middleware.logging
  (:require [taskmaster.operation :as op]
   [clojure.tools.logging :as log]))

(defn with-log [handler]
  (fn logger [{:keys [id queue-name] :as job}]
    (log/infof "queue=%s id=%s status=start" queue-name id)
    (let [start-time ^Long (System/currentTimeMillis)
          res (handler job)
          run-time ^Long (- (System/currentTimeMillis) start-time)]
      (if (= ::op/ack res)
        (log/infof "queue=%s id=%s status=ack time=%s" queue-name id run-time)
        (log/errorf "queue=%s id=%s status=reject time=%s" queue-name id run-time))
      res)))
