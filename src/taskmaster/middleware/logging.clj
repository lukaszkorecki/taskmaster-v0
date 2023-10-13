(ns taskmaster.middleware.logging
  (:require
    [clojure.tools.logging :as log]
    [taskmaster.operation :as op]))


(defn with-log
  "Logging middleware - will log when job procesing (as done by the `handler` argument) starts, finishes and
  how long it took"
  [handler]
  (fn logger [{:keys [id queue-name] :as job}]
    (log/infof "queue=%s id=%s status=start" queue-name id)
    (let [start-time ^Long (System/currentTimeMillis)
          res (handler job)
          run-time ^Long (- (System/currentTimeMillis) start-time)]
      (if (= ::op/ack res)
        (log/infof "queue=%s id=%s status=ack time=%s" queue-name id run-time)
        (log/errorf "queue=%s id=%s status=reject time=%s" queue-name id run-time))
      res)))
