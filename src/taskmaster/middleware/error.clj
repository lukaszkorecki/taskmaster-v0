(ns taskmaster.middleware.error
  (:require [taskmaster.operation :as op]
   [clojure.tools.logging :as log]))

(defn with-error [handler]
  (fn error-catcher [{:keys [id queue-name] :as job}]
    (try
      (handler job)
      (catch Exception e
        (log/errorf e "queue=%s id=%s status=failure data=%s" queue-name id (dissoc job :component))
        ::op/reject))))
