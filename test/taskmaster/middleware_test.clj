(ns taskmaster.middleware-test
  (:require
    [clojure.test :refer [deftest is]]
    [clojure.tools.logging.test :as logging.test]
    [taskmaster.middleware.error]
    [taskmaster.middleware.logging]))


(deftest error-handling
  (logging.test/with-log
    (let [xer (-> (fn [a] (throw (ex-info "fail"  {:a a})))
                  taskmaster.middleware.error/with-error
                  taskmaster.middleware.logging/with-log)]
      (is (= :taskmaster.operation/reject
             (xer  {:id "test-id" :queue-name "test" :payload "lol"})))
      (logging.test/logged? 'taskmaster.middleware.error :error #"id=test-id queue-name=test data=")
      (is (= [{:logger-ns "taskmaster.middleware.logging" :message "queue=test id=test-id status=start"}
              {:logger-ns "taskmaster.middleware.error" :message (str "queue=test id=test-id status=failure data=" {:id "test-id" :queue-name "test" :payload "lol"})}
              #_ {:logger-ns "taskmaster.middleware.error" :message "queue=test id=test-id status=reject time=4"}]
             (->> (logging.test/the-log)
                  (mapv #(select-keys % [:logger-ns :message]))
                  (mapv #(update % :logger-ns str))
                  (take 2) ; drop last!
                  )))
      (is (re-find #"queue=test id=test-id status=reject time=\d"
                   (->> (logging.test/the-log)
                        (mapv #(select-keys % [:logger-ns :message]))
                        (mapv #(update % :logger-ns str))
                        last
                        :message))))))
