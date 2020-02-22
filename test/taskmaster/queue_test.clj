(ns taskmaster.queue-test
  (:require [taskmaster.dev.connection :as conn]
            [taskmaster.queue :as queue]
            [taskmaster.operation :as op]
            [clojure.test :refer :all]))

(def pg1 (atom nil))
(def p (atom nil))

(def q-ok (atom []))
(def q-fail (atom []))

(defn callback [{:keys [id queue-name payload] :as job}]
  (if (even? (:number payload))
    (do
      (swap! q-ok conj job)
      ::queue/ack)
    (do
      (swap! q-fail conj job)
      ::queue/reject)))

(def queue "test_tm")

(defn start-conn! []
  (reset! pg1 (.start (conn/make-one)))
  (op/create-jobs-table! @pg1))

(defn start-queue! []
  (reset! p
          (queue/start!
           (queue/create-worker-pool
            @pg1
            {:queue-name queue
             :concurrency 2
             :callback callback}))))

(defn cleanup! []
  (queue/stop! @p)
  (reset! q-ok [])
  (reset! q-fail [])
  (op/drop-jobs-table! @pg1)
  (.stop @pg1))

(use-fixtures :each (fn [t]
                      (try
                        (start-conn!)
                        (start-queue!)
                        (t)
                        (finally
                          (cleanup!)))))

(deftest it-does-basic-ops
  (testing "it pushes to the queue"
    (queue/put! @pg1 {:queue-name queue :payload {:number 2}})
    (queue/put! @pg1 {:queue-name queue :payload {:number 4}})
    (queue/put! @pg1 {:queue-name queue :payload {:number 6}})
    (queue/put! @pg1 {:queue-name queue :payload {:number 1}}))
  (Thread/sleep 1000)
  (testing "it picks up and processes jobs"
    (is (= 3 (count (-> q-ok deref))))
    (is (= [2 4 6] (mapv #(-> % :payload :number) @q-ok)))
    (is (= 1 (count @q-fail)))
    (is (= [1] (mapv #(-> % :payload :number) @q-fail)))
    (is (= {:count 1}
           (op/queue-size @pg1 {:queue-name queue})))))

(deftest resuming-lots-of-jobs
  (queue/stop! @p)
  (mapv #(queue/put! @pg1 {:queue-name (str queue "-l") :payload {:number 2 :batch %}})
        (range 0 100))
  (queue/start! @p)
  (is (= :x (queue/put! @pg1 {:queue-name (str queue "-l") :payload {:number 2}})))
  (loop [i 0]
    (when (and
            (not= (count @q-ok) 101)
            (< i 130))
      (Thread/sleep 100)
      (recur (inc i))))

  (is (= {:count 0} (op/queue-size @pg1 {:queue-name (str queue "-l")})))
  (is (= 101 (count @q-ok))))
