(ns taskmaster.queue-test
  (:require [taskmaster.dev.connection :as conn]
            [taskmaster.queue :as queue]
            [taskmaster.operation :as op]
            [clojure.tools.logging :as log]
            [clojure.test :refer :all]))

(def pg1 (atom nil))
(def pg2 (atom nil))
(def p (atom nil))

(def q-ok (atom []))
(def q-fail (atom []))

(def all-jobs (atom []))

(defn callback [{:keys [payload] :as job}]
  (swap! all-jobs conj job)
  (if (even? (:number payload))
    (do
      (swap! q-ok conj job)
      :taskmaster.operation/ack)
    (do
      (swap! q-fail conj job)
      :taskmaster.operation/reject)))

(def queue "test_tm")

(defn start-conn! []
  (reset! pg1 (.start (conn/make-one)))
  (reset! pg2 (.start (conn/make-one)))
  (op/create-jobs-table! @pg1))

(defn start-queue! [queue-name]
  (reset! p
          (queue/start!
           @pg2
           {:queue-name queue-name
            :concurrency 2
            :callback callback})))

(defn cleanup! []
  (when-let [p @p]
    (queue/stop! p))
  (reset! q-ok [])
  (reset! q-fail [])
  (reset! all-jobs [])
(log/warn  (op/drop-jobs-table! @pg1))
  (.stop @pg1)
  (.stop @pg2))

(use-fixtures :each (fn [t]
                      (with-bindings {#'taskmaster.operation/*job-table-name* "taskmaster_jobs_test"}
                        (try
                          (start-conn!)
                          (t)
                          (cleanup!)
                          (catch Exception e

                            (cleanup!)
                            (throw e)
                            )))))

(deftest it-does-basic-ops

  (testing "it pushes to the queue"
    (is (= :x (queue/put! @pg1 {:queue-name queue :payload {:number 2}})))
  (start-queue! queue)
    (queue/put! @pg1 {:queue-name queue :payload {:number 4}})
    (queue/put! @pg1 {:queue-name queue :payload {:number 6}})
    (queue/put! @pg1 {:queue-name queue :payload {:number 1}}))
  (Thread/sleep 3300)
  (testing "it picks up and processes jobs"
    (testing "success jobs"
      (is (= 3 (count (-> q-ok deref))))
      (is (= [2 4 6] (mapv #(-> % :payload :number) @q-ok))))
    (testing "failed jobs"
      (is (= 1 (count @q-fail)))
      (is (= [1] (mapv #(-> % :payload :number) @q-fail)))
      (is (= {:count 1}
             (op/queue-size @pg1 {:queue-name queue})

             ))

      (is (= :x @all-jobs))
      )))

(deftest resuming-lots-of-jobs
  (let [queue  (str queue "_large")]
    (mapv #(queue/put! @pg1 {:queue-name queue :payload {:number 2 :batch %}})
          (range 0 100))
    (start-queue! queue)
    ;; FIXME: this shouldn't be needed!
    (is (= {:id 101} (queue/put! @pg1 {:queue-name queue :payload {:number 2}})))
    (loop [i 0] ; wait for all the jobs to run
      (when (and
             (not= (count @q-ok) 101)
             (< i 130))
        (Thread/sleep 10)
        (recur (inc i))))
    (is (= {:count 0} (op/queue-size @pg1 {:queue-name queue})))
    (is (= 101 (count @q-ok)))))
