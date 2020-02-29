(ns taskmaster.queue-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [taskmaster.dev.connection :as conn]
    [taskmaster.operation :as op]
    [taskmaster.queue :as queue]))


(def pg1 (atom nil))
(def pg2 (atom nil))
(def p (atom nil))

(def q-ok (atom []))
(def q-fail (atom []))

(def all-jobs (atom []))


(defn callback
  [{:keys [id queue-name payload] :as job}]
  (log/infof "CALLBACK! %s %s %s" id queue-name payload)
  (swap! all-jobs conj job)
  (if (even? (:number payload))
    (do
      (swap! q-ok conj job)
      :taskmaster.operation/ack)
    (do
      (swap! q-fail conj job)
      :taskmaster.operation/reject)))


(def queue-name "test_jobs")


(defn start-conn!
  []
  (reset! pg1 (.start (conn/make-one)))
  (reset! pg2 (.start (conn/make-one)))
  (op/create-jobs-table! @pg1))


(defn start-consumer!
  [queue-name]
  (reset! p
          (future
            (queue/start!
              @pg2
              {:queue-name queue-name
               :concurrency 1
               :callback callback}))))


(defn cleanup!
  []
  (when-let [p @@p]
    (queue/stop! p))
  (reset! q-ok [])
  (reset! q-fail [])
  (reset! all-jobs [])
  (op/drop-jobs-table! @pg1)
  (.stop @pg1)
  (.stop @pg2))


(use-fixtures :each (fn [t]
                      (with-bindings {#'taskmaster.operation/*job-table-name* "test_jobs_5"}
                        (try
                          (start-conn!)
                          (t)
                          (cleanup!)
                          (catch Exception e

                            (cleanup!)
                            (throw e))))))


(deftest it-does-basic-ops

  (testing "it pushes to the queue"
    (is (= [{:id 1} {:id 2} {:id 3}]
           [(queue/put! @pg1 {:queue-name queue-name :payload {:number 2}})
            (queue/put! @pg1 {:queue-name queue-name :payload {:number 4}})

            (queue/put! @pg1 {:queue-name queue-name :payload {:number 6}})]))
    (Thread/sleep 50)
    (start-consumer! queue-name)
    (queue/put! @pg1 {:queue-name queue-name :payload {:number 1}}))
  (is (= {:count 4} (op/queue-size @pg1 {:queue-name queue-name})))
  (Thread/sleep 3300)
  (testing "it picks up and processes jobs"
    (testing "success jobs"
      (is (= 3 (count (-> q-ok deref))))
      (is (= [2 4 6] (mapv #(-> % :payload :number) @q-ok))))
    (testing "failed jobs"
      (is (= 1 (count @q-fail)))
      (is (= [1] (mapv #(-> % :payload :number) @q-fail)))
      (is (= {:count 1}
             (op/queue-size @pg1 {:queue-name queue-name})))
      (is (= [{:number 2}
              {:number 4}
              {:number 6}
              {:number 1}]
             @all-jobs)))))


#_(deftest resuming-lots-of-jobs
  (let [queue  (str queue "_large")]
    (mapv #(queue/put! @pg1 {:queue-name queue :payload {:number 2 :batch %}})
          (range 0 100))
    (start-consumer! queue)
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
