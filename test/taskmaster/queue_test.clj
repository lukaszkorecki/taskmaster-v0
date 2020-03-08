(ns taskmaster.queue-test
  (:require
    [clojure.test :refer :all]
    [clojure.tools.logging :as log]
    [taskmaster.dev.connection :as conn]
    [taskmaster.operation :as op]
    [taskmaster.queue :as queue]))

(def db-conn (atom nil))
(def work-pool (atom nil))

(def queue-ok-results (atom []))
(def queue-fail-results (atom []))

(def all-jobs (atom []))
(def queue-name "test_jobs")

(defn callback
  [{:keys [id queue-name payload] :as job}]
  (println "fooooyoyoyoy")
  (log/infof "CALLBACK! %s %s %s" id queue-name payload)
  (swap! all-jobs conj job)
  (throw (ex-info "fail" job))
  (if (even? (:number payload))
    (do
      (swap! queue-ok-results conj job)
      :taskmaster.operation/ack)
    (do
      (swap! queue-fail-results conj job)
      :taskmaster.operation/reject)))

(defn start-conn!
  []
  (reset! db-conn (.start (conn/make-one)))
  (op/create-jobs-table! @db-conn))

(defn start-consumer!
  [queue-name]

          (queue/start!
            @db-conn
            {:queue-name queue-name
             :concurrency 2
             :callback callback}))

(defn cleanup!
  []
  (reset! queue-ok-results [])
  (reset! queue-fail-results [])
  (reset! all-jobs [])
  (op/drop-jobs-table! @db-conn)
  (.stop @db-conn))

(use-fixtures :each (fn [t]
                      (with-bindings {#'taskmaster.operation/*job-table-name* (str "test_" queue-name)}
                        (try
                          (start-conn!)
                          (t)
                          (cleanup!)
                          (catch Exception e
                            (cleanup!)
                            (throw e))))))

(deftest it-does-basic-ops
  (is (= {:count 0} (op/queue-size @db-conn {:queue-name queue-name})))
  (let [pool (start-consumer! queue-name)]
    (testing "it pushes to the queue"
      (is (= [{:id 1} {:id 2} {:id 3}]
             [(queue/put! @db-conn {:queue-name queue-name :payload {:number 2}})
              (queue/put! @db-conn {:queue-name queue-name :payload {:number 4}})
              (queue/put! @db-conn {:queue-name queue-name :payload {:number 6}})]))
      (queue/put! @db-conn {:queue-name queue-name :payload {:number 1}}))
    (Thread/sleep 1500)
    (is (= {:count 4} (op/queue-size @db-conn {:queue-name queue-name})))
    (queue/stop! pool))
  (Thread/sleep 15000)
  (testing "it picks up and processes jobs"
    (testing "success jobs"
      (is (= 3 (count (-> queue-ok-results deref))))
      #_ (is (= [2 4 6] (mapv #(-> % :payload :number) @queue-ok-results))))
    (testing "failed jobs"
      (is (= 1 (count @queue-fail-results)))
      #_(is (= [1] (mapv #(-> % :payload :number) @queue-fail-results)))
      #_ (is (= {:count 1}
                (op/queue-size @db-conn {:queue-name queue-name})))
      #_ (is (= [{:number 2}
                 {:number 4}
                 {:number 6}
                 {:number 1}]
                @all-jobs)))))




#_(deftest resuming-lots-of-jobs
    (let [queue  (str queue "_large")]
      (mapv #(queue/put! @db-conn {:queue-name queue :payload {:number 2 :batch %}})
            (range 0 100))
      (start-consumer! queue)
    ;; FIXME: this shouldn't be needed!
      (is (= {:id 101} (queue/put! @db-conn {:queue-name queue :payload {:number 2}})))
      (loop [i 0] ; wait for all the jobs to run
        (when (and
               (not= (count @queue-ok-results) 101)
               (< i 130))
          (Thread/sleep 10)
          (recur (inc i))))
      (is (= {:count 0} (op/queue-size @db-conn {:queue-name queue})))
      (is (= 101 (count @queue-ok-results)))))
