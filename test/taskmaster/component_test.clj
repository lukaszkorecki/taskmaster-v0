(ns taskmaster.component-test
  (:require
    [clojure.test :refer [deftest is testing use-fixtures]]
    [com.stuartsierra.component :as component]
    [taskmaster.component :as ts]
    [taskmaster.dev.connection :as conn]
    [taskmaster.operation :as op]))


(def queue-name "test_component_queue")

(def acked-jobs (atom []))
(def rejected-jobs (atom []))

(def all-jobs (atom []))
(def alt-queue-jobs (atom []))

(defn injected-fn [payload] (swap! all-jobs conj payload))


(defn recorder-middleware [handler]
  (fn [payload]
    (let [res (handler payload)]
      (if (= ::op/ack res)
        (swap! acked-jobs conj (dissoc payload :component))
        (swap! rejected-jobs conj (dissoc payload :component)))
      res)))


;; handler just checks if a number passed in is even.
;; also calls a dependcy component (a function in this case)
(defn handler [{:keys [payload component] :as job}]
  ((:injected-fn component) (dissoc job :component))
  (if (even? (:number payload))
    ::op/ack
    ::op/reject))

;; alternative handler, just acks jobs
(defn alt-handler [job]
  (swap! alt-queue-jobs conj (dissoc  job :component))
  ::op/ack)


(defn make-system [queue-name handler]
  (component/map->SystemMap
    {:db-conn (conn/make-one)
     :consumer (component/using
                 (ts/create-consumer {:queue-name queue-name
                                      :handler (recorder-middleware handler)
                                      :concurrency 2})
                 [:db-conn :injected-fn])
     :injected-fn injected-fn
     :publisher (component/using
                  (ts/create-publisher)
                  [:db-conn])}))


(def SYS (atom nil))


(defn with-db-conn
  "Helper to run tasks with a passed in one-off PG connection"
  [a-fn]
  (let [c (.start (conn/make-one))]
    (a-fn c)
    (.stop c)))


(defn setup! []
  (with-db-conn op/create-jobs-table!
    #_ (let [c (.start (conn/make-one))]
         (op/create-jobs-table! c)
         (.stop c))))


(defn cleanup!
  []
  (reset! acked-jobs [])
  (reset! rejected-jobs [])
  (reset! all-jobs [])
  (op/drop-jobs-table! (:db-conn @SYS)))


(use-fixtures :each (fn [test-fn]
                      (with-redefs  [taskmaster.operation/*job-table-name*  "taskmaster_test"]
                        ;; this has to happen outside of the system as we need the table to exist!
                        (when (setup!)
                          (reset! SYS (component/start-system (make-system queue-name handler))))
                        (test-fn)
                        (when (cleanup!)
                          (swap! SYS component/stop)))))


(deftest basic-pub-consume
  (is (= {:count 0} (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
  (testing "it pushes to the queue"
    (is (= [{:id 1} {:id 2} {:id 3}]
           [(ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number 2}})
            (ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number 4}})
            (ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number 6}})]))
    (ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number 1}}))
  ;; since the consumers are not running immediately, we can inspect the queue depth
  (is (= {:count 4} (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
  ;; wait for it
  (Thread/sleep 2000)
  ;; ...and done
  (is (= 4 (count @all-jobs)))
  (testing "it picks up and processes jobs"
    (testing "success jobs"
      (is (= 3 (count (-> acked-jobs deref))))
      (is (= [2 4 6] (mapv #(-> % :payload :number) @acked-jobs))))
    (testing "failed jobs"
      (is (= 1 (count @rejected-jobs)))
      (is (= [1] (mapv #(-> %  :payload :number) @rejected-jobs)))
      (is (= {:count 1}
             (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
      (is (= [{:id 1 :payload {:number 2} :queue-name queue-name}
              {:id 2 :payload {:number 4} :queue-name queue-name}
              {:id 3 :payload {:number 6} :queue-name queue-name}
              {:id 4 :payload {:number 1} :queue-name queue-name}]
             (map #(select-keys % [:id :queue-name :payload]) @all-jobs))))))


(deftest failed-job-handling
  (let [queue-name "retrying_queue"
        alt-syst (component/start-system (make-system queue-name handler))]
    (testing "fails processing, it doesn't try to run the failed job on consumer restart"
      ;; this wil fail, since :number is not even
      (ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number 1}})
      (Thread/sleep 2000)
      ;; we got it
      (is (= [1] (mapv #(-> %  :payload :number) @rejected-jobs)))
      (is (= {:count 1}
             (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
      (is (= [{:number 1}] (map :payload (op/find-failed-jobs (:db-conn @SYS) {:queue-name queue-name}))))
      (is (= 1 (count @rejected-jobs)))
      (testing "requeueing"
        ;; we will requeue the failed job
        ;; it will still fail, but only 1 job will be stored in PG
        (let [id (:id (last @rejected-jobs))]
          (is (= [{:failed 1, :pending 0, :queue-name "retrying_queue", :total 1}]
                 (op/queue-stats (:db-conn @SYS))))
          (op/requeue! (:db-conn @SYS) {:id [id]})
          (Thread/sleep 1500)
          (is (= [{:failed 1, :pending 0, :queue-name "retrying_queue", :total 1}]
                 (op/queue-stats (:db-conn @SYS))))
          (is (= 2 (count @rejected-jobs)))
          (is (= [1 2] (map :id @rejected-jobs)))))
      (testing "failures are not picked up on restart"
        ;; verify that the issue with reprocessing previously failed jobs doesn't persist
        (reset! rejected-jobs [])
        (component/stop alt-syst)
        (component/start alt-syst)
        (Thread/sleep 1000)
        ;; we have failed jobs, but we didn't consume them again
        (is (empty? @rejected-jobs))
        (is (= {:count 1}
               (op/queue-size (:db-conn @SYS) {:queue-name queue-name}))))
      (try
        (Thread/sleep 2000)
        (component/stop alt-syst)
        (catch Exception _
          :noop)))))


(deftest resume-consumption
  ;; start another system, with alternative consumer
  ;; but push jobs before that happens
  (mapv (fn [i]
          (ts/put! (:publisher @SYS) {:queue-name "resumable_queue" :payload {:number (* i 2)}})
          (ts/put! (:publisher @SYS) {:queue-name queue-name :payload {:number (* i 2)}}))
        (range 1 10))
  (with-db-conn (fn [c]
                  (is (= [{:failed 0 :pending 9 :queue-name "test_component_queue" :total 9}
                          {:failed 0 :pending 9 :queue-name "resumable_queue" :total 9}]
                         (op/queue-stats c)))))
  (let [other-syst (component/start-system (make-system "resumable_queue" alt-handler))]
    (Thread/sleep 7000)
    ;; only 9 here, because alt handler sends the data here
    (is (= 9  (count @alt-queue-jobs)))
    ;; the original consumer, setup in fixtures
    (is (= 9 (count @all-jobs)))
    ;; both consumers!
    (is (= 18 (count @acked-jobs)))
    (testing "it doesn't pick up jobs from other queue!"
      (is (= #{"resumable_queue"}
             (->> @alt-queue-jobs
                  (map :queue-name)
                  set))))
    (try
      (Thread/sleep 2000)
      (component/stop other-syst)
      (catch Exception _
        :noop))))
