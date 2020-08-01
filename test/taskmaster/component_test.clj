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


(defn handler [{:keys [payload component] :as job}]
  ((:injected-fn component) (dissoc job :component))
  (if (even? (:number payload))
    ::op/ack
    ::op/reject))


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


(defn setup! []
  (let [c (.start (conn/make-one))]
    (op/create-jobs-table! c)
    (.stop c)))


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
  (is (= {:count 4} (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
  (Thread/sleep 2000)
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


#_ (deftest failed-job-handling
  (let [alt-syst (component/start-system (make-system "retrying_queue" handler))]
    (testing "it doesn't try to run the failed job on restart"
      (ts/put! (:publisher @SYS) {:queue-name "retrying_queue" :payload {:number 1}})
      (Thread/sleep 2000)
    (is (= [1] (mapv #(-> %  :payload :number) @rejected-jobs)))
    (is (= {:count 1}
           (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
    (is (= 1 (count @rejected-jobs)))
    (component/stop alt-syst)
    (component/start alt-syst)
    (Thread/sleep 1000)
    (is (= [1] (mapv #(-> %  :payload :number) @rejected-jobs)))
    (is (= {:count 1}
           (op/queue-size (:db-conn @SYS) {:queue-name queue-name})))
    (is (= 1 (count @rejected-jobs)))
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
