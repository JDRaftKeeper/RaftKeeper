(ns jepsen.demo.main
  (:require [clojure.tools.logging :refer :all]
            [verschlimmbesserung.core :as v]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [jepsen.demo.utils :refer :all]
            [clojure.pprint :refer [pprint]]
;            [jepsen.demo.set :as set]
            [jepsen.demo.db :refer :all]
            [jepsen.demo.support :as s]
;            [jepsen.demo.nemesis :as custom-nemesis]
;            [jepsen.demo.register :as register]
;            [jepsen.demo.unique :as unique]
;            [jepsen.demo.queue :as queue]
;            [jepsen.demo.counter :as counter]
;            [jepsen.demo.constants :refer :all]
            [clojure.string :as str]
            [jepsen
             [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [nemesis :as nemesis]
             [generator :as gen]
             [independent :as independent]
             [tests :as tests]
             [util :as util :refer [meh]]]
            [jepsen.control.util :as cu]
            [jepsen.os.ubuntu :as ubuntu]
            [jepsen.os.debian :as debian]
            [jepsen.checker.timeline :as timeline]
            [clojure.java.io :as io]
            [zookeeper.data :as data]
            [zookeeper :as zk])
  (:import (org.apache.zookeeper ZooKeeper KeeperException KeeperException$BadVersionException)
           (ch.qos.logback.classic Level)
           (org.slf4j Logger LoggerFactory)))

(def cli-opts
  "Additional command line options."
  [
;    ["-w" "--workload NAME" "What workload should we run?"
;    :default "set"
;    :validate [workloads (cli/one-of workloads)]]
;   [nil "--nemesis NAME" "Which nemesis will poison our lives?"
;    :default "random-node-killer"
;    :validate [custom-nemesis/custom-nemesises (cli/one-of custom-nemesis/custom-nemesises)]]
   ["-q" "--quorum" "Use quorum reads, instead of reading from any primary."]
   ["-r" "--rate HZ" "Approximate number of requests per second, per thread."
    :default  10
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   ["-s" "--snapshot-distance NUM" "Number of log entries to create snapshot"
    :default 10000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--stale-log-gap NUM" "Number of log entries to send snapshot instead of separate logs"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--reserved-log-items NUM" "Number of log entries to keep after snapshot"
    :default 1000
    :parse-fn read-string
    :validate [#(and (number? %) (pos? %)) "Must be a positive number"]]
   [nil "--ops-per-key NUM" "Maximum number of operations on any given key."
    :default  100
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]
   [nil, "--lightweight-run" "Subset of workloads/nemesises which is simple to validate"]
   [nil, "--reuse-binary" "Use already downloaded binary if it exists, don't remove it on shutdown"
    :default true]])

;--------- client ---------

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (s/client-url node)
                                 {:timeout 5000})))

  (setup! [this test])
  (invoke! [_ test op]
    (try+
     (case (:f op)
       :read (let [value (-> conn
                             (v/get "foo" {:quorum? true})
                             parse-long)]
               (assoc op :type :ok, :value value))
       :write (do (v/reset! conn "foo" (:value op))
                (assoc op :type :ok))
       :cas (let [[old new] (:value op)]
              (assoc op :type (if (v/cas! conn "foo" old new)
                                :ok
                                :fail))))

     (catch java.net.SocketTimeoutException e
       (assoc op
              :type  (if (= :read (:f op)) :fail :info)
              :error :timeout))

     (catch [:errorCode 100] e
       (assoc op :type :fail, :error :not-found))))

  (teardown! [this test])

  (close! [_ test]
    ; If our connection were stateful, we'd close it here. Verschlimmmbesserung
    ; doesn't actually hold connections, so there's nothing to close.
    ))

;--------- checker -----------

(definterface Model
  (step [model op]
        "The job of a model is to *validate* that a sequence of operations
        applied to it is consistent. Each invocation of (step model op)
        returns a new state of the model, or, if the operation was
        inconsistent with the model's state, returns a (knossos/inconsistent
        msg). (reduce step model history) then validates that a particular
        history is valid, and returns the final state of the model.

        Models should be a pure, deterministic function of their state and an
        operation's :f and :value."))

(defrecord CASRegister [value]
  Model
  (step [this r op]
    (condp = (:f op)
      :write (CASRegister. (:value op))
      :cas   (let [[cur new] (:value op)]
               (if (= cur value)
                 (CASRegister. new)
                 (model/inconsistent (str "can't CAS " value " from " cur " to " new))))
      :read  (if (or (nil? (:value op))
                     (= value (:value op)))
               r
               (model/inconsistent (str "can't read " (:value op) " from register " value))))))


(defn chservice-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (merge tests/noop-test
         opts
         {:name "ch-service"
          :os   debian/os
          :db   (db "v3.1.5")
          :client   (Client. nil)
          :pure-generators true
          :nemesis         (nemesis/partition-random-halves)
          :checker         (checker/compose
                            {:perf (checker/perf)
                             :linear (checker/linearizable {:model (model/cas-register) :algorithm :linear})
                             :timeline (timeline/html)})
          :generator       (->> (gen/mix [r w cas])
                                (gen/stagger 1)
                                (gen/nemesis
                                 (cycle [(gen/sleep 5)
                                         {:type :info, :f :start}
                                         (gen/sleep 5)
                                         {:type :info, :f :stop}]))
                                (gen/time-limit 30))}))
(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn chservice-test :opt-spec cli-opts}) args))
