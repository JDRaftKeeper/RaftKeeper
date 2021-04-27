(ns jepsen.emo.set
  (:require [jepsen
             [checker :as checker]
             [client :as client]
             [generator :as gen]]
            [slingshot.slingshot :refer [try+]]
            [jepsen.demo.support :as s]
            [verschlimmbesserung.core :as v]))


(defrecord SetClient [k conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (v/connect (s/client-url node) {:timeout 5000})))

  (setup! [this test]
    (v/reset! conn k "#{}"))

  (invoke! [_ test op]
    (try+
     (case (:f op)
       :read (assoc op
                    :type :ok,
                    :value (read-string
                            (v/get conn k {:quorum? (:quorum test)})))

       :add (do (v/swap! conn k (fn [value]
                                  (-> value
                                      read-string
                                      (conj (:value op))
                                      pr-str)))
              (assoc op :type :ok)))

     (catch java.net.SocketTimeoutException e
       (assoc op
              :type  (if (= :read (:f op)) :fail :info)
              :error :timeout))))
  (teardown! [_ test])

  (close! [_ test]))

