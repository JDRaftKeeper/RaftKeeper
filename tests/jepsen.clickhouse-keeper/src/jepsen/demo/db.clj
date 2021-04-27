(ns jepsen.demo.db
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen [cli :as cli]
             [control :as c]
             [db :as db]
             [tests :as tests]]
            [jepsen.control.util :as cu]
            [jepsen.demo.support :as s]
            [jepsen.os.debian :as debian]))

(def dir     "/opt/etcd")
(def binary "etcd")
(def logfile (str dir "/etcd.log"))
(def pidfile (str dir "/etcd.pid"))

(defn db
  "Etcd DB for a particular version."
  [version]
  (reify db/DB
     (setup! [_ test node]
             (info node "installing etcd" version)
             (c/su
              (let [url (str "https://storage.googleapis.com/etcd/" version
                             "/etcd-" version "-linux-amd64.tar.gz")]
                (cu/install-archive! url dir))

              (cu/start-daemon!
               {:logfile logfile
                :pidfile pidfile
                :chdir   dir}
               binary
               :--log-output                   :stderr
               :--name                         (name node)
               :--listen-peer-urls             (s/peer-url   node)
               :--listen-client-urls           (s/client-url node)
               :--advertise-client-urls        (s/client-url node)
               :--initial-cluster-state        :new
               :--initial-advertise-peer-urls  (s/peer-url node)
               :--initial-cluster              (s/initial-cluster test))

              (Thread/sleep 3000)))

     (teardown! [_ test node]
                (info node "tearing down etcd")
                (cu/stop-daemon! binary pidfile)
                (c/su (c/exec :rm :-rf dir)))
    db/LogFiles
    (log-files [_ test node] [logfile])))
