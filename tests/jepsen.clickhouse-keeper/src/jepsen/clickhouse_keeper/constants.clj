(ns jepsen.clickhouse-keeper.constants)

(def common-prefix "/home/robot-clickhouse")

(def binary-name "clickhouse")

(def pid-file-path (str common-prefix "/clickhouse.pid"))

(def bin-dir (str common-prefix "/bin"))
(def logs-dir (str common-prefix "/logs"))
(def configs-dir (str common-prefix "/config"))
(def coordination-data-dir (str common-prefix "/data"))

(def binary-path (str bin-dir "/" binary-name))
(def stderr-file (str logs-dir "/stderr.log"))

(def coordination-logs-dir (str coordination-data-dir "/logs"))
(def coordination-snapshots-dir (str coordination-data-dir "/snapshots"))

(def binaries-cache-dir (str common-prefix "/binaries"))
