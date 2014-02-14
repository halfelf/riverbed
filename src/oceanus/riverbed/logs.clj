(ns oceanus.riverbed.logs
  (:require [me.raynes.fs :as fs])
  (:require [taoensso.timbre :as timbre])
  (:import org.apache.commons.io.FilenameUtils)
  (:gen-class))

(timbre/refer-timbre)
(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "./logs/riverbed.log")

(defn receive-req
  [req-type topo-id]
  (info 
    (format "%s request received. Topology id is %s." req-type topo-id)))

(defn not-exist
  [req-type topo-id]
  (info 
    (format "%s request received. Task %s doesn't exist." req-type topo-id)))

(defn del-zookeeper 
  [zoo-dir]
  (let [now-timestamp (System/currentTimeMillis)
        one-week-millis (* 7 86400000)]
    (if (fs/exists? zoo-dir)
      (doseq [one-log (file-seq (clojure.java.io/file zoo-dir))]
        (if (> (- now-timestamp (fs/mod-time one-log))
               one-week-millis)
          (fs/delete one-log)))
      )))


; storm logs are just too small and important
; maybe leave them alone for manual purge
; so the following functions are unfinished
(defn del-storm 
  [storm-dir]
  (let [now-timestamp (System/currentTimeMillis)
        log-dir  (. FilenameUtils concat storm-dir "logs")]     
    (if (fs/exists? log-dir)
      (doseq [one-log (file-seq (clojure.java.io/file log-dir))]
        )
    )))


(defn- tail-log
  [one-log]
  )
