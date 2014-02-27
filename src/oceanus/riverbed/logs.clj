(ns oceanus.riverbed.logs
  (:require [me.raynes.fs :as fs])
  (:require [taoensso.timbre :as timbre])
  (:import org.apache.commons.io.FilenameUtils)
  (:gen-class))

(timbre/refer-timbre)
(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "./logs/riverbed.log")

(defn start []
  (info "Riverbed Controller starts."))

(defn exception
  [thread message]
  (error
    (format "Exception: %s    in Thread %s." message thread)))

(defn receive-req
  [req-type topo-id]
  (info 
    (format "%s request received. Topology id is %s." req-type topo-id)))

(defn not-exist
  [req-type topo-id]
  (info 
    (format "%s request received. Task %s doesn't exist." req-type topo-id)))

(defn wrong-req
  [obj operation]
  (info
    (format "Bad request: to object: %s, operation: %s" obj operation)))

(defn del-zookeeper 
  [zoo-dir]
  (let [now-timestamp (System/currentTimeMillis)
        three-days-millis (* 3 86400000)]
    (if (fs/exists? zoo-dir)
      (doseq [one-log (file-seq (clojure.java.io/file zoo-dir))]
        (if (> (- now-timestamp (fs/mod-time one-log))
               three-days-millis)
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
