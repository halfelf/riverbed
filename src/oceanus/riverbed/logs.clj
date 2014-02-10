(ns oceanus.riverbed.logs
  (:require [me.raynes.fs :as fs])
  (:import org.apache.commons.io.FilenameUtils)
  (:gen-class))

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
