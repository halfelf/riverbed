(ns oceanus.riverbed.logs
  (:require [taoensso.timbre :as timbre])
  (:gen-class))

(timbre/refer-timbre)
(timbre/set-config! [:appenders :spit :enabled?] true)
(timbre/set-config! [:shared-appender-config :spit-filename] "./logs/riverbed.log")

(defn start []
  (info "Riverbed Controller starts."))

(defn receive-req
  [req-type topo-id]
  (info 
    (format "%s request received. Topology id is %s." req-type topo-id)))

(defn execute-req 
  [req-type topo-id]
  (info 
    (format "%s request executed. Topology id is %s." req-type topo-id)))

(defn not-exist
  [req-type topo-id]
  (info 
    (format "%s request received. Task %s doesn't exist." req-type topo-id)))

(defn wrong-req
  [obj operation]
  (info
    (format "Bad request: to object: %s, operation: %s" obj operation)))

(defn exception
  [excp]
  (warn (.getMessage excp)))

