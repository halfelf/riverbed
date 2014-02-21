(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.string :as string])
  (:require [clojure.edn :as edn])
  (:require [me.raynes.fs :as fs])
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [langohr core basic consumers])
  (:require [oceanus.riverbed
             [go :as go]
             [logs :as logs]
             [created-hook :as created-hook]])
  (:use [oceanus.riverbed.constants])
  (:gen-class))

(def config (edn/read-string (slurp "resources/config.edn")))

(def ^{:const true} two-mins 120000)  ; ms
(def console-msg (ref {}))

(defn- get-topic-ids
  [keywords]
  (let [_   (mg/connect! (config :mongo-conf))
        _   (mg/set-db!  (mg/get-db (config :mongo-db)))
        ids (vec (map 
              #(->> {:key %} (mc/find-one-as-map "keywords") :_id str)
              keywords))]
    (mg/disconnect!)
    ids))

(defn- join-may-empty
  [str1 str2]
  (cond
    (string/blank? str1) str2
    (string/blank? str2) str1
    :else (string/join "," [str1 str2])))

(defn- merge-filters
  [current-spec task-filter]
  (let [filter-id (task-filter :datafilters_id)
        query-filter (format "select * from %s where id=\"%d\""
                             (config :filter-table)
                             filter-id)
        this-filter (-> (jdbc/query (config :mysql-db) [query-filter])
                        first
                        (select-keys 
                          [:and_keywords :or_keywords :not_keywords]))]
    (merge-with join-may-empty current-spec this-filter)
    ))

(defn- get-topo-spec
  [topo-id]
  (let [query-task (format "select * from %s where id=\"%s\""
                            (config :topo-table)
                            topo-id)
        topo-info (first (jdbc/query (config :mysql-db) [query-task]))]
    (if (and topo-info (not= 2 (topo-info :status)))
      ; topo exists
      (let [query-topo-filter (format "select * from %s where datatasks_id=\"%s\""
                                      (config :topo-filter-table)
                                      topo-id)
            task-to-filters (jdbc/query (config :mysql-db) [query-topo-filter])
            keywords        (string/split (topo-info :seeds) #"[,，]")
            add-to-seg      (string/split (topo-info :seeds) #"[,，\s]")
            topic-ids       (get-topic-ids keywords)
            source-type     (topo-info :source)]
        (if task-to-filters
          ; topo has some filter(s)
          (reduce merge-filters {:topo-id     topo-id
                                 :topic-ids   topic-ids
                                 :keywords    keywords
                                 :add-to-seg  add-to-seg
                                 :source-type source-type}
                  task-to-filters)
          ; topo without filter
          {:topo-id   topo-id 
           :topic-ids topic-ids
           :and_keywords "" :or_keywords "" :not_keywords ""
           :source-type source-type
           :keywords keywords
           :add-to-seg add-to-seg}))
      ; no topo exists
      nil)))

(defn new-topo
  [topo-id cluster-mode]
  (let [topo-spec (get-topo-spec topo-id)]
    (logs/receive-req (format "NewTask (cluster-mode: %s)" cluster-mode) topo-id)
    (go/go-topo topo-spec cluster-mode config)))

(defn update-topo
  [topo-id]
  (let [topo-spec (get-topo-spec topo-id)]
    (logs/receive-req "Update" topo-id)
    (go/stop-topo topo-id)
    (go/go-topo topo-spec true config)))

(defn kill-topo
  [topo-id]
  (logs/receive-req "Stop" topo-id)
  (go/stop-topo topo-id))

(defn delete-consumer
  [topo-id]
  (let [zk-connect (format "%s:%s" 
                           (-> config :kafka :zk-host)
                           (-> config :kafka :zk-port))]
    (logs/receive-req "Delete Consumer" topo-id)
    (go/delete-consumer-info topo-id zk-connect)
    ))

(defn delete-topic
  [topic]
  (let [zk-connect (format "%s:%s" 
                           (-> config :kafka :zk-host)
                           (-> config :kafka :zk-port))]
    (go/delete-topic topic zk-connect)))

(defn delete-logs
  [logtype]
  (case logtype
    "zookeeper" (logs/del-zookeeper (config :zookeeper-logs))
    "storm"     (logs/del-storm (config :storm-dir))
    "wrong-type"))

(defn- process-requests
  []
  (dosync
    (doseq [[obj operation] @console-msg]
      (case operation
        :new    (future (new-topo obj true))
        :test   (future (new-topo obj false))
        :update (future (update-topo obj))
        :stop   (future (kill-topo obj))
        :del-consumer (future (delete-consumer obj))
        :del-topic    (future (delete-topic obj)) 
        :del-old-logs (future (delete-logs obj))
        (logs/wrong-req obj operation)))
    (ref-set console-msg {})
    ))

(defn- update-with-tid
  [request-map topo-id operation]
  (conj {topo-id operation}
    (if (contains? request-map topo-id)
      (dissoc request-map topo-id)
      request-map)))

(defn- save-request
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (dosync  ; just in case
    (alter console-msg update-with-tid (String. payload) (keyword type))
    ))

(defn -main
  [& args]
  (let [conn     (langohr.core/connect (config :rabbit))
        ch       (langohr.channel/open conn)
        qname    "storm.console"]
    (logs/start)
    (future (langohr.consumers/subscribe ch qname save-request :auto-ack true))
    (try
      (while true
        (do
          (process-requests)
          (Thread/sleep two-mins)))
      (catch Exception e
        (logs/exception "main" (.getMessage e)))
      (finally 
        (do
          (langohr.core/close ch)
          (langohr.core/close conn))))
    ))

