(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.string :as string])
  (:require [clojure.edn :as edn])
  (:require [clojure.set :refer [rename-keys]])
  (:require [me.raynes.fs :as fs])
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [langohr core basic consumers])
  (:require [oceanus.riverbed
             [logs :as logs]
             [created-hook :as created-hook]])
  (:import java.util.regex.Pattern)
  (:import [org.apache.curator RetryPolicy
                               framework.CuratorFramework
                               framework.CuratorFrameworkFactory
                               retry.ExponentialBackoffRetry])
  (:import [org.apache.zookeeper KeeperException$NodeExistsException
                                 KeeperException$NoNodeException])
  (:gen-class))

(def config (edn/read-string (slurp "resources/config.edn")))

(def ^{:const true} write-interval 30000)  ; ms
(def console-msg (ref {}))

(defn get-topic-ids
  [keywords data-source]
  (let [_   (mg/connect! (config :mongo-conf))
        _   (mg/set-db!  (mg/get-db (config :mongo-db)))
        ids (vec (map
              #(->> {:key % :source data-source}
                    (mc/find-one-as-map "keywords") :_id str)
              keywords))]
    (mg/disconnect!)
    ids))

(defn join-may-empty
  [str1 str2]
  (cond
    (string/blank? str1) str2
    (string/blank? str2) str1
    :else (string/join "," [str1 str2])))

(defn merge-filters
  [current-spec task-filter]
  (let [filter-id (task-filter :datafilters_id)
        query-filter (format "select * from %s where id=\"%d\""
                             (config :filter-table)
                             filter-id)
        this-filter (-> (jdbc/query (config :mysql-db) [query-filter])
                        first
                        (select-keys
                          [:and_keywords :or_keywords :not_keywords]))]
    (merge-with join-may-empty current-spec this-filter)))

(defn get-task-spec
  [task-id]
  (let [query-task (format "select * from %s where id=\"%s\""
                            (config :task-table)
                            task-id)
        task-info (first (jdbc/query (config :mysql-db) [query-task]))]
    (if (and task-info (not= 2 (task-info :status)))
      ; task exists
      (let [query-task-filter (format "select * from %s where datatasks_id=\"%s\""
                                      (config :task-filter-table)
                                      task-id)
            task-to-filters (jdbc/query (config :mysql-db) [query-task-filter])
            keywords        (string/split (task-info :seeds) #"[,，]")
            add-to-seg      (string/split (task-info :seeds) #"[,，\s]")
            data-source     (task-info :source)
            topic-ids       (get-topic-ids keywords data-source)
            source-type     (task-info :source)]
        (if task-to-filters
          ; task has some filter(s)
          (reduce merge-filters {:task-id     task-id
                                 :topics      (zipmap topic-ids keywords)
                                 :add-to-seg  add-to-seg
                                 :source-type source-type}
                  task-to-filters)
          ; task without filter
          {:task-id   task-id
           :topics    (zipmap topic-ids keywords)
           :and_keywords "" :or_keywords "" :not_keywords ""
           :source-type source-type
           :add-to-seg add-to-seg}))
      ; no task exists
      nil)))

(defn- check-empty-or-split
  "if empty str => [], else => split it"
  [maybe-words]
  (if (empty? maybe-words)
    []
    (string/split maybe-words #",")))

(defn make-regex
  "generating regex"
  [words-list filter-type]
  (case filter-type
    :include-any (string/join "|" (map #(. Pattern quote %) words-list))
    :exclude-any (string/join "|" (map #(. Pattern quote %) words-list))
    :include-all (string/join (map #(str "(?=.*" (. Pattern quote %) ")") words-list))
      ; "\\Qa\\E|\\Qb\\E|\\Qc\\E"  or
      ; "(?=.*\\Qa\\E)(?=.*\\Qb\\E)(?=.*\\Qc\\E)"
    ))

(defn refine-spec
  "Split words in :and_keywords, or_keywords, :not_keywords
   and rename these keys, btw rename :conditions"
  [task-spec]
  (let [split-words   (reduce #(update-in %1 [%2] check-empty-or-split)
                            task-spec
                            [:or_keywords :and_keywords :not_keywords])
        refined-spec  (-> split-words
                        (update-in [:conditions] #(or % "and"))  ; default to "and"
                        (rename-keys {:or_keywords  :include-any
                                      :and_keywords :include-all
                                      :not_keywords :exclude-any
                                      :conditions   :condition})
                        (update-in [:include-any] #(make-regex % :include-any))
                        (update-in [:include-all] #(make-regex % :include-all))
                        (update-in [:exclude-any] #(make-regex % :exclude-any)))]
    refined-spec))

(defn zk-path
  "Accept a task id, return `/tasks/taskid`"
  [subpath & {:keys [root-path]
              :or   {root-path "/tasks"}}]
  (format "%s/%s" root-path subpath))

(defn new-task
  "create zookeeper path, set value to spec (json-like)"
  [task-id curator]
  (let [task-spec    (get-task-spec task-id)
        refined-spec (refine-spec task-spec)]
    (doseq [one-keyword (refined-spec :add-to-seg)]
      (created-hook/insert-keyword-to-dict (config :innerapi) one-keyword))
    (try
      (.. curator create inBackground
                  (forPath
                    (zk-path task-id)
                    (.getBytes (generate-string refined-spec))))
      (catch KeeperException$NodeExistsException e (logs/exception e)))
    (logs/execute-req "NewTask" task-id)))

(defn update-task
  [task-id curator]
  (let [task-spec    (get-task-spec task-id)
        refined-spec (refine-spec task-spec)]
    (try
      (.. curator setData inBackground
                  (forPath
                    (zk-path task-id)
                    (.getBytes (generate-string refined-spec))))
      (catch KeeperException$NoNodeException e (logs/exception e)))
    (logs/execute-req "Update" task-id)))

(defn stop-task
  [task-id curator]
  (try
    (.. curator delete inBackground
                (forPath
                  (zk-path task-id)))
    (catch KeeperException$NoNodeException e (logs/exception e)))
  (logs/execute-req "Stop" task-id))

(defn- process-requests
  [curator]
  (doseq [[obj operation] @console-msg]
    (case operation
      ; I guess synchronized operation is OK now,
      ; since it only write some data into zookeeper.
      :new    (new-task    obj curator)
      :stop   (stop-task   obj curator)
      :update (update-task obj curator)
      :exit   (throw (Exception. "Exit Command"))
      (logs/wrong-req obj operation)))
  (dosync
    (ref-set console-msg {}))
  )

(defn- update-with-tid
  [request-map task-id operation]
  (conj {task-id operation}
    (if (contains? request-map task-id)
      (dissoc request-map task-id)
      request-map)))

(defn- save-request
  [ch {:keys [content-type delivery-tag type] :as meta} ^bytes payload]
  (dosync
    (alter console-msg update-with-tid (String. payload) (keyword type))
    (logs/receive-req (str type) (String. payload))
    ))

(defn -main
  [& args]
  (let [conn     (langohr.core/connect (config :rabbit))
        ch       (langohr.channel/open conn)
        qname    "storm.console"
        curator  (CuratorFrameworkFactory/newClient
                   (-> config :storm :zk-host)
                   (ExponentialBackoffRetry. 1000 3))]
    (logs/start)
    (.start curator)
    (future (langohr.consumers/subscribe ch qname save-request :auto-ack true))
    (try
      (while true
        (do
          (process-requests curator)
          (Thread/sleep write-interval)))
      ;(catch Exception e
      ;  (logs/exception e))
      (finally
        (do
          (.close curator)
          (langohr.core/close ch)
          (langohr.core/close conn))))
    ))

