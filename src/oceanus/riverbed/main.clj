(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.string :as string])
  (:require [clojure.edn :as edn])
  (:require [me.raynes.fs :as fs])
  (:use compojure.core
        compojure.handler
        org.httpkit.server)
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [oceanus.riverbed
             [go :as go]
             [logs :as logs]
             [created-hook :as created-hook]])
  (:use [oceanus.riverbed.constants])
  (:gen-class))

(def config (edn/read-string (slurp "resources/config.edn")))

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
            keywords        (string/split (topo-info :seeds) #",")
            topic-ids       (get-topic-ids keywords)
            source-type     (topo-info :source)]
        (if task-to-filters
          ; topo has some filter(s)
          (reduce merge-filters {:topo-id     topo-id
                                 :topic-ids   topic-ids
                                 :keywords    keywords
                                 :source-type source-type}
                  task-to-filters)
          ; topo without filter
          {:topo-id   topo-id 
           :topic-ids topic-ids
           :and_keywords "" :or_keywords "" :not_keywords ""
           :source-type source-type
           :keywords keywords}))
      ; no topo exists
      nil)))


(defn hello-handler [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "This is dataminr riverbed server."})

(defn generate-test-topology 
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (logs/receive-req "New task (test)" topo-id)
      (if topo-spec
        (do
          (send! channel received)
          (go/go-topo topo-spec true config)) ; true means do not compile
        (do 
          (send! channel no-such-topo)
          (logs/not-exist "New task (test)" topo-id)))
      )))

(defn generate-topology 
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (logs/receive-req "New task" topo-id)
      (if topo-spec
        (do
          (send! channel received)
          (go/go-topo topo-spec false config)) 
        (do
          (send! channel no-such-topo)
          (logs/not-exist "New task" topo-id)))
      )))

(defn update-topology-by-id
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (logs/receive-req "Update task" topo-id)
      (if topo-spec
        (do
          (send! channel received)
          (go/stop-topo topo-id)
          (Thread/sleep 60000)  ; wait for killing topology
          (go/go-topo topo-spec false config))
        (do
          (send! channel no-such-topo)
          (logs/not-exist "Update task" topo-id)))
      )))

(defn stop-topology-by-id
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (logs/receive-req "Stop task" topo-id)
      (if topo-spec
        (do
          (send! channel received)
          (go/stop-topo topo-id))
        (do
          (send! channel no-such-topo)
          (logs/not-exist "Stop task" topo-id)))
      )))

(defn deactivate-handler
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (if topo-spec
        (do
          (send! channel received)
          (go/deactivate topo-id))
        (send! channel no-such-topo))
      )))

(defn activate-handler
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (if topo-spec
        (do
          (send! channel received)
          (go/activate topo-id))
        (send! channel no-such-topo))
      )))


(defn delete-consumer-handler
  [req]
  (let [topo-id (:tpid (:route-params req))
        zk-connect (format "%s:%s" 
                           (-> config :kafka :zk-host)
                           (-> config :kafka :zk-port))]
    (logs/receive-req "Delete Consumer" topo-id)
    (go/delete-consumer-info topo-id zk-connect)
    {:status  200
     :headers {"Content-Type" "text/plain"}
     :body    "ok"}))
      
(defn delete-topic-handler
  [req]
  (let [topic (:topic (:route-params req))
        zk-connect (format "%s:%s" 
                           (-> config :kafka :zk-host)
                           (-> config :kafka :zk-port))]
    (go/delete-topic topic zk-connect)
    {:status  200
     :headers {"Content-Type" "text/plain"}
     :body    "ok"}))

(defn delete-logs-handler
  [req]
  (with-channel req channel
    (let [logtype (:logtype (:route-params req))]
      (case logtype
        "zookeeper" (do (send! channel received) (logs/del-zookeeper (config :zookeeper-logs)))
        "storm"     (do (send! channel received) (logs/del-storm (config :storm-dir)))
        (send! channel wrong-type))
      )))

(defroutes all-routes
  ; all handlers which will execute `storm` command are async
  (GET "/" [] hello-handler)
  (context "/topology/:tpid" []
           (POST   "/" [] generate-topology)     ; async
           (PUT    "/" [] update-topology-by-id) ; async
           (DELETE "/" [] stop-topology-by-id))  ; async
  (GET "/topology/deactivate/:tpid" [] deactivate-handler) ;async
  (GET "/topology/activate/:tpid" [] activate-handler) ;async
  (POST "/topology/test/:tpid" [] generate-test-topology)
  (DELETE "/consumer/:tpid" [] delete-consumer-handler)
  (DELETE "/topic/:topic" [] delete-topic-handler)
  (DELETE "/logs/:logtype" [] delete-logs-handler) ;async
  (route/not-found "404"))

(defn -main
  [& args]
  (defonce server (run-server (api #'all-routes) {:port 8010 :join? false})))

