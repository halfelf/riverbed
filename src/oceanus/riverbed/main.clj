(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.string :as string])
  (:require [me.raynes.fs :as fs])
  (:use compojure.core
        compojure.handler
        org.httpkit.server)
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [oceanus.riverbed
             [go :as go]
             [created-hook :as created-hook]])
  (:gen-class))


(def mongo-conf {:host "store" :port 27017})
(def mysql-db {:subprotocol "mysql"
               :subname "//192.168.122.1:3306/insight_online_5"
               :user "topo_gen"
               :password "Topo^Gen"})
(def mongo-db "sandbox_mongo_1")
(def topo-table "inhome_datatasks")
(def topo-filter-table "inhome_datatasks_filters")
(def filter-table "inhome_datafilters")

(def no-such-topo 
     {:status  404
      :headers {"Content-Type" "text/plain"}
      :body    "no such topo"})
(def received
     {:status  200
      :headers {"Content-Type" "text/plain"}
      :body    "received"})
      


(defn- get-topic-ids
  [keywords]
  (let [_   (mg/connect! mongo-conf)
        _   (mg/set-db!  (mg/get-db mongo-db))
        ids (vec (map 
              #(->> {:key %} (mc/find-one-as-map "keywords") :_id str)
              keywords))]
    (mg/disconnect!)
    ids))


(defn- get-topo-spec
  [topo-id]
  (let [query-task (format "select * from %s where id=\"%s\""
                            topo-table
                            topo-id)
        topo-info (first (jdbc/query mysql-db [query-task]))]
    (if (and topo-info (not= 2 (topo-info :status)))
      ; topo exists
      (let [query-topo-filter (format "select * from %s where datatasks_id=\"%s\""
                                      topo-filter-table
                                      topo-id)
            task-to-filter (first (jdbc/query mysql-db [query-topo-filter]))
            keywords       (string/split (topo-info :seeds) #",")
            topic-ids      (get-topic-ids keywords)]
        (if task-to-filter
          ; topo has a filter
          (let [filter-id (task-to-filter :datafilters_id)
                query-filter (format "select * from %s where id=\"%d\""
                                     filter-table
                                     filter-id)]
            (merge (first (jdbc/query mysql-db [query-filter]))
                   {:topo-id topo-id :topic-ids topic-ids
                    :keywords keywords}))
          ; topo without filter
          {:topo-id   topo-id 
           :topic-ids topic-ids
           :and_keywords "" :or_keywords "" :not_keywords ""
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
      (if topo-spec
        (do
          (send! channel received)
          (go/go-topo topo-spec true)) ; false means not local mode
        (send! channel no-such-topo)))
    ))

(defn generate-topology 
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (if topo-spec
        (do
          (send! channel received)
          (go/go-topo topo-spec false)) ; false means not local mode
        (send! channel no-such-topo)))
    ))

(defn update-topology-by-id
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (if topo-spec
        (do
          (send! channel received)
          (go/stop-topo topo-id)
          (Thread/sleep 60000)  ; wait for killing topology
          (go/go-topo topo-spec false))
        (send! channel no-such-topo)))
    ))

(defn stop-topology-by-id
  [req]
  (with-channel req channel
    (let [topo-id (:tpid (:route-params req))
          topo-spec (get-topo-spec topo-id)]
      (if topo-spec
        (do
          (send! channel received)
          (go/stop-topo topo-id))
        (send! channel no-such-topo))
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
  (route/not-found "404"))

(run-server (api #'all-routes) {:port 8010})

