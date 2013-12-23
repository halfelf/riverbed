(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.string :as string])
  (:use compojure.core
        compojure.handler
        org.httpkit.server)
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [oceanus.riverbed.go :as go])
  (:gen-class))


(def mongo-conf {:host "store" :port 27017})
(def mysql-db {:subprotocol "mysql"
               :subname "//192.168.122.1:3306/insight_online_5"
               :user "topo_gen"
               :password "Topo^Gen"})
(def topo-table "inhome_datatasks")
(def topo-filter-table "inhome_datatasks_filters")
(def filter-table "inhome_datafilters")


(defn- get-topic-ids
  [keywords]
  (let [_   (mg/connect! mongo-conf)
        _   (mg/set-db!  (mg/get-db "sandbox_mongo_1"))
        ids (vec (map 
              #(->> {:key %} (mc/find-one-as-map "keywords") :_id str)
              keywords))]
   ; (try 
    (mg/disconnect!)
    ids))
   ;   (catch Exception e (str "caught exception: " (.getMessage e)))
   ;   (finally (mg/disconnect!)))))


(defn- get-topo-spec
  [topo-id]
  (let [query-task (format "select * from %s where id=\"%s\""
                            topo-table
                            topo-id)
        topo-info (first (jdbc/query mysql-db [query-task]))]
    (if topo-info
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

(defn generate-topology 
  [req]
  (let [topo-id (:tpid (:route-params req))
        topo-spec (get-topo-spec topo-id)]
    (go/go-topo topo-spec)
    {:status 201
     :headers {"Content-Type" "text/plain"}
     :body "created"}
  ))

(defn update-topology-by-id
  [req]
  (let [topo-id (:tpid (:route-params req))
        topo-spec (get-topo-spec topo-id)]
    (if-not (nil? topo-spec)
      (do
        (go/stop-topo topo-spec)
        (go/go-topo topo-spec)
        {:status  201
         :headers {"Content-Type" "application/json"}
         :body    "updated"})
      {:status  404
       :headers {"Content-Type" "application/json"}
       :body    "no such topo"})
  ))

;  (let [body (parse-string (String. (.bytes (:body req))))

(defn get-topology-by-id
  [req]
  (let [topo-id (:tpid (:route-params req))
        topo-spec (get-topo-spec topo-id)]
    (if-not (nil? topo-spec)
      {:status  200
       :headers {"Content-Type" "application/json"}
       :body    (generate-string topo-spec)}
      {:status  404
       :headers {"Content-Type" "application/json"}
       :body    "no such topo"})
  ))

(defn stop-topology-by-id
  [req]
  (let [topo-id (:tpid (:route-params req))
        topo-spec (get-topo-spec topo-id)]
    (if-not (nil? topo-spec)
      (do
        (go/stop-topo topo-spec)
        {:status  200
         :headers {"Content-Type" "application/json"}
         :body    "deleted"})
      {:status  404
       :headers {"Content-Type" "application/json"}
       :body    "no such topo"})
  ))

(defroutes all-routes
  (GET "/" [] hello-handler)
  (context "/topology/:tpid" []
           (GET    "/" [] get-topology-by-id)
           (POST   "/" [] generate-topology)
           (PUT    "/" [] update-topology-by-id)
           (DELETE "/" [] stop-topology-by-id))
  (route/not-found "404"))

(run-server (api #'all-routes) {:port 8010})

