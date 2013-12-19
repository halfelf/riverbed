(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require [clojure.java.jdbc :as jdbc])
  (:use compojure.core
        compojure.handler
        org.httpkit.server)
  (:require [oceanus.riverbed.go :as go])
  (:gen-class))


(def mysql-db {:subprotocol "mysql"
               :subname "//192.168.122.1:3306/insight_online_5"
               :user "topo_gen"
               :password "Topo^Gen"})
(def topo-table "inhome_datafilters")

(defn hello-handler [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "This is dataminr riverbed server."})

(defn generate-topology 
  [req]
  (let [topo-id (:tpid (:route-params req))
        query-string (format "select * from %s where id=\"%s\""
                             topo-table
                             topo-id)
        topo-spec (first (jdbc/query mysql-db [query-string]))]
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;({:name "test", :conditions "and", :not_keywords "bla", 
    ;  :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14})
    (go/go-topo topo-spec)
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    {:status 201
     :headers {"Content-Type" "text/plain"}
     :body "created"}
  ))

;  (let [body (parse-string (String. (.bytes (:body req))))

(defn get-topology-by-id
  [req]
  (let [topo-id   (:tpid (:route-params req))
        query-string (format "select * from %s where id=\"%s\""
                             topo-table
                             topo-id)
        topo-spec (first (jdbc/query mysql-db [query-string]))]
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
  (let [topo-id   (:tpid (:route-params req))
        query-string (format "select * from %s where id=\"%s\""
                             topo-table
                             topo-id)
        topo-spec (first (jdbc/query mysql-db [query-string]))]
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
           (DELETE "/" [] stop-topology-by-id))
  (route/not-found "404"))

(run-server (api #'all-routes) {:port 8010})

