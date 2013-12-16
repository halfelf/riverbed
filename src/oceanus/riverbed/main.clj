(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require [clojure.java.jdbc :as jdbc])
  (:use compojure.core
        compojure.handler
        org.httpkit.server)
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
  (let [topo-name (:tpname (:route-params req))
        query-string (format "select * from %s where name=\"%s\""
                             topo-table
                             topo-name)
        topo-spec (first (jdbc/query mysql-db [query-string]))]
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    ;({:name "test", :conditions "and", :not_keywords "bla", 
    ;  :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14})
    ; TODO
    ; generate and run topo here
    ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
    {:status 201
     :headers {"Content-Type" "text/plain"}
     :body "created"}
  ))

;  (let [body (parse-string (String. (.bytes (:body req))))

(defn get-topology-by-name
  [req]
  (let [topo-name (:tpname (:route-params req))
        topo-spec (jdbc/query mysql-db
                    [(format "select * from %s where name=\"%s\"" 
                             topo-table
                             topo-name)])]
    (prn topo-spec)  
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (generate-string topo-spec)}
  ))

(defroutes all-routes
  (GET "/" [] hello-handler)
  (context "/topology/:tpname" []
           (GET "/" [] get-topology-by-name)
           (POST "/" [] generate-topology))
  (route/not-found "404"))

;(run-server (api #'all-routes) {:port 8010})

