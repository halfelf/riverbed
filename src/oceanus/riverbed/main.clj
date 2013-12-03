(ns oceanus.riverbed.main
  (:require [cheshire.core :refer :all])
  (:require [compojure.route :as route])
  (:require monger.core
            monger.collection)
  (:import [org.bson.types ObjectId]
           [com.mongodb DB WriteConcern])
  (:use compojure.core
        compojure.handler
        org.httpkit.server))

(defn hello-handler [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "This is dataminr riverbed server."})

(defn update-topology 
  [req]
  (let [body (parse-string (String. (.bytes (:body req))))
        tpid-map {:tpid (Integer. (:tpid (:route-params req)))}]
    (try
      (monger.core/connect! {:host "store" :port 27017})
      (monger.core/set-db! (monger.core/get-db "topologies"))
      (monger.collection/update "current_running" 
                                tpid-map 
                                (merge tpid-map body) 
                                :upsert true)
      {:status  201
       :headers {"Content-Type" "text/plain"}
       :body    "update ok"} 
      (catch Exception e (str "caught exception: " (.getMessage e)))
      (finally (monger.core/disconnect!))))
  )

(defn get-topology-by-id
  [req]
  (try
    (monger.core/connect! {:host "store" :port 27017})
    (monger.core/set-db! (monger.core/get-db "topologies"))
    (let [tpid-map {:tpid (Integer. (:tpid (:route-params req)))}
          topo-spec (monger.collection/find-one "current_running" tpid-map)]
      {:status  200
       :headers {"Content-Type" "text/plain"}
       :body    (str topo-spec)})
    (catch Exception e (str "caught exception: " (.getMessage e)))
    (finally (monger.core/disconnect!)))
  )

(defroutes all-routes
  (GET "/" [] hello-handler)
  (context "/topology/:tpid" []
           (GET "/" [] get-topology-by-id)
           (POST "/" [] update-topology))
  (route/not-found "404"))

(run-server (api #'all-routes) {:port 8010})

