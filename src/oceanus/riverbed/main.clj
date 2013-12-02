(ns oceanus.riverbed.main
  (:require [compojure.route :as route])
  (:use compojure.core
        compojure.handler
        org.httpkit.server))

(defn hello-handler [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body    "hello HTTP!"})


(defroutes all-routes
  (GET "/" [] hello-handler)
;  (context "/topology/:id" []
;           (GET "/" [] get-topology-by-id)
;           (POST "/" [] update-topology))
  (route/not-found "404"))

(run-server (api #'all-routes) {:port 8010})

;(defn app [req]
;  {:status  200
;   :headers {"Content-Type" "text/html"}
;   :body    "hello HTTP!"})
;(run-server app {:port 8010})

