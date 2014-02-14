(ns oceanus.riverbed.constants
  (:gen-class))

(def no-such-topo 
     {:status  404
      :headers {"Content-Type" "text/plain"}
      :body    "no such topo"})
(def received
     {:status  200
      :headers {"Content-Type" "text/plain"}
      :body    "received"})
(def conflict
     {:status  409
      :headers {"Content-Type" "text/plain"}
      :body    "conflict"})
(def wrong-type
     {:status  404
      :headers {"Content-Type" "text/plain"}
      :body    "no such log type"})

