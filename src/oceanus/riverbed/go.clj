(ns oceanus.riverbed.go
  ;(:require [oceanus.glacier :as :decex] 
  (:use [clojure.java.shell :only [sh]])
  (:import [java.io File])
  (:import [org.apache.commons.io FileUtils])
  (:require [oceanus.glacier [topology-maker :as topology-maker]
                             [str-bolts-maker :as str-bolts-maker]])
  (:gen-class))

(comment   ; a node spec map
  {:type        "bolt"
   :name        "my-bolt"
   :impl        "word-count"  
   :serial      "3" 
   :parameters  ["blabla" "blablabla"]
   :input       {"1" "shuffle" "2" ["word"]}
   :output      ["passed" "failed"]
   :dop         4 }     ;degree of parallelism
         )

    ;{:name "test", :conditions "and", :not_keywords "bla", 
    ;  :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14}

(defn go-topo
  "Generate a project dir, define topo, define nodes, run"
  [spec]
  (let [topo-name (spec :name)]
    (FileUtils/copyDirectory 
      (File. "resources/skeleton") 
      (File. (format "/streaming/%s/" topo-name) false)) ; not copy FileDate
    (topology-maker/generate-topology spec)
  ))

;(defmulti to-nodes 
;  "Turn a topo spec hash map into a vector of nodes spec"
;  :relation)
;(defmethod to-nodes :and [spec]
;  (let [tpid    (:tpid spec)
;        filters (vec (:bolts spec))]
;    (loop [sq filters res [] serial 2]  ; 2 should be bolt start serial later
;      (if (empty? sq)
;        res
;        (let [current (first sq)]
;          ; [:include-any "text"]        
;          (recur (rest sq) 
;                 (conj res (to-node-with-seg (current 0) tpid serial))
;                 (+ 2 serial))
;  )))))
;(defmethod to-nodes :or [spec]
;  )

