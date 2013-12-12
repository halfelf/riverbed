(ns oceanus.riverbed.go
  (:require [clojure.string :as string])
  (:use [clojure.java.shell :only [sh]])
  (:import [java.io File])
  (:import [org.apache.commons.io FileUtils])
  (:require [oceanus.glacier [topology-maker :as topology-maker]
                             [str-bolts-maker :as str-bolts-maker]])
  (:gen-class))

    ;{:name "test", :conditions "and", :not_keywords "bla", 
    ;  :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14}

(defn go-topo
  "Generate a project dir, define topo, define nodes, run"
  [spec]
  (let [topo-name (spec :name)
        topo-spec (replace {:or_keywords  :include-any
                            :and_keywords :include-all
                            :not_keywords :exclude-any}
                    (-> spec
                      (update-in [:or_keywords]  #(string/split % #"\s"))
                      (update-in [:and_keywords] #(string/split % #"\s"))
                      (update-in [:not_keywords] #(string/split % #"\s"))))
        topo-def  (topology-maker/generate-topology spec)]
    (FileUtils/copyDirectory 
      (File. "resources/skeleton") 
      (File. (format "/streaming/%s/" topo-name) false)) ; not copy FileDate
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

