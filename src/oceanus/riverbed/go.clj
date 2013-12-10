(ns oceanus.riverbed.go
  ;(:require [oceanus.glacier :as :decex] 
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

  "a topo spec" 
  {:tpid     123456
   :bolts    {:include-any "text"  ; bolt-type field-name
              :exclude-any "text"} 
            ; auto generating segmentation and counter bolts
   :relation :and
   }



(defn to-node-with-seg
  "accept type, topo and component id, input/output spec, give node specs"
  [bolt-type tpid component-id input-map output]
  [{:type "bolt"
    :name (format "segment-%s-%d" tpid component-id)
    :impl "segmentation"
    :serial (str component-id)
    :parameters []
    :input nil
    :output nil
    :dop 1
    }
   {:type "bolt"
   :name (format "%s-%s-%d" bolt-type tpid (inc component-id))
   :impl (str bolt-type)
   :serial (str (inc component-id))
   :parameters []  ; custom later
   :input input-map
   :output output
   :dop 1  ; custom later
   }]
  )


(defmulti to-nodes 
  "Turn a topo spec hash map into a vector of nodes spec"
  :relation)
(defmethod to-nodes :and [spec]
  (let [tpid    (:tpid spec)
        filters (vec (:bolts spec))]
    (loop [sq filters res [] serial 2]  ; 2 should be bolt start serial later
      (if (empty? sq)
        res
        (let [current (first sq)]
          ; [:include-any "text"]        
          (recur (rest sq) 
                 (conj res (to-node-with-seg (current 0) tpid serial))
                 (+ 2 serial))
  )))))
(defmethod to-nodes :or [spec]
  )

(defn go
  [spec]
  (let [nodes (to-nodes spec)]
    )
  )
         )
