(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:require [clojure.string :as string])
  (:require clojure.walk)
  (:gen-class))

(comment   ; a node spec map
  [{:type        "bolt"
    :name        "my-bolt"
    :impl        "word-count"
    :serial      "3" 
    :parameters  ["blabla" "blablabla"]
    :input       {"1" "shuffle" "2" ["word"]}
    :dop         4 }];)     ;degree of parallelism
         )
         

           
(defmulti generate-spec :type)
(defmethod generate-spec "spout" [node]
  (str (format "\"%s\"" (node :serial))
       " (spout-spec "
    (if (nil? (node :parameters))
      (if (nil? (node :dop))
        (format "%s)" (node :name))
        (format "%s :p %d)" (node :name) (node :dop)))
      (if (nil? (node :dop))
        (format "(%s %s))"  (node :name) (node :parameters))
        (format "(%s %s) :p %d)" (node :name) (node :parameters) (node :dop))))))
(defmethod generate-spec "bolt" [node]
  (str (format "\"%s\"" (node :serial))
       (format " (bolt-spec %s %s"
               (clojure.walk/stringify-keys (node :input))
               (node :name))
       (if (nil? (node :dop))
         ")"
         (format " :p %d)" (node :dop)))))


(defn generate-filters-spec
  [id condition words-map]
  (loop [bolts-spec   ""
         current-id   id 
         rest-filters (keys words-map)]
    (if (empty? rest-filters)
      bolts-spec 
      (recur (str bolts-spec 
                  (if (empty? (words-map (first rest-filters)))
                    nil
                    (format "\"%d\" (bolt-spec {\"%d\" :shuffle}\n %s)\n"
                            current-id 
                            (dec current-id)
                            (str condition "-" (name (first rest-filters)))))) 
             (inc current-id) 
             (rest rest-filters))))
  )


;{:name "test", :conditions "and", :not_keywords "bla", 
; :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14}
         
(defn generate-topology
  "Generates whole topology DSL from a hashmap."
  [topo-map]
  (let [in-any      (string/split (topo-map :or_keywords)  #"\s")
        in-all      (string/split (topo-map :and_keywords) #"\s")
        ex-any      (string/split (topo-map :not_keywords) #"\s")
        words-map   {:include-any in-any
                     :include-all in-all
                     :exclude-any ex-any}
        condition   (topo-map :conditions) ; should be "or" or "and"
        current-id  (if (= "and" condition) 3 4)]
    (str "(topology\n"
         "  {\"1\" (spout-spec kafka-spout)}\n"
         "  {\"2\" (bolt-spec {\"1\" :shuffle}\n"
         "                  segmentation-bolt)\n"
         (if (= "and" condition)
           nil
           (str "   \"3\" (bolt-spec {\"2\" :shuffle}\n" 
                "                  pass-tag-adder)\n"))
         (generate-filters-spec current-id 
                                condition
                                words-map)
         "})"
  )))

