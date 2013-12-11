(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:require [clojure.string :as string])
  (:require clojure.walk)
  (:gen-class))

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
             (rest rest-filters)))))

;{:name "test", :conditions "and", :not_keywords "bla", 
; :or_keywords "bar", :and_keywords "foo", :in_user_id 1, :id 14}
         
(defn generate-topology
  "Generates whole topology DSL from a hashmap."
  [topo-map]
  (let [words-map   (select-keys topo-map 
                                 [:include-any :include-all :exclude-any])
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

