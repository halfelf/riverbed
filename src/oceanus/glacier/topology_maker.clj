(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:gen-class))

(defn generate-filters-spec
  [id condition words-map]
  (loop [bolts-spec   ""
         current-id   id 
         rest-filters (keys words-map)]
    (if (empty? rest-filters)
      [current-id bolts-spec]
      (recur (str bolts-spec 
                  (if (empty? (words-map (first rest-filters)))
                    nil
                    (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n                     %s)\n"
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
        current-id  (if (= "and" condition) 3 4)
        [after-filter-id filters-str] (generate-filters-spec
                                        current-id condition words-map)]
    (str "(defn mk-topology []\n"
         "  (topology\n"
         "    {\"1\" (spout-spec kafka-spout)}\n"
         "    {\"2\" (bolt-spec {\"1\" :shuffle}\n"
         "                  segmentation-bolt)\n"
         (if (= "or" condition)
           (str "     \"3\" (bolt-spec {\"2\" :shuffle}\n" 
                "                    pass-tag-adder)\n"))
         filters-str
         (if (= "or" condition)
           (str
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n" 
                     after-filter-id (dec after-filter-id))
               "                    pass-filter)\n"
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                     (inc after-filter-id) after-filter-id)
               "                   mq-spitter-bolt)\n")
           (str 
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                     after-filter-id (dec after-filter-id))
               "                   mq-spitter-bolt)\n"))
         "  }))\n\n")
  ))

