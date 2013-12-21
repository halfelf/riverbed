(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:gen-class))

(defn generate-filters-spec
  [id condition words-map]
  (loop [bolts-spec   ""
         current-id   id 
         rest-filters (keys words-map)
         filter-empty (empty? (words-map (first rest-filters)))]
    (if (empty? rest-filters)
      [current-id bolts-spec]
      (recur (str bolts-spec 
                  (if filter-empty
                    nil
                    (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n                     %s)\n"
                            current-id 
                            (dec current-id)
                            (str condition "-" (name (first rest-filters))))))
             (if filter-empty
               current-id 
               (inc current-id))
             (rest rest-filters)
             (empty? (words-map (first (rest rest-filters))))))))


(defn generate-spouts-spec
  [spouts-count]
  (->> (map #(format
               "     \"%d\" (spout-spec kafka-spout-%d)"
               % %)
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
          (take spouts-count (iterate inc 1)))
       (clojure.string/join "\n")))

         
(defn generate-topology
  "Generates whole topology DSL from a hashmap."
  [topo-map]
  (let [words-map    (select-keys topo-map 
                                 [:include-any :include-all :exclude-any])
        condition    (topo-map :conditions) ; should be "or" or "and"
        spouts-count (count (topo-map :keywords))
        current-id   (if (= "and" condition)  ; skip some id which
                       (+ 3 spouts-count)     ;   reserved for tid-adder and
                       (+ 4 spouts-count))    ;   segmentation and pass-tag-adder
        [after-filter-id filters-str] (generate-filters-spec
                                        current-id condition words-map)]
    (str "(defn mk-topology []\n"
         "  (topology\n"
         "    {\n"
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
         (generate-spouts-spec spouts-count)
         "}\n"
         (format
         "    {\"%d\" (bolt-spec {"
           (inc spouts-count))
         (reduce #(str %1 "\"" %2 "\" :shuffle ")  
                 "" 
                 (take 3 (iterate inc 1)))  ; generate several "%d :shuffle"
         "}\n"
         "                     tid-adder)\n"
         (format
         "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
           (+ 2 spouts-count) (inc spouts-count))
         "                     segmentation-bolt)\n"
         (if (= "or" condition)
           (str 
             (format
             "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n" 
               (+ 3 spouts-count) (+ 2 spouts-count))
             "                    pass-tag-adder)\n"))
         filters-str
         (if (= "or" condition)
           (str
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n" 
                     after-filter-id (dec after-filter-id))
               "                    pass-filter)\n"
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                     (inc after-filter-id) after-filter-id)
               "                     mq-spitter-bolt)\n")
           (str 
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                     after-filter-id (dec after-filter-id))
               "                     mq-spitter-bolt)\n"))
         "  }))\n\n")
  ))

