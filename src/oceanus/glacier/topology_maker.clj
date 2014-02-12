(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:gen-class))

(defn- generate-filters-spec
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


(defn- generate-spouts-spec
  [spouts-count]
  (->> (map #(format
               "     \"%d\" (spout-spec kafka-spout-%d)"
               % %)
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
          (take spouts-count (iterate inc 1)))
       (clojure.string/join "\n")))


(defn- generate-sentiment-spec
  [spouts-count]
  (->> (map #(format
               "     \"%d\" (bolt-spec {\"%d\" :shuffle} sentiment-judger-%d)"
               (+ spouts-count %) % %)
         ;"    \"6\" (bolt-spec {\"1\" :shuffle} sentiment-bolt-%d)\n"
          (take spouts-count (iterate inc 1)))
       (clojure.string/join "\n")))

         
(defn generate-topology
  "Generates whole topology DSL from a hashmap."
  [topo-map]
  (let [words-map    (select-keys topo-map 
                                 [:include-any :include-all :exclude-any])
        condition    (topo-map :conditions) ; should be "or" or "and"
        spouts-count (count (topo-map :keywords))
        tid-adder-id (-> spouts-count (* 2) (+ 1))
        sundry-id    (inc tid-adder-id)
        seg-bolt-id  (inc sundry-id)
        pass-tag-id  (inc seg-bolt-id) 
        current-id   (if (= "and" condition) 
                       (inc seg-bolt-id)    
                       (inc pass-tag-id))  
        [after-str-filter-id filters-str] (generate-filters-spec
                                        current-id condition words-map)
        after-filter-id (if (= "and" condition)
                          after-str-filter-id
                          (inc after-str-filter-id))]
    ; There are x spouts, where x is count of keywords crawled
    ;   1 or 0 string filter bolt
    ;   1 sentiment bolt
    ;   1 tid adder bolt, 
    ;   1 sundries extractor bolt
    ;   1 segmentation bolt, 
    ;   1 ad tagger bolt
    ;   1 similar text tagger bolt
    ;   1 spitter bolt
    (str "(defn mk-topology []\n"
         "  (topology\n"
         "    {\n"
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
         (generate-spouts-spec spouts-count)
         "}\n"
         "    {\n"
         (generate-sentiment-spec spouts-count)
         (format
         "\n     \"%d\" (bolt-spec {"
           tid-adder-id)
         (reduce #(str %1 "\"" %2 "\" :shuffle ")  
                 ""  
                 ; generate several "%d :shuffle"
                 (take spouts-count (iterate inc (inc spouts-count)))) 
         "}\n"
         "                     tid-adder)\n"
         (format
         "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
           sundry-id tid-adder-id)
         "                     sundries-extractor)\n"
         (format
         "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
           seg-bolt-id sundry-id)
         "                     segmentation-bolt)\n"
         (if (= "or" condition)
           (str 
             (format
             "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n" 
               pass-tag-id seg-bolt-id)
             "                    pass-tag-adder)\n"))
         filters-str

         ; then the may-exists pass-tag-filter
         (if (= "or" condition)
           (str 
             (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n" 
                     after-str-filter-id (dec after-str-filter-id))
             "                    pass-filter)\n"))
         
         ; ad-tagger
         (str
           (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                   after-filter-id (dec after-filter-id))
           "                     ads-tagger)\n")

         ; similar-tagger
         (str
           (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                   (inc after-filter-id) after-filter-id)
           "                     similar-tagger)\n")

         ; spitter
         (str
           (format "     \"%d\" (bolt-spec {\"%d\" :shuffle}\n"
                   (+ 2 after-filter-id) (inc after-filter-id))
;                   (inc after-filter-id) after-filter-id)
           "                     mq-spitter-bolt)\n")
         "  }))\n\n")
  ))

