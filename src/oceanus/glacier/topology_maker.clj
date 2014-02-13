(ns oceanus.glacier.topology-maker
  (:require [cheshire.core :refer :all])
  (:require [clojure.string :as string])
  (:gen-class))


(defn- generate-spouts-spec
  [spouts-count]
  (->> (map #(format
               "     \"%d\" (spout-spec kafka-spout-%d)"
               % %)
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
          (take spouts-count (iterate inc 1)))
       (clojure.string/join "\n")))


(defn- merge-spouts
  [spouts-count]
  (->> (iterate inc 1) 
       (take spouts-count) 
       (map #(format "\"%d\" :shuffle" %)) 
       (string/join " ")))


(defn generate-topology
  "Generates mk-topology DSL from a hashmap."
  [topo-map]
  (let [has-str-bolt (not-every? empty?
                       (->> [:include-any :include-all :exclude-any]
                            (select-keys topo-map)
                            vals))
        condition    (topo-map :conditions) ; should be "or" or "and"
        spouts-count (count (topo-map :keywords))
        str-bolt-id  (if has-str-bolt (inc spouts-count) spouts-count)
        sentiment-id (inc str-bolt-id)
        tid-adder-id (inc sentiment-id)
        sundry-id    (inc tid-adder-id)
        seg-id       (inc sundry-id)
        ad-id        (inc seg-id)
        sim-id       (inc ad-id)
        spitter-id   (inc sim-id)]
    ; There are x spouts, where x is count of kafka topics consumed
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
      (if has-str-bolt
 (format "     \"%d\" (bolt-spec {%s} string-filter-bolt)\n" str-bolt-id (merge-spouts spouts-count))
         )

 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} sentiment-judger)\n" sentiment-id str-bolt-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} tid-adder)\n" tid-adder-id sentiment-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} sundries-extractor)\n" sundry-id tid-adder-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} segmentation-bolt)\n" seg-id sundry-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} ads-tagger)\n" ad-id seg-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} similar-tagger)\n" sim-id ad-id)
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} mq-spitter-bolt)\n" spitter-id sim-id)
         "  }))\n\n")
  ))

