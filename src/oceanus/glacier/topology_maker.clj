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
        nlp-id       (inc str-bolt-id)
        spitter-id   (inc nlp-id)]
    ; There are x spouts, where x is count of kafka topics consumed
    ;   1 or 0 string filter bolt
    ;   1 NLP bolt
    ;   1 spitter bolt
    (str "(defn mk-topology []\n"
         "  (topology\n"
         "    {\n"
         ;"    {\"1\" (spout-spec kafka-spout)}\n"
         (generate-spouts-spec spouts-count)
         "}\n"
         "    {\n"
      (if has-str-bolt
; if part
 (str
 (format "     \"%d\" (bolt-spec {%s} string-filter-bolt)\n" str-bolt-id (merge-spouts spouts-count))
 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} nlp-bolt)\n" nlp-id str-bolt-id))
; else part
 (format "     \"%d\" (bolt-spec {%s} nlp-bolt)\n" nlp-id (merge-spouts spouts-count))
         )
; end of if

 (format "     \"%d\" (bolt-spec {\"%d\" :shuffle} mq-spitter-bolt)\n" spitter-id nlp-id)
         "  }))\n\n")
  ))

