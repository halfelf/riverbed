(ns oceanus.glacier.str-bolts-maker
  (:use [clojure.string :only [join]])
  (:import java.util.regex.Pattern)
  (:gen-class))


(defn string-filter-maker
  [topo-spec]
  (let [condition (topo-spec :conditions)
        prediction (if (= "and" condition) "every" "some")
        include-any (topo-spec :include-any)
        include-all (topo-spec :include-all)
        exclude-any (topo-spec :exclude-any)]
    (str
              "(defbolt string-filter-bolt [\"filtered\"] [tuple collector]\n"
              "  (let [info-map (.getValue tuple 0)\n"
              "        text     (info-map :txt)]\n"
      (format "    (if (%s? true?\n" prediction)
              "          [\n"
            (if-not (empty? include-any)
      (format "           (boolean (re-find #\"%s\" text))\n" 
              (join "|" (map #(. Pattern quote %) include-any))))
            (if-not (empty? include-all)
      (format "           (boolean (re-find #\"%s\" text))\n" 
              (join (map #(str "(?=.*" (. Pattern quote %) ")") include-all))))
            (if-not (empty? exclude-any)
      (format "           (not (boolean (re-find #\"%s\" text)))\n" 
              (join "|" (map #(. Pattern quote %) exclude-any))))
              "          ])\n"
              "      (do\n"
              "        (emit-bolt! collector [info-map] :anchor tuple)\n"
              "        (ack! collector tuple)))))\n\n"
      )))

