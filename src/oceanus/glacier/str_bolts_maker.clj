(ns oceanus.glacier.str-bolts-maker
  (:use [clojure.string :only [join]])
  (:gen-class))


(defn string-filter-maker
  [topo-spec]
  (let [condition (topo-spec :conditions)
        include-any (topo-spec :include-any)
        include-all (topo-spec :include-all)
        exclude-any (topo-spec :exclude-any)
        prediction (if (= "and" condition) "every" "some")]
              "(defn include-any [text words]\n"
      (format "  (if (re-find #\"(%s)\" text)\n" (clojure.string/join "|" words))
              "    true\n"
              "    false)\n\n"

              "(defbolt string-filter-bolt [\"filtered\"] [tuple collector]\n"
              "  (let [info-map (.getValue tuple 0)\n"
              "        text     (info-map :txt)]\n"
      (format "    (if (%s? true?\n" prediction)
              "          [\n"

(join "|" (map #(. Pattern quote %) b))


            (if-not? (empty? include-any)
      (format "           (boolean (re-find #\"%s\" text))\n" (join "|" include-any)))
            (if-not? (empty? include-all)
      (format "           (boolean (re-find #\"%s\" text))\n" (join (map #(str "(?=.*" % ")") include-any))))
            (if-not? (empty? exclude-any)
      (format "           (not (boolean (re-find #\"%s\" text)))\n" (join "|" exclude-any)))
              "          ])\n"
              "      (do\n"
              "        (emit-bolt! collector [info-map] :anchor tuple)\n"
              "        (ack! collector tuple)))))\n\n"
      ))

