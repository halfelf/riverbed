(ns oceanus.glacier.similar-tagger-maker
  (:gen-class))

(defn generate-similar-tagger
  [source-type]
  (let [field (cond 
                (= source-type :sinaweibo) "_id"
                :else "id")]
    (str 
              "(defbolt similar-tagger [\"maybe-similar\"] [tuple collector]\n"
              "  (let [info-map      (.getValue tuple 0)\n"
      (format "        is-similar (similar-judge (info-map :lite) (info-map :%s) \"%s\")\n" field source-type)
              "        new-record (merge info-map {:sim is-similar})]\n"
              "    (emit-bolt! collector [new-record] :anchor tuple)\n"
              "    (ack! collector tuple)))\n\n"))
    )

