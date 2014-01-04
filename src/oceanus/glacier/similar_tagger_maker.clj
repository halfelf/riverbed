(ns oceanus.glacier.similar-tagger-maker
  (:gen-class))

(defn generate-similar-tagger
  []
  (str 
    "(defbolt similar-tagger [\"maybe-similar\"] [tuple collector]\n"
    "  (let [weibo      (.getValue tuple 0)\n"
    "        is-similar (similar-judge (weibo \"txt\") (weibo \"_id\"))\n"
    "        new-record (merge weibo {:sim is-similar})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"))

