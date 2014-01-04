(ns oceanus.glacier.ads-tagger-maker
  (:gen-class))

(defn generate-ads-tagger
  []
  (str 
    "(defbolt ads-tagger [\"maybe-ads\"] [tuple collector]\n"
    "  (let [weibo (.getValue tuple 0)\n"
    "        is-ad (ads-recognize (weibo \"txt\"))\n"
    "        new-record (merge weibo {:ad is-ad})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"))

