(ns oceanus.glacier.pass-filter-maker
  (:gen-class))

(defn generate-pass-bolt
  []
  (str 
    "(defbolt pass-filter [\"passed-weibo\"] [tuple collector]\n"
    "  (let [info-map (.getValue tuple 0)]\n"
    "    (if (= true (info-map :passed))\n"
    "      (do\n"
    "        (emit-bolt! collector [info-map] :anchor tuple)\n"
    "        (ack! collector tuple)))))\n\n"))

