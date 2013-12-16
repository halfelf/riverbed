(ns oceanus.glacier.pass-filter-maker
  (:gen-class))

(defn generate-pass-bolt
  []
  (str 
    "(defbolt pass-filter [\"passed-weibo\"] [tuple collector]\n"
    "  (let [weibo (.getValue tuple 0)]\n"
    "    (if (= true (weibo :passed))\n"
    "      (do\n"
    "        (emit-bolt! collector [weibo] :anchor tuple)\n"
    "        (ack! collector tuple)))))\n\n"))

