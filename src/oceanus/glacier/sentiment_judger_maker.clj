(ns oceanus.glacier.sentiment-judger-maker
  (:gen-class))

(defn generate-sentiment-judger
  []
  (str 
    "(defbolt sentiment-judger [\"sentiment-txt\"] [tuple collector]\n" 
    "  (let [info-map (.getValue tuple 0)\n"
    "        st (zh-sentiment (info-map :txt) (ids-keys (info-map :keyid)) )\n"
    "        new-record (merge info-map {:st st})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"
    ))

