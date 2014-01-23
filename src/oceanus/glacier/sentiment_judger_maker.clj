(ns oceanus.glacier.sentiment-judger-maker
  (:gen-class))

(defn generate-sentiment-judger
  [target serial-number]
  (str 
    (format "(defbolt sentiment-judger-%d [\"sentiment-txt\"] [tuple collector]\n" serial-number)
            "  (let [info-map (.getValue tuple 0)\n"
    (format "        st (zh-sentiment (info-map :txt) \"%s\")\n" target)
            "        new-record (merge info-map {:st st})]\n"
            "    (emit-bolt! collector [new-record] :anchor tuple)\n"
            "    (ack! collector tuple)))\n\n"
    ))

