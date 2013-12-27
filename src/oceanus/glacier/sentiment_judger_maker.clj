(ns oceanus.glacier.sentiment-judger-maker
  (:gen-class))

(defn generate-sentiment-judger
  [target serial-number]
  (str 
    (format "(defbolt sentiment-judger-%d [\"sentiment-weibo\"] [tuple collector]\n" serial-number)
            "  (let [weibo (.getValue tuple 0)\n"
    (format "        st (zh-sentiment (weibo \"txt\") \"%s\")\n" target)
            "        new-record (merge weibo {:st st})]\n"
            "    (emit-bolt! collector [new-record] :anchor tuple)\n"
            "    (ack! collector tuple)))\n\n"
    ))

