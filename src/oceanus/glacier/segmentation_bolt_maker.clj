(ns oceanus.glacier.segmentation-bolt-maker
  (:gen-class))


(defn generate-seg-bolt
  []
  (str 
            "(defbolt segmentation-bolt [\"seged-weibo\"] [tuple collector]\n"
            "  (let [weibo-map (.getValue tuple 0)\n"
            "        words (zh-segmentation (weibo-map :lite))\n"
            "        new-record (merge weibo-map {:seg words})]\n"
            "    (if-not (nil? words)\n"
            "      (emit-bolt! collector [new-record] :anchor tuple))\n"
            "    (ack! collector tuple)))\n\n"))

