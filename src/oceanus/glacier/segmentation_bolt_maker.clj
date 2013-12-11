(ns oceanus.glacier.segmentation-bolt-maker
  (:gen-class))


(defn generate-seg-bolt
  [field-to-seg]
  (str 
            "(defbolt segmentation-bolt \"seged-weibo\" [tuple collector]\n"
            "  (let [weibo-map (.getValue tuple 0)\n"
    (format "        words (zh-segmentation (weibo-map \"%s\"))\n" field-to-seg)
    (format "        new-record (merge {:%s-seged words} weibo-map)]\n" field-to-seg)
            "    (emit-bolt! collector [new-record] :anchor tuple)\n"
            "    (ack! collector tuple)))\n"))

