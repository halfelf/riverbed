(ns oceanus.glacier.segmentation-bolt-maker
  (:gen-class))


(defn generate-seg-bolt
  [field-to-seg]
  (str 
    (format "(defbolt seg-%s \"word\" [tuple collector]\n" field-to-seg)
            "  (let [info-dict (.getValue tuple 0)\n"
    (format "        words (zh-segmentation (info-dict \"%s\"))]\n" field-to-seg)
    (format "        new-record (merge {\"%s-seged\" words} info-dict)]\n" field-to-seg)
            "    (emit-bolt! collector [new-record] :anchor tuple)\n"
            "    (ack! collector tuple)))\n"))

;(defbolt split-sentence ["word"] [tuple collector]
;  (let [words (zh-segmentation (.getString tuple 0))]
;    (doseq [w words]
;      (emit-bolt! collector [w] :anchor tuple))
;    (ack! collector tuple)
;    ))
