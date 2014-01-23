(ns oceanus.glacier.segmentation-bolt-maker
  (:gen-class))


(defn generate-seg-bolt
  []
  (str 
    "(defbolt segmentation-bolt [\"seged\"] [tuple collector]\n"
    "  (let [info-map (.getValue tuple 0)\n"
    "        words (zh-segmentation (info-map :lite))\n"
    "        not-digit (vec (filter #(not (re-matches #\"\\d+\" %)) words))\n"
    ; no digit until we can segment text like "1945年" as a whole word
    "        new-record (merge info-map {:seg not-digit})]\n"
    "    (if-not (nil? not-digit)\n"
    "      (emit-bolt! collector [new-record] :anchor tuple))\n"
    "    (ack! collector tuple)))\n\n"))

