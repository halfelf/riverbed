(ns oceanus.glacier.sundries-extractor-maker
  (:gen-class))

(defn generate-sundries-extractor
  []
  (str 
    "(defbolt sundries-extractor [\"without-sundries\"] [tuple collector]\n"
    "  (let [info-map (.getValue tuple 0)\n"
    "        lite-one (-> info-map :txt\n"
    "                     (string/replace\n"
    "                       #\"(\\[\\p{L}+\\]|(https?|ftp)://(-\\.)?([^\\s/?\\.#-]+\\.?)+(/[^\\s]*)?|@.+?(?=\\s)|#|//@.+?[:\\s])\" \"\"))\n"
    "        new-record (merge info-map {:lite lite-one})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"))

