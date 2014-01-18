(ns oceanus.glacier.sundries-extractor-maker
  (:gen-class))

(defn generate-sundries-extractor
  []
  (str 
    "(defbolt sundries-extractor [\"without-sundries\"] [tuple collector]\n"
    "  (let [weibo (.getValue tuple 0)\n"
    "        lite-one (-> weibo :txt\n"
    "                     (string/replace\n"
    "                       #\"(\\[\\p{L}+\\]|http://t\\.cn/[\\d\\w]+|@\\p{L}+(?=\\s)|#|//@\\p{L}+:)\" \"\"))\n"
    "        new-record (merge weibo {:lite lite-one})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"))

