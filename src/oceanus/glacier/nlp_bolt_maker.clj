(ns oceanus.glacier.nlp-bolt-maker
  (:gen-class))

(defn generate-nlp-bolt
  [] 
  (str 
        "(defbolt nlp-bolt [\"after-nlp\"] [tuple collector]\n" 
        "  (let [info-map   (.getValue tuple 0)\n"
        "        ttype      (info-map :ttype)\n"
        "        raw_txt    (info-map :txt)\n"
        "        sim-tag    (case ttype\n"
        "                     \"qqweibo\" \"tw\"\n"
        "                     \"youku\"   \"yk\"\n"
        "                     \"sw\")\n"
        "        st         (zh-sentiment raw_txt (ids-keys (info-map :kid)))\n"
        "        is-ad      (ads-recognize raw_txt)\n"
        "        lite-one   (-> raw_txt\n"
        "                     (string/replace\n"
        "                       #\"(\\[\\p{L}+\\]|(https?|ftp)://(-\\.)?([^\\s/?\\.#-]+\\.?)+(/[^\\s]*)?|@.+?(?=\\s)|#|//@.+?[:\\s])\" \"\"))\n"
                 ; no face expression, no links, no @someone, no sharp sign #, no forwarding //@someone...
        "        words      (zh-segmentation lite-one)\n"
        "        no-shits   (vec (filter #(not (re-matches #\"[？：；“”！。，\\\"'?:;!.,_\\s\\d]+\" %)) words))\n"
        ; no digit until we can segment text like "1945年" as a whole word
        "        no-single  (vec (filter #(not= 1 (count %)) no-shits))\n"
 ;       "        log-source (keyword (get-in info-map [:general :source]))\n"
 ;       "        log-type   (keyword (get-in info-map [:general :type]))\n"
        "        [unique sim-id] (similar-judge\n"
        "                      lite-one\n" 
        "                      (info-map :_id)\n" 
        "                      sim-tag\n"
        "                      (:tid info-map))\n"
;        "                      (get-in data-format [log-source log-type :sim-tag]))\n"
        "        new-record (merge info-map {:st st\n" 
        "                                    :seg no-single\n" 
        "                                    :ad is-ad\n" 
        "                                    :sim sim-id\n"
        "                                    :uniq unique})]\n"
        "    (if-not (empty? no-single)\n"
        "      (emit-bolt! collector [new-record] :anchor tuple))\n"
        "    (ack! collector tuple)))\n\n"
    ))

