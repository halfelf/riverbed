(ns oceanus.glacier.str-bolts-maker
  (:gen-class))

;(defbolt name output-declaration *option-map & impl)
;
;(defbolt word-count ["word" "count"] {:prepare true}
;  [conf context collector]
;  (let [counts (atom {})]
;    (bolt
;     (execute [tuple]
;       (let [word (.getString tuple 0)]
;         (swap! counts (partial merge-with +) {word 1})
;         (emit-bolt! collector [word (@counts word)] :anchor tuple)
;         (ack! collector tuple)
;         )))))
;
;(defbolt split-sentence ["word"] [tuple collector]
;    (let [words (.split (.getString tuple 0) " ")]
;          (doseq [w words]
;                  (emit-bolt! collector [w] :anchor tuple))
;          (ack! collector tuple)
;          ))


(defn include-all-maker
  [words condition]
  (str
        (format "(defbolt %s-include-all [\"include-all\"] [tuple collector]\n" condition)
    (if (= "and" condition)
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)]\n"
        (format "    (if (every? (set (weibo :seged-text)) %s)\n" words)
                "      (do\n"
                "        (emit-bolt! collector [weibo] :anchor tuple)\n"
                "        (ack! collector tuple)))))\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
        (format "        passed (boolean (every? (set (weibo :seged-text)) %s))\n" words)
                "        new-record (merge {:passed passed} weibo)]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n")
      )))

(defn include-any-maker
  [words condition]
  (str
        (format "(defbolt %s-include-any [\"include-all\"] [tuple collector]\n" condition)
    (if (= "and" condition)
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)]\n"
        (format "    (if (some (set (weibo :seged-text)) %s)\n" words)
                "      (do\n"
                "        (emit-bolt! collector [weibo] :anchor tuple)\n"
                "        (ack! collector tuple)))))\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
        (format "        passed (boolean (some (set (weibo :seged-text)) %s))\n" words)
                "        new-record (merge {:passed passed} weibo)]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n")
      )))

(defn exclude-any-maker
  [words condition]
  (str
        (format "(defbolt %s-exclude-any [\"include-all\"] [tuple collector]\n" condition)
    (if (= "and" condition)
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)]\n"
        (format "    (if-not (some (set (weibo :seged-text)) %s)\n" words)
                "      (do\n"
                "        (emit-bolt! collector [weibo] :anchor tuple)\n"
                "        (ack! collector tuple)))))\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
        (format "        passed (boolean (some (set (weibo :seged-text)) %s))\n" words)
                "        new-record (merge {:passed (not passed)} weibo)]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n")
      )))

