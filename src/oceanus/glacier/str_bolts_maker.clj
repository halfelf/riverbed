(ns oceanus.glacier.str-bolts-maker
  (:gen-class))

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
                "        (ack! collector tuple)))))\n\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
                "        passed (or (weibo :passed) \n"
        (format "          (boolean (every? (set (weibo :seged-text)) %s)))\n" words)
                "        new-record (merge weibo {:passed passed})]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n\n")
      )))

(defn include-any-maker
  [words condition]
  (str
        (format "(defbolt %s-include-any [\"include-any\"] [tuple collector]\n" condition)
    (if (= "and" condition)
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)]\n"
        (format "    (if (some (set (weibo :seged-text)) %s)\n" words)
                "      (do\n"
                "        (emit-bolt! collector [weibo] :anchor tuple)\n"
                "        (ack! collector tuple)))))\n\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
                "        passed (or (weibo :passed) \n" 
        (format "          (boolean (some (set (weibo :seged-text)) %s)))\n" words)
                "        new-record (merge weibo {:passed passed})]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n\n")
      )))

(defn exclude-any-maker
  [words condition]
  (str
        (format "(defbolt %s-exclude-any [\"exclude-any\"] [tuple collector]\n" condition)
    (if (= "and" condition)
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)]\n"
        (format "    (if-not (some (set (weibo :seged-text)) %s)\n" words)
                "      (do\n"
                "        (emit-bolt! collector [weibo] :anchor tuple)\n"
                "        (ack! collector tuple)))))\n\n")
      ;;;;;;;;;;;;;;;;;;;;;;;;;;
      (str      "  (let [weibo (.getValue tuple 0)\n"
                "        passed (or (weibo :passed) \n"
        (format "          (not (boolean (some (set (weibo :seged-text)) %s))))\n" words)
                "        new-record (merge weibo {:passed passed})]\n" 
                "    (emit-bolt! collector [new-record] :anchor tuple)\n"
                "    (ack! collector tuple)))\n\n")
      )))

