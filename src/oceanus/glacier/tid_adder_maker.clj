(ns oceanus.glacier.tid-adder-maker
  (:gen-class))

(defn generate-tid-bolt
  [tid]
  (str 
            "(defbolt tid-adder [\"with-tid\"] [tuple collector]\n"
            "  (let [weibo (.getValue tuple 0)\n"
    (format "        new-record (merge weibo {:tid %d})]\n" tid)
            "    (emit-bolt! collector [new-record] :anchor tuple)\n"
            "    (ack! collector tuple)))\n\n"))

