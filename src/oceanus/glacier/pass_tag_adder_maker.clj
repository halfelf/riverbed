(ns oceanus.glacier.pass-tag-adder-maker
  (:gen-class))

(defn generate-tag-bolt
  []
  (str 
    "(defbolt pass-tag-adder [\"tagged-weibo\"] [tuple collector]\n"
    "  (let [weibo (.getValue tuple 0)\n"
    "        new-record (merge weibo {:passed false})]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n\n"))

