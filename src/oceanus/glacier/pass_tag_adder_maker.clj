(ns oceanus.glacier.pass-tag-adder-maker
  (:gen-class))

(defn generate-seg-bolt
  []
  (str 
    "(defbolt pass-tag-adder \"tagged-weibo\" [tuple collector]\n"
    "  (let [weibo-map (.getValue tuple 0)\n"
    "        new-record (merge {:pass false} weibo-map)]\n"
    "    (emit-bolt! collector [new-record] :anchor tuple)\n"
    "    (ack! collector tuple)))\n"))

