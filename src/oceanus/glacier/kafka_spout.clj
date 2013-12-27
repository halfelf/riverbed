(ns oceanus.glacier.kafka-spout
  (:gen-class))

(defn default-kafka-spout
  [kafka-topic serial-number]
  (str
    (format "(defspout kafka-spout-%d [\"sina_status\"]\n" serial-number)
            "  [conf context collector]\n"
    (format "  (let [stream (atom (get-one-stream props \"%s\"))]\n" kafka-topic)
            "    (spout\n"
            "      (nextTuple []\n"
            "        (doseq [raw-one-log @stream]\n"
            "          (Thread/sleep 100)\n"
            "          (let [one-log (-> (.message raw-one-log) String. parse-string)]\n"
            "            (emit-spout! collector [one-log]))))\n"
            "      (ack [id]\n"
            "        ))))\n\n"
  ))

