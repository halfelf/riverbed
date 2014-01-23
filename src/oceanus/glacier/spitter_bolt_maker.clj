(ns oceanus.glacier.spitter-bolt-maker
  (:gen-class))

(defn mq-spitter-bolt
  "output to rabbit and mongo, `collection-name` should be feeded on topology-id"
  [rabbit-conf]
  (str
            "(defbolt mq-spitter-bolt [\"info\"] {:prepare true}\n"
            "  [conf context collector]\n"
    (format "  (let [conn   (langohr.core/connect {:host \"%s\"})\n" (rabbit-conf :host))
            "        ch     (langohr.channel/open conn)\n"
            "        _      (langohr.exchange/declare ch exchange-name \"fanout\" :durable true :auto-delete false)]\n"
            "      (bolt\n"
            "        (execute [tuple]\n"
            "          (let [info-map (.getValue tuple 0)]\n"
            "            (langohr.basic/publish ch exchange-name \"\" \n"
            "                                   (generate-string info-map)\n"
            "                                   :content-type \"text/plain\"\n"
            "                                   :type \"info_map\")\n"
            "            (emit-bolt! collector [info-map] :anchor tuple)\n"
            "            (ack! collector tuple))))\n"
            "  ))\n\n"
    ))

