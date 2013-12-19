(ns oceanus.glacier.spitter-bolt-maker
  (:gen-class))

(defn mq-spitter-bolt
  "output to rabbit and mongo, `collection-name` should be feeded on topology-id"
  [rabbit-conf mongo-conf collection-name]
  (str
      "(defbolt mq-spitter-bolt [\"weibo\"] {:prepare true}\n"
      "  [conf context collector]\n"
    (format
      "  (let [conn   (langohr.core/connect {:host \"%s\"})\n"
      (rabbit-conf :host))
      "        ch     (langohr.channel/open conn)\n"
    (format
      "        qname  \"%s\"\n" (rabbit-conf :queue))
    (format
      "        mongo_conn (monger.core/connect! {:host \"%s\" :port %d})\n"
      (mongo-conf :host) (mongo-conf :port))
    (format
      "        db     (monger.core/set-db! (monger.core/get-db \"%s\"))]\n"
      (mongo-conf :db))
;      "    (try\n"
      "      (bolt\n"
      "        (execute [tuple]\n"
      "          (let [weibo (.getValue tuple 0)]\n"
      "            (langohr.basic/publish ch exchange-name qname \n"
      "                                   (generate-string weibo)\n"
      "                                   :content-type \"text/plain\"\n"
      "                                   :type \"weibo\")\n"
    (format
      "            (monger.collection/update \"%s\" (select-keys weibo [\"_id\"]) weibo :upsert true)\n"
      collection-name)
      "            (emit-bolt! collector [weibo] :anchor tuple)\n"
      "            (ack! collector tuple))))\n"
      "  ))\n\n"
;      "      (catch Exception e (str \"caught exception: \" (.getMessage e)))\n"
;      "      (finally (langohr.core/close ch)\n"
;      "               (langohr.core/close conn)))))\n"
;      "               (monger.core/disconnect!)))))\n\n"
    ))

