(ns oceanus.glacier.ht-maker ;header and tail
  (:gen-class))

(defn clj-header-maker
  "Generate clj ns/require/import, other consts, etc."
  [kafka-conf topo-spec]
  (str 
    (format
      "(ns oceanus.anduin.%s\n" (topo-spec :topo-id))
      "  (:require [clojure.string :as string])\n"
      "  (:use [oceanus.anduin.consumer :only [get-one-stream]])\n"
      "  (:use [oceanus.anduin.nlp])\n"
      "  (:require [langohr core channel queue basic exchange])\n"
      "  (:import [backtype.storm StormSubmitter LocalCluster])\n"
      "  (:use [backtype.storm clojure config])\n"
      "  (:require [cheshire.core :refer :all])\n"
      "  (:gen-class))\n\n"
      ;;;;;;;;;;;;;;;;;;;;
      "(def ^{:const true}\n" 
    (format
      "  props {\"zookeeper.connect\"           \"%s:%s\"\n" 
      (kafka-conf :zk-host) (kafka-conf :zk-port))
    (format
      "         \"group.id\"                    \"%s\",\n" (topo-spec :topo-id))
      "         \"socket.receive.buffer.bytes\" 65536,\n"
      "         \"auto.commit.interval.ms\"     1000,\n"
      "         \"queued.max.messages.chunks\"  1000})\n\n"
      ;;;;;;;;;;;;;;;;;;;;
      "(def ^{:const true}\n"
      "  exchange-name \"outfall\")\n\n"
      "(def ^{:const true} ids-keys\n  "
      (str (zipmap (topo-spec :topic-ids) 
                   (reduce 
                     #(conj %1 
                            (if 
                              (re-find #"(https?|ftp)://(-\.)?([^\s/?\.#-]+\.?)+(/[^\s]*)?" %2) 
                              "" %2)) 
                     [] 
                     (topo-spec :keywords))))
    ; actually (zipmap topic-ids keywords)
    ; but url in keywords replaced by empty string
      ")\n\n"
    ))


(defn clj-tail-maker
  [topo-id]
  (str 
    "(defn run-local! []\n"
    "  (let [cluster (LocalCluster.)]\n"
    (format 
    "    (.submitTopology cluster \"%s\" {TOPOLOGY-DEBUG false} (mk-topology))\n" topo-id)
    "    (try\n"
    "      (loop [x 1]\n"
    "        (if (< x 0)\n"
    "          x\n"
    "          (recur (do (Thread/sleep 100000) x))))\n"
    "      (catch Exception e\n"
    "        (str \"caught exception: \" (.getMessage e)))\n"
    "      (finally (.shutdown cluster)))))\n\n"
    ;;;;;;;;;;
    "(defn submit-topology! [name]\n"
    "  (StormSubmitter/submitTopology\n"
    "   name\n"
    "   {TOPOLOGY-DEBUG false\n"
    "    TOPOLOGY-WORKERS 1}\n"
    "   (mk-topology)))\n\n"
    ;;;;;;;;;;
    "(defn -main\n"
    "  ([]\n"
    "   (run-local!))\n"
    "  ([name]\n"
    "   (submit-topology! name)))\n\n"
  ))

