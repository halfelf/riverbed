(ns oceanus.riverbed.go
  (:require [clojure.string :as string])
  (:use clojure.set)
  (:use [clojure.java.shell]) 
  (:require [me.raynes.fs :as fs])
  (:require [oceanus.glacier 
             [topology-maker :as topology-maker]
             [str-bolts-maker :as str-bolts-maker]
             [kafka-spout :as kafka-spout]
             [segmentation-bolt-maker :as segmentation-bolt-maker]
             [spitter-bolt-maker :as spitter-bolt-maker]
             [tid-adder-maker :as tid-adder-maker]
             [pass-tag-adder-maker :as pass-tag-adder-maker]
             [pass-filter-maker :as pass-filter-maker]])
  (:gen-class))


(defn clj-header-maker
  "Generate clj ns/require/import, other consts, etc."
  [topo-name]
  (str 
    (format
      "(ns oceanus.anduin.%s\n" topo-name)
      "  (:use [oceanus.anduin.consumer :only [get-one-stream]])\n"
      "  (:use [oceanus.anduin.nlp])\n"
      "  (:require [langohr core channel queue basic])\n"
      "  (:import [backtype.storm StormSubmitter LocalCluster])\n"
      "  (:use [backtype.storm clojure config])\n"
      "  (:require [cheshire.core :refer :all])\n"
      "  (:require [monger core collection])\n"
      "  (:import [org.bson.types ObjectId]\n"
      "           [com.mongodb DB WriteConcern])\n"
      "  (:gen-class))\n\n"
      ;;;;;;;;;;;;;;;;;;;;
      "(def ^{:const true}\n" 
      "  props {\"zookeeper.connect\"           \"general:2181\"\n"
      "         \"zk.connectiontimeout.ms\"     1000000\n"
      "         \"group.id\"                    \"auto\",\n"
      "         \"fetch.size\"                  2097152,\n"
      "         \"socket.receive.buffer.bytes\" 65536,\n"
      "         \"auto.commit.interval.ms\"     1000,\n"
      "         \"queued.max.messages\"         100})\n\n"
      ;;;;;;;;;;;;;;;;;;;;
      "(def ^{:const true}\n"
      "  exchange-name \"\")\n\n"
  ))


(defn clj-tail-maker
  [topo-name]
  (str 
    "(defn run-local! []\n"
    "  (let [cluster (LocalCluster.)]\n"
    (format 
    "    (.submitTopology cluster \"%s\" {TOPOLOGY-DEBUG false} (mk-topology))\n" topo-name)
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
    "    TOPOLOGY-WORKERS 4}\n"
    "   (mk-topology)))\n\n"
    ;;;;;;;;;;
    "(defn -main\n"
    "  ([]\n"
    "   (run-local!))\n"
    "  ([name]\n"
    "   (submit-topology! name)))\n\n"
  ))


(def ^{:const true}
  rabbit-conf {:host  "general"
               :queue "hello"})

(def ^{:const true}
  mongo-conf  {:host  "store"
               :port  27017
               :db    "sina_status"})


(defn- check-empty-or-split
  "if empty str => [], else => split"
  [maybe-words]
  (if (empty? maybe-words)
    []
    (string/split maybe-words #",")))

(defn go-topo
  "Generate a project dir, define topo, define nodes, run"
  [spec]
  (let [topo-name   (spec :name)
        topo-id     (spec :id)
        split-words (reduce #(update-in %1 [%2] check-empty-or-split)
                            spec
                            [:or_keywords :and_keywords :not_keywords])
        topo-spec (-> split-words
                    (update-in [:conditions] #(or % "and"))
                    (rename-keys {:or_keywords  :include-any
                                  :and_keywords :include-all
                                  :not_keywords :exclude-any}))
        condition (topo-spec :conditions) 
        topo-root (str "/streaming/" topo-id)
        src-root  (str topo-root "/src/clj/oceanus/anduin/")
        main-clj  (format "%s/%s.clj" src-root topo-name)]
    (fs/copy-dir "resources/skeleton" topo-root)
    ; header, spout, bolts(tag, filters, spitter...etc), topo-def, tail
    (spit main-clj (clj-header-maker topo-name))
    (spit main-clj (kafka-spout/default-kafka-spout "store_topic") :append true)
    (spit main-clj (tid-adder-maker/generate-tid-bolt topo-id) :append true)
    (spit main-clj (segmentation-bolt-maker/generate-seg-bolt "txt") :append true)
    (if (= "or" condition)
      (spit main-clj (pass-tag-adder-maker/generate-tag-bolt) :append true))
    (if-not (empty? (topo-spec :include-any))
      (spit main-clj (str-bolts-maker/include-any-maker
                       (topo-spec :include-any) condition) :append true))
    (if-not (empty? (topo-spec :include-all))
      (spit main-clj (str-bolts-maker/include-all-maker
                       (topo-spec :include-all) condition) :append true))
    (if-not (empty? (topo-spec :exclude-any))
      (spit main-clj (str-bolts-maker/exclude-any-maker
                       (topo-spec :exclude-any) condition) :append true))
    (if (= "or" condition)
      (spit main-clj (pass-filter-maker/generate-pass-bolt) :append true))
    (spit main-clj (spitter-bolt-maker/mq-spitter-bolt rabbit-conf mongo-conf (str topo-id)) :append true)
    (spit main-clj (topology-maker/generate-topology topo-spec) :append true)
    (spit main-clj (clj-tail-maker topo-name) :append true)
    (with-sh-dir topo-root
      (sh "sh" "-c" (format "lein run -m oceanus.anduin.%s 1>info.log 2>error.log &" topo-name)))
  ))

(defn stop-topo
  [spec]
  (let [topo-id   (spec :id)
        topo-name (spec :name)
        topo-root (str "/streaming/" topo-id)]
    (with-sh-dir topo-root
      (sh "sh" "-c" 
          (format 
            "ps aux|grep java|grep \"/streaming/%d\"|grep -v \"grep\"|awk '{print $2}'|xargs kill" 
            topo-id)))
    (fs/delete-dir topo-root)
  ))

