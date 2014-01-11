(ns oceanus.riverbed.go
  (:require [clojure.string :as string])
  (:use [clojure.set])
  (:use [clojure.java.shell]) 
  (:require [me.raynes.fs :as fs])
  (:require [oceanus.glacier 
             [project-maker :as project-maker]
             [ht-maker :as ht-maker]
             [topology-maker :as topology-maker]
             [str-bolts-maker :as str-bolts-maker]
             [kafka-spout :as kafka-spout]
             [segmentation-bolt-maker :as segmentation-bolt-maker]
             [spitter-bolt-maker :as spitter-bolt-maker]
             [sentiment-judger-maker :as sentiment-judger-maker]
             [tid-adder-maker :as tid-adder-maker]
             [ads-tagger-maker :as ads-tagger-maker]
             [similar-tagger-maker :as similar-tagger-maker]
             [pass-tag-adder-maker :as pass-tag-adder-maker]
             [pass-filter-maker :as pass-filter-maker]])
  (:gen-class))


(def ^{:const true}
  rabbit-conf {:host  "general"})

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
  (let [topo-id     (spec :topo-id)
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
        main-clj  (format "%s/%s.clj" src-root topo-id)
        project-clj (format "%s/project.clj" topo-root)]
    (fs/copy-dir "resources/skeleton" topo-root)
    (spit project-clj (project-maker/project-def topo-id))
    ; header, spout, bolts(tag, filters, spitter...etc), topo-def, tail
    (spit main-clj (ht-maker/clj-header-maker topo-id))
    (doseq [[one-topic serial]
            (map list 
                 (spec :topic-ids)
                 (take (count (spec :keywords)) (iterate inc 1)))]
      (spit main-clj 
            (kafka-spout/default-kafka-spout one-topic serial) 
            :append true))
    (doseq [[one-target serial]
            (map list 
                 (spec :keywords)
                 (take (count (spec :keywords)) (iterate inc 1)))]
      (spit main-clj 
            (sentiment-judger-maker/generate-sentiment-judger one-target serial) 
            :append true))
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
    (spit main-clj (ads-tagger-maker/generate-ads-tagger) :append true)
    (spit main-clj (similar-tagger-maker/generate-similar-tagger) :append true)
    (spit main-clj (spitter-bolt-maker/mq-spitter-bolt rabbit-conf) :append true)
    (spit main-clj (topology-maker/generate-topology topo-spec) :append true)
    (spit main-clj (ht-maker/clj-tail-maker topo-id) :append true)
    ; package & submit to cluster
    (with-sh-dir topo-root
      (sh "sh" "-c" "lein compile"))
    (with-sh-dir topo-root
      (sh "sh" "-c" "lein uberjar"))
    (with-sh-dir topo-root
      (sh "sh" "-c" (format "storm jar target/task%s-0.1.0-standalone.jar oceanus.anduin.%s task%s" topo-id topo-id topo-id)))
    ))

(defn stop-topo
  [topo-id]
  (let [topo-root (str "/streaming/" topo-id)]
    (sh "sh" "-c"
      (format "storm kill task%s" topo-id))
    (fs/delete-dir topo-root)))
    
(defn deactivate
  [topo-id]
  (sh "sh" "-c"
      (format "storm deactivate task%s" topo-id)))

(defn activate
  [topo-id]
  (sh "sh" "-c"
      (format "storm activate task%s" topo-id)))


