(ns oceanus.anduin.nlp
  (:require [clj-http.client :as client])
  (:require [cheshire.core :refer :all])
  (:gen-class))

(def inner-api "http://innerapi.bestminr.com")

(def common-headers
  {:content-type :json
   :accept :json
   :conn-timeout 1000})

(defn zh-segmentation [text]
  "Segmentation for Chinese text, return a vector of words"
  (let [text-json (generate-string {"text" text} {:escape-non-ascii true})
        seg-uri   (str inner-api "/segmentation/")
        options   (merge {:body text-json} common-headers)]
    (try
      (-> (client/post seg-uri options) :body parse-string vec)
      (catch Exception e nil))))

(defn zh-sentiment [text key-word]
  "Chinese sentiment judge, return int in #{-1 1 0}"
  (let [text-json    (generate-string 
                       {"text" text "target" key-word} 
                       {:escape-non-ascii true})
        senti-uri    (str inner-api "/sentiment/weibo/")
        options      (merge {:body text-json} common-headers)
        senti-result (client/post senti-uri options)]
    (try
      (-> senti-result :body (parse-string true) :Result first)
       ; `senti-result` form:  {:body "{\"Result\" [-1, -0.xxxx]}"}
      (catch Exception e nil))))

(defn ads-recognize [text]
  "Judge if the text is an ad or not, return true or false"
  (let [text-json (generate-string {"text" text} {:escape-non-ascii true})
        ad-uri    (str inner-api "/spam/recognize/")
        options   (merge {:body text-json} common-headers)]
    (try
      (-> (client/post ad-uri options) :body parse-string)
      (catch Exception e false))))

(defn similar-judge [text id]
  "Judge if the text is similar to anyone stored, return 0 or weibo-id now"
  (let [text-json (generate-string {"text" text "_id" id} {:escape-non-ascii true})
        sim-uri   (str inner-api "/similar/")
        options   (merge {:body text-json} common-headers)]
    (try
      (-> (client/post sim-uri options) :body parse-string)
      (catch Exception e 0))))

