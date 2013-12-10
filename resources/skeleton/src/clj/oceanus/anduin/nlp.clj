(ns oceanus.anduin.nlp
  (:require [clj-http.client :as client])
  (:require [cheshire.core :refer :all])
  (:gen-class))

(def inner-api "http://innerapi.bestminr.com")

(def common-headers
  {:content-type :json
   :accept :json
   :conn-timeout 1000})

(defn ^vector zh-segmentation [^String text]
  "Segmentation for Chinese text"
  (let [text-json   (generate-string {"text" text} {:escape-non-ascii true})
        seg-uri     (str inner-api "/segmentation/")
        options     (merge {:body text-json} common-headers)
        seg-result  (client/post seg-uri options)]
    (vec (parse-string (seg-result :body)))))

(defn ^float zh-sentiment [^String text ^String key-word]
  "Chinese sentiment judge, return value in [-1.0, 1.0]"
  (let [text-json    (generate-string 
                       {"text" text "target" key-word} 
                       {:escape-non-ascii true})
        senti-uri    (str inner-api "/sentiment/document")
        options      (merge {:body text-json} common-headers)
        senti-result (client/post senti-uri options)]
    ((parse-string (senti-result :body)) "Result")))

