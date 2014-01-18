(ns oceanus.riverbed.created-hook
  (:require [clj-http.client :as client])
  (:require [cheshire.core :refer :all])
  (:gen-class))

(def inner-api "http://192.168.122.104")

(def common-headers
  {:content-type :json
   :accept :json
   :conn-timeout 1000})

(defn insert-keyword-to-dict
  [word]
  (let [text-json (generate-string {:word word} {:escape-non-ascii true})
        seg-uri   (str inner-api ":8010/seg/new_word/")
        options   (merge {:body text-json} common-headers)]
   ; (try
      (client/post seg-uri options)
    ))
   ;   (catch Exception e nil))))

