(ns oceanus.glacier.str-bolts-maker
  (:gen-class))

;(defbolt name output-declaration *option-map & impl)
;
;(defbolt word-count ["word" "count"] {:prepare true}
;  [conf context collector]
;  (let [counts (atom {})]
;    (bolt
;     (execute [tuple]
;       (let [word (.getString tuple 0)]
;         (swap! counts (partial merge-with +) {word 1})
;         (emit-bolt! collector [word (@counts word)] :anchor tuple)
;         (ack! collector tuple)
;         )))))
;

(comment ; a string bolt spec map
  {:name              "my-bolt"
   :output-declr      ["word"]
   :type              :include-any   ; :include-any (default)
                                     ; :include-all
                                     ; :exclude-any :exclude-all
                                     ; :sentiment
   :field             :weibo
   ;option-map itself and every key in it are not necessary
   :option-map        {:params  ["this"]  ; default nil
                       :prepare true}     ; true when side effect exists
   ;side-effect itself and every key in it are not necessary
   :side-effect-conf  {:db {:db-type    "mongo" 
                            :db-name    "xxx"
                            :db-table   "ttt"
                            :db-host    "192.168.1.1:88888"
                            :side-spec  "192.168.1.2/bolt-spec/bolt_number}"}
                       :mq {:mq-type    "rabbit"
                            :mq-name    "counts"
                            :mq-host    "192.168.1.1:99999"
                            :mq-group   "abc"
                            :side-spec  "192.168.1.2/bolt-spec/bolt_number}"}}
   })
                            

(defmulti generate-str-bolt
  (fn [bolt-spec] [(:type bolt-spec)
                   (:side-effect-conf bolt-spec)]))

;(defbolt split-sentence ["word"] [tuple collector]
;  (let [words (.split (.getString tuple 0) " ")]
;    (doseq [w words]
;      (emit-bolt! collector [w] :anchor tuple))
;    (ack! collector tuple)
;    ))

(defmethod generate-str-bolt [:include-any nil]
  [bolt-spec]
  (str 
    (format "(defbolt %s %s [tuple collector]\n" 
        (:name bolt-spec) 
        (:output-declr bolt-spec))
            "  (if-let [info-dict (.getValue tuple 0)\n"
    (format "           words (\"%s\" info-dict)\n" (:field bolt-spec))
            "           test_set (set (:params (:option-map bolt-spec)))]\n"
            "    (if (some test_set words)\n"
            "      (emit-bolt! collector [tuple] :anchor tuple))\n"
            "    (ack! collector tuple)))\n"
    ))

(defmethod generate-str-bolt [:exclude-any nil]
  [bolt-spec]
  (str
    (format "(defbolt %s %s [tuple collector]\n" 
        (:name bolt-spec) 
        (:output-declr bolt-spec))
            "  (if-let [info-dict (.getValue tuple 0)\n"
    (format "           words (\"%s\" info-dict)\n" (:field bolt-spec))
            "           test_set (set (:params (:option-map bolt-spec)))]\n"
            "    (if (not-any? test_set words)\n"
            "      (emit-bolt! collector [tuple] :anchor tuple))\n"
            "    (ack! collector tuple)))\n"
    ))
  
(defmethod generate-str-bolt [:include-all nil]
  [bolt-spec]
  (str
    (format "(defbolt %s %s [tuple collector]\n" 
        (:name bolt-spec) 
        (:output-declr bolt-spec))
            "  (if-let [info-dict (.getValue tuple 0)\n"
    (format "           words (set (\"%s\" info-dict))\n" (:field bolt-spec))
            "           test_set (:params (:option-map bolt-spec))]\n"
            "    (if (every? words test_set)\n"
            "      (emit-bolt! collector [tuple] :anchor tuple))\n"
            "    (ack! collector tuple)))\n"
    ))

(defmethod generate-str-bolt [:exclude-all nil]
  [bolt-spec]
  (str
    (format "(defbolt %s %s [tuple collector]\n" 
        (:name bolt-spec) 
        (:output-declr bolt-spec))
            "  (if-let [info-dict (.getValue tuple 0)\n"
    (format "           words (set (\"%s\" info-dict))\n" (:field bolt-spec))
            "           test_set (:params (:option-map bolt-spec))]\n"
            "    (if (not-every? words test_set)\n"
            "      (emit-bolt! collector [tuple] :anchor tuple))\n"
            "    (ack! collector tuple)))\n"
    ))


(defmethod generate-str-bolt [:include-any :mq]
  [bolt-spec]
    )

(defmethod generate-str-bolt [:include-all :mq]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:exclude-any :mq]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:exclude-all :mq]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:include-any :db]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:include-any :db]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:exclude-any :db]
  [bolt-spec]
    )
(defmethod generate-str-bolt [:exclude-all :db]
  [bolt-spec]
    )
