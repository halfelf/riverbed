(ns oceanus.anduin.consumer
  (:gen-class)
  (:import [kafka.consumer ConsumerConfig Consumer KafkaStream]
           [kafka.javaapi.consumer ConsumerConnector]
           [java.util Properties]))

(defn- make-props
  "convert a clojure map into a Properties object."
  [m]
  (let [props (Properties.)]
    (doseq [[k v] m]
      (.put props k (str v)))
    props))

(defn- get-streams-map [conf topics]
  (-> conf make-props ConsumerConfig.
    Consumer/createJavaConsumerConnector
    (.createMessageStreams topics)))

(defn get-streams [props topic total-partitions]
  (for [i (range total-partitions)]
    (-> props
      (get-streams-map {topic (int 1)})
      (.get topic)
      first)))

(defn get-one-stream [props topic]
  (-> props
    (get-streams-map {topic (int 1)})
    (.get topic)
    first))

(defn- count-events [props topic total-partitions]
  (let [counter (atom 0)]
    (doseq [stream (get-streams props topic total-partitions)]
      (.start
        (Thread.
          #(doseq [m stream]
             (swap! counter inc)))))
    (Thread/sleep 15000)
    @counter))

