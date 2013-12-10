(defproject streamingware "0.1.0-SNAPSHOT"
  :description ""
  :url "http://dataminr.com"
  :source-paths ["src/clj"]
  :aot :all
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [storm "0.9.0.1"] 
                 [org.apache.kafka/kafka_2.9.2 "0.8.0-beta1"]
                 [org.scala-lang/scala-library "2.9.2"]
                 [log4j "1.2.15"]
                 [clj-http "0.7.7"]
                 [cheshire "5.2.0"]
                 [com.novemberain/langohr "1.7.0" :exclusions [com.google.guava/guava]]
                 [com.novemberain/monger "1.5.0" :exclusions [com.google.guava/guava]]
                 [commons-collections/commons-collections "3.2.1"]
                 [org.easytesting/fest-assert-core "2.0M8"]
                 [org.mockito/mockito-all "1.9.0"]
                 [com.101tec/zkclient "0.4" :exclusions [org.apache.zookeeper/zookeeper]]
                 [com.yammer.metrics/metrics-core "2.2.0"]
                 [org.jmock/jmock "2.6.0"]]
  :exclusions [javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri]
  :min-lein-version "2.0.0"
  )
