(ns oceanus.glacier.project-maker ; project.clj
  (:gen-class))

(defn project-def
  [topo-id]
  (str
    (format "(defproject task%s \"0.1.0\"\n" topo-id)
            "  :description \"\"\n"
            "  :url \"http://dataminr.com\"\n"
            "  :source-paths [\"src/clj\"]\n"
            "  :aot :all\n"
            "  :dependencies [[org.apache.kafka/kafka_2.9.2 \"0.8.0\"]\n"
            "                 [org.scala-lang/scala-library \"2.9.2\"]\n"
            "                 [log4j \"1.2.15\"]\n"
            "                 [clj-http \"0.7.8\"]\n"
            "                 [cheshire \"5.2.0\"]\n"
            "                 [com.novemberain/langohr \"1.7.0\" :exclusions [com.google.guava/guava]]\n"
            "                 [commons-collections/commons-collections \"3.2.1\"]\n"
            "                 [com.101tec/zkclient \"0.4\" :exclusions [org.apache.zookeeper/zookeeper]]\n"
            "                 [com.yammer.metrics/metrics-core \"2.2.0\"]]\n"
            "  :profiles {:dev\n"
            "              {:dependencies [[org.apache.storm/storm-core \"0.9.1-incubating\"]\n"
            "                              [org.clojure/clojure \"1.4.0\"]\n"
            "                              [org.easytesting/fest-assert-core \"2.0M8\"]\n"
            "                              [org.mockito/mockito-all \"1.9.0\"]\n"
            "                              [org.jmock/jmock \"2.6.0\"]\n"
            "                              ]}}\n"
            "  :exclusions [javax.jms/jms\n"
            "               com.sun.jdmk/jmxtools\n"
            "               com.sun.jmx/jmxri]\n"
            "  :min-lein-version \"2.0.0\")\n"
  ))
