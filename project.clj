(defproject riverbed "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :aot :all
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [http-kit "2.1.16"]
                 [javax.servlet/servlet-api "2.5"] ; see ring github page
                 [clj-http "0.7.8"]
                 [compojure "1.1.6"]
                 [com.novemberain/monger "1.7.0-beta1"]
                 [cheshire "5.2.0"]
                 [org.clojure/java.jdbc "0.3.0-beta2"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [org.apache.kafka/kafka_2.10 "0.8.0"]
                 [com.101tec/zkclient "0.4" :exclusions [org.apache.zookeeper/zookeeper]]
                 [me.raynes/fs "1.4.4"]]
  :exclusions [javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri]
  :min-lein-version "2.0.0"
  )

