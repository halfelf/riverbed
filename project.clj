(defproject riverbed "0.1.1-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :aot :all
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.taoensso/timbre "3.0.1"]
                 [clj-http "0.9.1"]
                 [compojure "1.1.6"]
                 [com.novemberain/langohr "2.8.1" :exclusions [com.google.guava/guava]]
                 [com.novemberain/monger "1.7.0"]
                 [cheshire "5.3.1"]
                 [org.clojure/java.jdbc "0.3.0-beta2"]
                 [mysql/mysql-connector-java "5.1.25"]
                 [org.apache.curator/curator-framework "2.4.1"]
                 [org.apache.curator/curator-recipes "2.4.1"]
                 [org.apache.zookeeper/zookeeper "3.4.5"]
                 [me.raynes/fs "1.4.4"]]
  :exclusions [javax.jms/jms
               com.sun.jdmk/jmxtools
               com.sun.jmx/jmxri]
  :min-lein-version "2.0.0"
  )

