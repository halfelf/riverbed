(defproject xxx "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src"]
  :aot :all
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [http-kit "2.1.13"]
                 [javax.servlet/servlet-api "2.5"]
                 [compojure "1.1.6"]]
  :main oceanus.riverbed.main
  :min-lein-version "2.0.0"
  )

