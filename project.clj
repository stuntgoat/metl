(defproject metl "0.1.0-SNAPSHOT"
  :description "mETL a miniature ETL"
  :url "https://github.com/stuntgoat/metl"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojars.hozumi/clj-commons-exec "1.1.0"]]
  :main metl.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
