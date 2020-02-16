(defproject taskmaster "0.0.1-SNAPSHOT"
  :description "Background queue on top of Postgres and JDBC"
  :url "https://github.com/lukaszkorecki/taskmaster"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[nomnom/utility-belt.sql "1.0.0.beta1"]]
  :profiles {:dev {:dependencies [[org.clojure/tools.logging "0.5.0"]
                                  [com.stuartsierra/component "0.4.0"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.clojure/clojure "1.10.1"]]}}
  :deploy-repositories [["releases" :clojars]])
