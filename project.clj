(defproject taskmaster "0.0.4-SNAPSHOT-1"
  :description "Background publisher/consumer on top of Postgres, next.jdbc and hikari-cp"
  :url "https://github.com/lukaszkorecki/taskmaster"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[seancorfield/next.jdbc "1.1.582"]
                 [org.postgresql/postgresql "42.2.16"]
                 [cheshire "5.10.0"]
                 [clj-time "0.15.2"]

                 [hikari-cp "2.11.0"]

                 [nomnom/utility-belt.sql "1.0.0.beta4" :exclusions [seancorfield/next.jdbc hikari-cp org.postgresql/postgresql]]]
  :profiles {:dev {:dependencies [[org.clojure/tools.logging "1.1.0"]
                                  [com.stuartsierra/component "1.0.0"]
                                  [ch.qos.logback/logback-classic "1.2.3"]
                                  [org.clojure/clojure "1.10.1"]]
                   :resource-paths ["dev-resources"]}}
  :deploy-repositories {"clojars" {:sign-releases false
                                   :username :env/clojars_username
                                   :password :env/clojars_password}})
