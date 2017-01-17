(defproject postgres-tools "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]]
  :profiles {:provided {:dependencies [[funcool/clojure.jdbc "0.9.0"]
                                       [org.clojure/java.jdbc "0.6.1"]
                                       [org.postgresql/postgresql "9.4.1212"]]}
             :dev      {:resource-paths ["test/resources"]
                        :dependencies   [[hikari-cp "1.7.5"]
                                         [mount "0.1.10"]
                                         [metosin/lokit "0.1.0"]
                                         [juxt/iota "0.2.3"]]}})
