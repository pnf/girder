(defproject girder "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["sonatype" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                               :update :always}]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [digest "1.4.4"]
                 [clj-time "0.7.0"]
                ;[amazonica "0.2.16"]
                 [com.datomic/datomic-pro "0.9.4766.11" :exclusions [[org.slf4j/log4j-over-slf4j]]]
                 [org.clojure/tools.cli "0.3.1"]]

  :jvm-opts  ^:replace ["-Xmx1g" "-server" ] 
  :source-paths ["src"]

  :main girder.core
)
