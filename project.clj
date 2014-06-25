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
                 [amazonica "0.2.16"]
                 [clj-ssh "0.5.10"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"]
                 [com.datomic/datomic-pro "0.9.4766.11" :exclusions [[org.slf4j/log4j-over-slf4j]]]
                 [com.taoensso/timbre "3.2.0"] 
                 [com.taoensso/carmine "2.6.0"]
                 [org.clojure/tools.cli "0.3.1"]

]

  :jvm-opts  ^:replace ["-Xmx1g" "-server" ] 
  :source-paths ["src"]


  :aot :all
  :uberjar-name "girder.jar"
  :main girder.core
)
