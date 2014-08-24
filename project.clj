(defproject acyclic/girder "0.1.0-SNAPSHOT"
  :author "Peter Fraenkel <http://podsnap.com>"
  :description "Distributed re-entrant grid"
  :url "http://github.com/pnf/girder"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["sonatype" {:url "https://oss.sonatype.org/content/repositories/snapshots"
                               :update :always}]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [digest "1.4.4"]
                 [clj-time "0.7.0"]
                 [amazonica "0.2.16"]
                 [clj-ssh "0.5.10"]
                 [org.clojure/core.async "0.1.303.0-886421-alpha"
                  :exclusions [[org.clojure/core.cache]]]
                 ;[org.clojure/core.cache "0.6.5"]
                 [core.cache "0.6.5-pnf-SNAPSHOT"]
                 [com.taoensso/timbre "3.2.0"]
                 [acyclic/utils "0.1.0-SNAPSHOT"]
                 [acyclic/awstools "0.1.0-SNAPSHOT"]
                 [com.taoensso/carmine "2.7.0-RC1"]]

  :jvm-opts  ^:replace ["-Xmx1g" "-server" ] 
  :source-paths ["src"]
  :test-paths ["test"]

  :aot [acyclic.girder.testutils.grid]
  :uberjar-name "girder.jar"

)
