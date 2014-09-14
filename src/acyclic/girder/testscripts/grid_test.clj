(ns acyclic.girder.grid-test
  (:require 
   [clj-ssh.ssh :as ssh]
   [clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close!]]
   [taoensso.timbre :as timbre]
   [taoensso.timbre.appenders.carmine :as car-appender])
  (:use [acyclic.awstools.core]))
(timbre/refer-timbre)

;; (def caws (first (bring-up-aws 10)))

(def n 50)

(slurp-ec2-info "EC2DATA.clj")

(def runjar ["java" "-cp" "girder.jar" "acyclic.girder.testutils.grid" "--opts"])
(defn download-jar [sessions] (async/map vector  (map #(ex-async  % "aws s3 cp s3://dist-ec2/girder.jar .") sessions)))

(def cmd-getjar
  "aws s3 cp s3://dist-ec2/girder.jar .\n")
(defn cmd-workers [n pool ip]
  (str cmd-getjar
       "java -cp girder.jar acyclic.girder.testutils.grid --worker " n " 1 --pool " pool "--host " ip " --hang 1000\n"))
(defn cmd-dist [pool ip]
  (str cmd-getjar
       "java -cp girder.jar acyclic.girder.testutils.grid --distributor " pool " --host " ip " --hang 1000\n"))
(defn cmd-job [pool ip jobs reclevel]
  (str "java -cp girder.jar acyclic.girder.testutils.grid --pool "pool " --host " ip " --jobs " jobs " --reclevel " reclevel))


(def cr (bring-up-aws my-req 1 :udata "bin/redis-server\n" :itype "m3.medium" :price 0.03))
(def req-redis (<!! cr))
(def redis (first (:ips req-redis)))
(def r)

ssh/forward-remote-port

(def redconn {:pool {} :spec {:host "localhost" :port 6379}})



(def cw  (bring-up-aws my-req 10 :udata (cmd-workers 2 redis) :itype "t1.micro" :price 0.01))
(def req-workers (<!! cw))

(def cp (bring-up-aws my-req 1 :udata (cmd-dist "pool" redis) :itype "t1.micro" :price 0.01))
(def req-pool (<<!! cp))

(def sess (ssh-session (first (:hosts req-workers))))
(ex sess (cmd-job "pool" redis 50 2))







;; (def caws (first (bring-up-aws 10)))
;; (def r1-raw (<!! (test1 (:sessions aws))))
;; (def tss (exs->tss r1-raw))
;; girder.tests> (map #(-> % :out read-string :time (* 0.0001)) r1-raw)
;; (2.2591 2.3295 2.3756 2.3816 2.9259 2.27 2.249 2.2487 2.1299 2.2466)
;; (def r2-raw (<!! (test2 (:sessions aws))))
;; girder.tests> (map #(-> % :out read-string :time (* 0.0001)) r2-raw)
;; (1.5958 1.6817 1.7074 1.7345000000000002 1.7221000000000002 2.4846 1.7578 1.7805000000000002 1.7654 1.9578)
;; So we're looking at 2.5 ms per insert, simultaneously.  1.6-2ms read.


