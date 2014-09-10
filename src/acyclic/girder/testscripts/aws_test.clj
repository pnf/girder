(ns acyclic.girder.testscripts.aws-test
  (:use [amazonica.core]
        [amazonica.aws.ec2]
        [amazonica.aws.sqs]
        acyclic.utils.pinhole
        acyclic.utils.log
        acyclic.awstools.core)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]))
(timbre/refer-timbre)

(defn cmd-getjar []
  (str "aws s3 cp s3://dist-ec2/girder.jar ."))

(defn cmds-workers [n pool ip]
  [(cmd-getjar)
   (str "java -cp girder.jar acyclic.girder.testutils.grid --worker " n
        " --pool " pool " --host " ip " --hang 1000")])

(defn cmds-dist [pool ip]
    [(cmd-getjar)
     (str "java -cp girder.jar acyclic.girder.testutils.grid --distributor " pool
          " --host " ip " --hang 1000")])


(defn cmd-job [pool ip jobs reclevel]
  (str  "java -cp girder.jar acyclic.girder.testutils.grid --pool " pool 
        " --host " ip " --jobs " jobs " --reclevel " reclevel))

(defn cmds-redis [] [(str "bin/redis-server")])


(slurp-ec2-info "/Users/pnf/dev/girder/EC2DATA.clj")

(def c-listener (start-up-listener))

(def c-nat (bring-up-instances [(:nat-id my-ec2-info)]))
(def r-nat (<!! c-nat))

(def c-redis  (bring-up-spots my-req 1 (cmds-redis) :subnet my-sub-public :itype "m3.medium" :price 0.05 :key "telekhine" :log "debug"))
(def r-redis (<!! c-redis))
(def redis (first (vals r-redis)))
(def redis-ip (:ip redis))
(def redis-host (:host redis))

(def s-redis (ssh-session redis-host))


(def c-dist (bring-up-spots my-req 1 (cmds-dist "pool" redis-ip) :subnet my-sub-private :itype "t1.micro" :price 0.01 :key "girder" :minutes 100))
(def r-dist (<!! c-dist))

(def c-workers (bring-up-spots my-req 10 (cmds-workers 1 "pool" redis-ip) :subnet my-sub-private :itype "t1.micro" :price 0.01 :key "girder" :minutes 100))
(def r-workers (<!! c-workers))

(ex s-redis (cmd-job "pool" redis-ip 1 0))
