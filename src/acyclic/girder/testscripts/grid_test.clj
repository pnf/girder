(ns acyclic.girder.grid-test
  (:require 
   [clj-ssh.ssh :as ssh]
   [clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close!]])
  (:use [acyclic.awsutils.aws-script]))


;; (def caws (first (bring-up-aws 10)))

(def n 50)

(def runjar ["java" "-cp" "girder.jar" "acyclic.girder.testutils.grid" "--opts"])

(defn download-jar [sessions] (async/map vector  (map #(ex-async  % "aws s3 cp s3://dist-ec2/girder.jar .") sessions)))


(comment 

(def my-subnets {"us-east-1c"  "subnet-08eff44e"
                 "us-east-1a"  "subnet-f290abda"
                 "us-east-1b"  "subnet-572dd420"})
(def my-zone    "us-east-1a")
(def my-req {:spot-price 		0.01
             :instance-count 		1
             :type 			"one-time"
             :launch-specification     {:image-id 	       "ami-5cd51634"
                                        :instance-type 	       "t1.micro"
                                        :placement             {:availability-zone my-zone}
                                        :key-name	       "telekhine"
                                        :security-groups-ids   ["sg-78deaf1d"]
                                        :subnet-id             (get my-subnets my-zone)
                                        :iam-instance-profile  {:arn "arn:aws:iam::633840533036:instance-profile/girder-peer"}}})


  (def a1 (bring-up-aws my-req :n 1 :itype "m3.medium" :price 0.01))
  (def m (<!! (first a1)))
  (def rsess (-> m :sessions first))
  (def r (ex-async rsess "bin/redis-server"))
  (def red (-> m :ips first))


  (def aa (bring-up-aws :n 99 :price 0.01 :itype "t1.micro"))
  (def ts (<!! (first aa)))
  (def sess (:sessions ts))
  (def c  (download-jar sess))
  ;; bring up distributor and helper on first t1

  (def ca (map #(ex % "killall java") sess))
  (def cu (ex-async (first sess) (conj runjar {:cleanup true :host red} )))
  (def di (ex-async (first sess) (conj runjar {:distributor "dist" :host red :hang 10000})))
  (def he (ex-async (first sess) (conj runjar {:helper 100 :pool "dist" :host red :hang 10000})))
  (def ws (async/map vector (map-indexed  #(ex-async %2 (conj runjar {:worker (str "w" %1) :id  (str %1) :pool "dist" :host red :hang 10000})) (rest sess))))


  (def res1  (ex (second sess) (conj runjar {:pool "dist" :host red :jobs 20 :reclevel 0})))
  (def res1  (ex (second sess) (conj runjar {:pool "dist" :host red :jobs 20 :reclevel 3})))
  (def res1  (ex (second sess) (conj runjar {:pool "dist" :host red :jobs 20 :reclevel 3 :cmt 111 })))
  (def res1  (ex (second sess) (conj runjar {:pool "dist" :host red :jobs 1000 :reclevel 3 :cmt 111 })))

  (def vs (-> res1 (get :out) read-string :result :jobs))

  (-> res1 :out read-string :time)

  )




;; (def caws (first (bring-up-aws 10)))
;; (def r1-raw (<!! (test1 (:sessions aws))))
;; (def tss (exs->tss r1-raw))
;; girder.tests> (map #(-> % :out read-string :time (* 0.0001)) r1-raw)
;; (2.2591 2.3295 2.3756 2.3816 2.9259 2.27 2.249 2.2487 2.1299 2.2466)
;; (def r2-raw (<!! (test2 (:sessions aws))))
;; girder.tests> (map #(-> % :out read-string :time (* 0.0001)) r2-raw)
;; (1.5958 1.6817 1.7074 1.7345000000000002 1.7221000000000002 2.4846 1.7578 1.7805000000000002 1.7654 1.9578)
;; So we're looking at 2.5 ms per insert, simultaneously.  1.6-2ms read.


