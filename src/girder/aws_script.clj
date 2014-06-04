(ns girder.aws-script 
  (:use [amazonica.core]
        [amazonica.aws.ec2]
        )
  (:require [clj-ssh.ssh :as ssh]
            [ clojure.core.async :as async 
             :refer [<! >! <!! timeout chan alt!! go close!]]))

(def cred (read-string (slurp "AWS.clj")))

(apply defcredential (map cred [:access-key :secret-key :endpoint]))

;(describe-images :owners ["self"])
;aws ec2 request-spot-instances --spot-price 0.005 --instance-count 1 --type "one-time" --launch-specification '{"ImageId" : "ami-a048bec8", "InstanceType" : "t1.micro", "Placement" : { "AvailabilityZone" : "us-east-1c"}, "KeyName" : "telekhine", "SecurityGroupIds" : ["sg-149f3c7e"]}'
; "IamInstanceProfile" : {"Arn" : "arn:aws:iam::633840533036:instance-profile/datomic-aws-peer" }


(defn requests [n]
  (let [r (request-spot-instances
           :spot-price 			"0.005"
           :instance-count 		n
           :type 			"one-time"
           :launch-specification 
           {:image-id 			"ami-a048bec8"
            :instance-type 		"t1.micro"
            :placement                  {:availability-zone 	"us-east-1c"}
            :key-name			"telekhine"
            :security-groups		["launch-wizard-6"]
            :iam-instance-profile       {:arn "arn:aws:iam::633840533036:instance-profile/girder-peer"}})]
    (map :spot-instance-request-id  (:spot-instance-requests r))))

(defn monitor-status [rs]
  (loop [prev nil]
    

)
 

)


(defn request-status [rs]
  (let [d (describe-spot-instance-requests :spot-instance-request-ids rs)]
    (map :state (:spot-instance-requests d))))

(defn request-instances [rs]
  (let [d (describe-spot-instance-requests :spot-instance-request-ids rs)]
    (map :instance-id (:spot-instance-requests d))))

(defn dns-names [is]
  (let [ds (:reservations (describe-instances :instance-ids is))]
    (map #(get-in % [:instances 0 :public-dns-name]) ds)))

(defn terminate [is]
  (terminate-instances :instance-ids is))

(defn cancel [rs]
  (:cancelled-spot-instance-requests
   (cancel-spot-instance-requests :spot-instance-request-ids rs)))

(def ag (ssh/ssh-agent {}))

(defn ssh-sessions [hosts]
  (map #(ssh/session ag % {:strict-host-key-checking :no
                            :username "ec2-user"})
       hosts))

(defn ex [sess cmd]
  (or (ssh/connected? sess) (ssh/connect sess))
  (ssh/ssh-exec sess cmd "" "" {}))

(defn ex-async [sess cmd c]
  (or (ssh/connected? sess) (ssh/connect sess))
  (go (>! c (ssh/ssh-exec sess cmd "" "" {})))
  c)



;(map disconnect ss)
;(describe-spot-instance-requests :spot-instance-request-ids r)
; (filter #(= "sir-b7586249" (:spot-instance-request-id %)) (get-in d [:spot-instance-requests]))

#_(def prepare-cmds
  "cd $HOME/girder
git pull
$HOME/bin/lein deps
$HOME/bin/lein compile
$HOME/bin/lein run --nTt 10")

(def get-jar )
