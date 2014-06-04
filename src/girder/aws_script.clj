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

(defn requests [n]
  (let [r (request-spot-instances
           :spot-price 			"0.005"
           :instance-count 		n
           :type 			"one-time"
           :launch-specification 
           {:image-id 			"ami-a048bec8"
            :instance-type 		"t1.micro"
            :placement
            {:availability-zone 	"us-east-1c"}
            :key-name			"telekhine"
            :security-groups		["launch-wizard-6"]})]
    (map :spot-instance-request-id  (:spot-instance-requests r))))

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


(defn ex [sess c cmd]
  (or (ssh/connected? sess) (ssh/connect sess))
  (go (>! c (ssh/ssh-exec sess cmd "" "" {})))
  c)



;(map disconnect ss)



;(describe-spot-instance-requests :spot-instance-request-ids r)

; (filter #(= "sir-b7586249" (:spot-instance-request-id %)) (get-in d [:spot-instance-requests]))
