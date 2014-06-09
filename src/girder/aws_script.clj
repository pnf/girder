(ns girder.aws-script 
  (:use [amazonica.core]
        [amazonica.aws.ec2]
        )
  (:require [clj-ssh.ssh :as ssh]
            [ clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close!]]))

(def cred (read-string (slurp "AWS.clj")))

(apply defcredential (map cred [:access-key :secret-key :endpoint]))

;(describe-images :owners ["self"])
;aws ec2 request-spot-instances --spot-price 0.005 --instance-count 1 --type "one-time" --launch-specification '{"ImageId" : "ami-a048bec8", "InstanceType" : "t1.micro", "Placement" : { "AvailabilityZone" : "us-east-1c"}, "KeyName" : "telekhine", "SecurityGroupIds" : ["sg-149f3c7e"]}'
; "IamInstanceProfile" : {"Arn" : "arn:aws:iam::633840533036:instance-profile/datomic-aws-peer" }



(defn request-spots [n]
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


(defn patience
  "Invoke (genfn) every msec until predicate returns true or timeout channel is closed."
  [pred genfn msec tout]
  (let [c    (chan)]
    (close! (async/go-loop []
               (let [r (genfn)]
                 (if (or (pred r) (not (first (async/alts! [tout] :default true))))
                   (do (println "Returning" r)
                       (>! c r) (close! c))
                   (do 
                     (println "Got" r "...waiting" msec)
                     (<! (timeout msec))
                     (recur))))))
    c))

(defn patience-every [pred collfn msec tout]
  (patience #(every? pred %) collfn msec tout))

(defn pluck [c]
  (let [a (atom nil)]
    (close! (go (let [x (<! c)]
                  (reset! a x))))
    a))

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


(defn bring-up-aws
  "Bring up n AWS t1.micro instances and return channel that will contain a single map
of {:instance ids :hosts names and :sessions objects}."
  [n]
  (let [c    (chan)
        tout (timeout (* 60 1000 10))]
    (go 
      (let [rs       (request-spots n)
            _        (println "requests" rs)
            _        (<! (timeout 1000))
            ss       (<! (patience-every #(= "active" %) #(request-status rs) 60000 tout))
            _        (println "Requesting instances for" rs)
            is       (request-instances rs)
            _        (println "instances" is)
            hosts    (dns-names is)
            _        (println "hosts" hosts)
            sessions (ssh-sessions hosts)
            _        (println "Done!")]
        (>! c {:instances is :hosts hosts :sessions sessions}) (close! c)))
    [c tout]))


(defn EDNify
"EDN-ify an arbitrary object, leaving it alone if its an innocuous string."
  [x] 
  (let [x (if (string? x) x (pr-str x))
        x (if (re-matches #"[0-9a-zA-z-_\.]+" x) x (pr-str x))]
    x))


(defn commandify
  "If cmd is a sequence, convert it into a space-delimited string, EDNifying as necessary."
  [cmd]
  (cond (string? cmd) cmd
        (seq cmd) (clojure.string/join " " (map EDNify cmd))))

(defn ex
  "Make sure the session is connected and run the command remotely via
ssh-exec, yielding a map of :exit code, :out string and :err string."
  [sess cmd]
  (or (ssh/connected? sess) (ssh/connect sess))
  (ssh/ssh-exec sess (commandify cmd) "" "" {}))

(defn ex-async [sess cmd]
  "As ex, but returns a channel that will contain the map."
  (or (ssh/connected? sess) (ssh/connect sess))
  (let [c (chan)]
    (go (let [cmd (commandify cmd)
              _   (println "Running in" sess cmd)
              res (ssh/ssh-exec sess cmd "" "" {})]
          (println "Returning from" sess res)
          (>! c res) (close! c)))
    c))
