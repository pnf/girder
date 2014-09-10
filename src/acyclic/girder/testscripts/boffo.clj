(ns acyclic.girder.testscripts.boffo
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            acyclic.girder.grid.redis)
  (:use acyclic.girder.grid))
(timbre/refer-timbre)


(timbre/set-level! :trace)
(acyclic.girder.grid.redis/init!)
(cleanup)

(cdefn divide [x y] (float (/ x y)))
(cdefn ratio [i] (request (divide i (dec i))))


(cdefn bogosity [msec jobnum args]
  (Thread/sleep msec)
  (str "Bogosity:" *nodeid* ":" jobnum ":" msec ":" args))


(cdefn recbog [msec jobnum reclevel numrecjobs args]
       (debug "here we are in recbog" *nodeid* msec jobnum reclevel numrecjobs args)
       (if-not (pos? reclevel)
         (let [res (str jobnum)]
           (Thread/sleep msec)
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           res)
         (let [reqs (map #(vector recbog msec % (dec reclevel) numrecjobs args) (range numrecjobs))
               vs  (requests reqs)
               res (str "RecBog:" *nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")]
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           (Thread/sleep msec)
           res
    )))

(cdefn recbog2 [msec jobnum reclevel numrecjobs args]
       (debug "here we are in recbog" *nodeid* msec jobnum reclevel numrecjobs args)
       (if-not (pos? reclevel)
         (let [res (str jobnum)]
           (Thread/sleep msec)
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           res)
         (let [reqs (map #(vector recbog msec % (dec reclevel) numrecjobs args) (range numrecjobs))
               vs  (seq-request reqs)
               res (str "RecBog:" *nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")]
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           (Thread/sleep msec)
           res
    )))


(def poolctl (launch-distributor "pool"))
(def w1ctl (launch-worker "w1" "pool"))
(def w2ctl (launch-worker "w2" "pool"))
;(def helperctl (launch-helper "pool" 1000))

(comment

  (def c (async/map vector (map #(enqueue "w1" [bogosity 10000 % 113]) (range 50))))
  (def c (async/map vector (map #(enqueue "pool" [bogosity 2 % 111]) (range 5))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 111]) (range 1))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 1 5 111]) (range 1))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 3 5 222]) (range 10))))
  (requests "pool" (map #(vector recbog 1 % 3 5 222) (range 5))  )
  (def c (async/map vector (map #(enqueue "pool" [recbog 10 % 0 5 211]) (range 50))))

  (load-file "src/acyclic/girder/testutils/grid.clj")

  (ns acyclic.girder.testscripts.boffo)
  (load-file "src/acyclic/girder/testscripts/boffo.clj")


  (def r-dist (<!! c-dist))



  ;; startup workers (private)
  (def r-workers (request-spots my-req :n 20 :subnet (:subnet-private my-ec2-info) :itype "t1.micro" :price 0.01 :udata (cmd-workers rsa 1 "pool "(:local-ipv4 redis))))

  (ex s-redis "bin/redis-cli keys '*'")


  (ex s-jobber ((cmd-job "pool" (:local-ipv4 redis) 50 2)))


  
  ;; startup job-requestor
  (def r-job (request-spots my-req :subnet (:subnet-public my-ec2-info) :itype "t1.micro" :price 0.01 :udata (str (send-up) (cmd-getjar))))
  (def jobber (b64->ec2-data (<!! cl)))
  (def s-jobber (ssh-session (:public-hostname jobber)))
  
  

  )



