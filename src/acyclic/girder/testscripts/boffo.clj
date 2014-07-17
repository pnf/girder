(ns acyclic.girder.testscripts.boffo
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            acyclic.girder.grid.redis)
  (:use acyclic.girder.grid))
(timbre/refer-timbre)


(cdefn invo [x] 
       (float (/ 1.0 x)))

(cdefn foo1 [x1 x2 bleh]
       (let [[y1 y2] (call-reentrant [[invo x1] [invo x2]])]
         (str "foo:" y1 ":" y2)))

(timbre/set-level! :trace)
(acyclic.girder.grid.redis/init!)
(cleanup)


(cdefn bogosity [msec jobnum args]
  (Thread/sleep msec)
  (str "Bogosity:" *nodeid* ":" jobnum ":" msec ":" args))

(cdefn recbog [msec jobnum reclevel numrecjobs args]
  (let [_ (debug "here we are in recbog")
        reqs (map #(vector recbog msec % (dec reclevel)  numrecjobs args) (range reclevel))
        vs  (call-reentrant reqs)]
    (Thread/sleep msec)
    (str "RecBog:" *nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")    ))


(def poolctl (launch-distributor "pool"))
(def w1ctl (launch-worker "w1" "pool"))
(def w2ctl (launch-worker "w2" "pool"))
;(def helperctl (launch-helper "pool" 1000))

(comment
  (def c (async/map vector (map #(enqueue "w1" [bogosity 10000 % 113]) (range 50))))
  (def c (async/map vector (map #(enqueue "pool" [bogosity 2 % 111]) (range 5))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 111]) (range 1))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 1 5 111]) (range 1))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 211]) (range 10))))
  (def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 211]) (range 50))))
 )



