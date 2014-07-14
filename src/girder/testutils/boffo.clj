(ns girder.testutils.boffo
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [girder.utils]
            girder.grid.redis)
  (:use girder.grid
        girder.grid.async))
(timbre/refer-timbre)


(timbre/set-level! :trace)
(girder.grid.redis/init!)
(cleanup)

(cdefn bogosity [msec jobnum args]
  (Thread/sleep msec)
  (str "Bogosity:" *nodeid* ":" jobnum ":" msec ":" args))

(cdefn recbog [msec jobnum reclevel numrecjobs args]
  (let [_ (debug "here we are in recbog")
        reqs (map #(vector recbog msec % (dec reclevel)  numrecjobs args) (range reclevel))
        _   (debug "recbog asking for" reqs)
        cre (enqueue-reentrant reqs)
        res (<! cre)
        _   (debug "recbog got" res)
        vs  (map :value res)]
    (Thread/sleep msec)
    (str "RecBog:" *nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")    ))


(def poolctl (launch-distributor "pool"))
(def w1ctl (launch-worker "w1" "pool"))
(def w2ctl (launch-worker "w2" "pool"))
;(def helperctl (launch-helper "pool" 1000))

;(def c (async/map vector (map #(enqueue "w1" [bogosity 10000 % 113]) (range 50))))
;(def c (async/map vector (map #(enqueue "pool" [bogosity 2 % 111]) (range 5))))
;(def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 111]) (range 1))))
;(def c (async/map vector (map #(enqueue "pool" [recbog 1 % 1 5 111]) (range 1))))
;(def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 211]) (range 10))))
;(def c (async/map vector (map #(enqueue "pool" [recbog 1 % 0 5 211]) (range 50))))


