(ns girder.redis
  (:use girder.async)
   (:require [taoensso.carmine :as car :refer (wcar)]
             digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))

(def redis-local-default {:pool {} :spec {:host "localhost" :port 6379}})

(defn crpush [redis key]
  (let [in (chan)]
    (async/go-loop []
      (when-let [val (<! in)]                  ; otherwise it's closed
        (wcar redis (car/rpush key val))
        (recur)))
  in))

(defn clpop [redis key]
  (let [out (chan)]
    (async/go-loop []
      (let [[_ val] (wcar redis (car/blpop key 10))]
        (when-not (closed? out)
          (when val (>! out val))
          (recur))))
    out))

(defn rpush
  ([redis key val & vals]
     (wcar redis (apply car/rpush key val vals)))
  ([redis key val]
     (wcar redis (car/rpush key val))))

(defn members [redis key]
  (wcar redis (car/smembers key)))


(defn lall [redis key] (wcar redis (car/lrange key 0 -1)))
