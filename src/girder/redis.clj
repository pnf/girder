(ns girder.redis
"Implementation of KV-Listener-Manager and Girder-Backend using Redis."
  (:use girder.async girder.back-end)
   (:require [taoensso.carmine :as car :refer (wcar)]
             [taoensso.timbre :as timbre]
             digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))
(timbre/refer-timbre)

(defn req-queue-key [nodeid] (str "req-queue-" nodeid))
(defn req-set-key   [nodeid] (str "req-set-" nodeid))
(defn vols-key [nodeid] (str "vol-queue-" nodeid))
(defn mems-key [nodeid] (str "mem-set-" nodeid))
(defn state-key [reqid] (str "status-" reqid))


(defrecord Redis-KV-Listener [redis topic state listener]

  KV-Listener-Manager

  (kv-listen [kvl k]
    (let [{a :state l :listener} kvl
          c                      (lchan (pr-str kvl k))]
      (swap! a (fn [cmap] (assoc-in cmap [k c] 1)))
      c))

  (kv-publish [kvl k v]
    (let [[redis topic _ _] kvl]
      (wcar redis (car/publish topic [k v]))))

  (kv-close [kvl]
    (let [[redis _ _ listener] kvl] 
      (wcar redis (car/close-listener listener)))))

(defrecord Redis-Backend [config]

  Girder-Backend

  (crpush [redis key]
    (let [in (lchan (str "crpush-" key))]
      (async/go-loop []
        (when-let [val (<! in)]                  ; otherwise it's closed
          (wcar redis (car/rpush key val))
          (recur)))
      in))

  (push [redis key val]
    (wcar redis (car/rpush key val)))

  (rpush [redis key val & vals]
     (wcar redis (apply car/rpush key val vals)))
  (rpush [redis key val]
     (wcar redis (car/rpush key val)))

  (clpop [redis key]
    (let [out   (lchan (str "clpop-" key))]
      (async/go-loop []
        (debug "Calling blpop" key)
        (let [[qkey val] (wcar redis (car/blpop key 10))]
          (debug "clpop" key "got" val "from redis list" qkey)
          (when (still-open? out)
            (debug "clpop" key "still running")
            (when val 
              (debug "Pushing" val "onto" out)
              (>! out val))
            (recur))))
      out))  

  
  ;; Listen on topic for [x y] messages and publish

  (kv-listener [redis topic]
    (letfn [(kv-message-listener [a [etype topic val :as msg]]
    (prn  msg)
    (when (and (= etype "message") (vector? val))
      (let [[k v] val]
        (prn k v)
        (swap! a (fn [cmap]
                   (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                   (dissoc cmap k))))))]


      (let [a    (atom {})   ;; { key {c1 r1, c2 r2}} -- rs currently ignored
            l    (car/with-new-pubsub-listener
                   (:spec redis)
                   {topic (partial kv-message-listener a)}
                   (car/subscribe topic)) ]
        (->Redis-KV-Listener redis topic a l))))

  (kv-listener [redis] (kv-listener redis "CALCS"))

  (get-members [redis key]
    (wcar redis (car/smembers key)))

  (get-state [redis key] (wcar redis (car/get (state-key key))))

  (lall [redis key] (wcar redis (car/lrange key 0 -1)))

  (watch [redis key]
    (wcar redis (car/watch key)))

  ( unwatch [redis key]
    (wcar redis (car/unwatch key)))

  (sadd [redis key val]
    (wcar redis (car/sadd key val)))

  (srem [redis key val]
    (wcar redis (car/srem key val)))

  (enqueue-listen
    [redis kvl
     nodeid reqid
     enqueue-pred done-pred]
    (let [qkey (req-queue-key nodeid)
          rkey reqid
          pkey reqid
          vkey (state-key reqid)
          [_ v] (wcar redis
                      (car/watch vkey)
                      (car/get vkey))
          c     (kv-listen kvl pkey)]
      (cond
       (done-pred v)  (go (>! c v) (close! c))
       (enqueue-pred v) (wcar redis
                              (car/multi)
                              (car/rpush qkey rkey)
                              (car/exec)))
      (wcar redis (car/unwatch))
      c)))


(defn local-back-end [] (->Redis-Backend  {:pool {} :spec {:host "localhost" :port 6379}}))
