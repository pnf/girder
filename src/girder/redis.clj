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

(defrecord Redis-KV-Listener [redis topic publisher listener]

  KV-Listener-Manager

  #_(kv-listen [kvl k]
    (let [c   (lchan (pr-str kvl k))
          pu  (:publisher kvl)]
      (debug "Subscribing on" pu "for" k)
      (async/sub pu k c)
      (async/map second [c])))

  (kv-listen [kvl k]
    (let [{a :publisher
           l :listener} kvl
           c             (lchan (pr-str kvl k))]
      (swap! a (fn [cmap] (assoc-in cmap [k c] 1)))
      c))

  (kv-publish [kvl k v]
    (let [{redis :redis
           topic :topic} kvl]
      (wcar redis (car/publish topic [k v]))))

  (kv-close [kvl]
    (let [{listener :listener
           redis    :redis}   kvl] 
      (wcar redis (car/close-listener listener)))))

(defrecord Redis-Backend [config]

  Girder-Backend

  (req-queue-key [redis nodeid] (str "req-queue-" nodeid))
  (req-set-key   [redis nodeid] (str "req-set-" nodeid))
  (vols-key [redis nodeid] (str "vol-queue-" nodeid))
  (mems-key [redis nodeid] (str "mem-set-" nodeid))
  (state-key [redis reqid] (str "status-" reqid))
  (val-key [redis reqid] (str "value-" reqid))

  (crpush [redis key]
    (let [in (lchan (str "crpush-" key))]
      (async/go-loop []
        (when-let [val (<! in)]         ; otherwise it's closed
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
        (let [[qkey val] (wcar redis (car/blpop key 60))]
          (debug "clpop" key "got" val "from redis list" qkey)
          (when (still-open? out)
            (debug "clpop" key "still running")
            (when val 
              (debug "Pushing" val "onto" out)
              (>! out val))
            (recur))))
      out))  

  
  ;; Experimental version using core.async pub/sub.
  ;; I'm worried about the circumstances where the source channel might block, and
  ;; I would also like the subscribing channels to be closed immediately.
  #_(kv-listener [redis topic]
      (let [source                  (chan)
            publisher               (async/pub source first)
            kv-message-listener (fn [[etype topic kv :as msg]]
                                  (debug "kv-message-listener" (pr-str msg))
                                  (when (and (= etype "message") (vector? kv))
                                    (do (debug "kv-message-listener publishing" kv)
                                        (go (>! source kv)))))
            redis-listener      (car/with-new-pubsub-listener
                                  (:spec redis)
                                  {topic kv-message-listener}
                                  (car/subscribe topic))]
        (->Redis-KV-Listener redis topic publisher redis-listener)))

  ;; In this version, the publisher field is actually a map atom containing
  ;;     {reqid1    {chana 1
  ;;                 chanb 1}
  ;;      reqid2    {chanc 1
  ;;                {chand 1}}
  ;; The callback we register with redis/carmine receives values like
  ;;             [etype topic [reqid value]]
  ;; If etype is "message", then atomically notify all the subscribed channels and close them,
  ;; then remove from the map.
  #_(kv-listener [redis topic]
    (letfn [(kv-message-cb [a [etype topic val :as msg]]
              (debug "kv-message-cb" (pr-str msg))
              (when (and (= etype "message") (vector? val))
                (let [[k v] val]
                  (debug "kv-message-cb" (pr-str k) (pr-str v))
                  (swap! a (fn [cmap]
                             ;; Notify all channels subscribed to this topic and close them.
                             (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                             (dissoc cmap k))))))]
      (let [publisher          (atom {}) ;; { key {c1 r1, c2 r2}}
            redis-listener     (car/with-new-pubsub-listener
                                 (:spec redis)
                                 {topic (partial kv-message-cb publisher)}
                                 (car/subscribe topic))]
        (->Redis-KV-Listener redis topic publisher redis-listener))))



(kv-listener [redis topic]
  (let [publisher          (atom {}) ;; { key {c1 r1, c2 r2}}
        kv-message-cb      (fn [a [etype _ val :as msg]]
                             (debug "kv-message-cb" (pr-str msg))
                             (when (and (= etype "message") (vector? val))
                               (let [[k v] val]
                                 (debug "kv-message-cb" (pr-str k) (pr-str v))
                                 (swap! a (fn [cmap]
                                            ;; Notify all channels subscribed to this topic and close them.
                                            (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                                            (dissoc cmap k))))))
        redis-listener     (car/with-new-pubsub-listener
                                 (:spec redis)
                                 {topic (partial kv-message-cb publisher)}
                                 (car/subscribe topic))]
        (->Redis-KV-Listener redis topic publisher redis-listener)))

  (kv-listener [redis] (kv-listener redis "CALCS"))

  (get-members [redis key]
    (wcar redis (car/smembers key)))

  (get-state [redis key] (wcar redis (car/get (state-key redis key))))

  (lall [redis key] (wcar redis (car/lrange key 0 -1)))

  (watch [redis key]
    (wcar redis (car/watch key)))

  (unwatch [redis key]
    (wcar redis (car/unwatch key)))

  (sadd [redis key val]
    (wcar redis (car/sadd key val)))

  (srem [redis key val]
    (wcar redis (car/srem key val)))

  (enqueue-listen
    [redis kvl
     nodeid reqid
     enqueue-pred done-pred]
    (debug "enqueue/listen" redis kvl nodeid reqid enqueue-pred done-pred)
    (let [qkey (req-queue-key redis nodeid)
          vkey (state-key redis reqid)
          [_ v] (wcar redis
                      (car/watch vkey)
                      (car/get vkey))
          c     (kv-listen kvl reqid)]
      (cond
       (done-pred v)  (do 
                        (debug "enqeue-listen" reqid "already done")
                        (go (>! c v) (close! c)))
       (enqueue-pred v) (do 
                          (debug "enqueue-listen" reqid "sending" v)
                          (wcar redis
                                (car/multi)
                                (car/rpush qkey reqid)
                                (car/exec)))
       :else           (debug "enqeue-listen" reqid "doing nothing"))
      (wcar redis (car/unwatch))
      c)))


(defmacro init-local! []
  '(do
     (def girder.grid/back-end (girder.redis/->Redis-Backend  {:pool {} :spec {:host "localhost" :port 6379}}))
     (def girder.grid/kvl (kv-listener back-end "CALCS"))))
