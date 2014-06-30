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

(defn queue-key [nodeid queue-type] (str (name queue-type) "-queue-" nodeid))
(defn set-key [nodeid set-type] (str (name set-type) "-set-" nodeid))
(defn vols-key [nodeid] (str "vol-queue-" nodeid))
(defn state-key [reqid] (str "status-" reqid))

(defrecord Redis-KV-Listener [topic publisher listener])

(defrecord Redis-Backend [redis kvl]

  Girder-Backend

  (crpush [this key queue-type]
    (let [redis  (:redis this)
          key (queue-key key queue-type)
          in (lchan (str "crpush-" key))]
      (async/go-loop []
        (when-let [val (<! in)]         ; otherwise it's closed
          (wcar redis (car/rpush key val))
          (recur)))
      in))

  (rpush [this key queue-type val & vals]
    (wcar (:redis this) (apply car/rpush (queue-key key queue-type) val vals)))
  (rpush [this key queue-type val]
    (wcar (:redis this) (car/rpush (queue-key key queue-type) val)))

  (clpop [this key queue-type]
    (let [key   (queue-key key queue-type)
          out   (lchan (str "clpop-" key))]
      (async/go-loop []
        (debug "Calling blpop" key)
        (let [[qkey val] (wcar (:redis this) (car/blpop key 60))]
          (debug "clpop" key "got" val "from redis list" qkey)
          (when (still-open? out)
            (debug "clpop" key "still running")
            (when val 
              (debug "Pushing" val "onto" out)
              (>! out val))
            (recur))))
      out))
  
  (get-state [this key] (wcar (:redis this) (car/get (state-key key))))
  (set-state [this key val] (wcar (:redis this) (car/getset key val)))

  (qall [this key queue-type] (wcar (:redis this) (car/lrange (queue-key key queue-type) 0 -1)))

  (add-member [this key set-type val]
    (wcar (:redis this) (car/sadd (set-key key set-type) val)))

  (remove-member [this key set-type val]
    (wcar (:redis this) (car/srem (set-key key set-type) val)))

  (get-members [this key set-type]
    (wcar (:redis this) (car/smembers (set-key key set-type))))

  #_(kv-listen [kvl k]
    (let [c   (lchan (pr-str kvl k))
          pu  (:publisher kvl)]
      (debug "Subscribing on" pu "for" k)
      (async/sub pu k c)
      (async/map second [c])))

  (kv-listen [this k]
    (let [{{a :publisher
            l :listener} :kvl} this
           c             (lchan (pr-str kvl k))]
      (debug "Here we are in kv-listen" a l c)
      (swap! a (fn [cmap] (assoc-in cmap [k c] 1)))
      c))

  (kv-publish [this k v]
    (let [{redis :redis
           {topic :topic} :kvl} this]
      (wcar redis
            (car/publish topic [k v]))))

  (kv-close [this]
    (let [{redis  :redis
           {listener :listener} :kvl} this]
      (wcar redis (car/close-listener listener))))

  (enqueue-listen
    [this
     nodeid reqid
     enqueue-pred done-pred]
    (debug "enqueue/listen" this nodeid reqid enqueue-pred done-pred)
    (let [redis (:redis this)
          qkey (queue-key nodeid :requests)
          vkey (state-key reqid)
          [_ v] (wcar redis
                      (car/watch vkey)
                      (car/get vkey))
          c     (kv-listen this reqid)]
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
      c))



)


;; In this version, the publisher field is actually a map atom containing
;;     {reqid1    {chana 1
;;                 chanb 1}
;;      reqid2    {chanc 1
;;                {chand 1}}
;; The callback we register with redis/carmine receives values like
;;             [etype topic [reqid value]]
;; If etype is "message", then atomically notify all the subscribed channels and close them,
;; then remove from the map.

(defn- kv-listener [redis topic]
  (let [publisher          (atom {}) ;; { key {c1 r1, c2 r2}}
        kv-message-cb      (fn [a [etype _ val :as msg]]
                             (debug "kv-message-cb message" (pr-str msg))
                             (when (and (= etype "message") (vector? val))
                               (let [[k v] val]
                                 (debug "kv-message-cb k=" (pr-str k) "v=" (pr-str v))
                                 (swap! a (fn [cmap]
                                            ;; Notify all channels subscribed to this topic and close them.
                                            (debug "kv-message-cb publishing" v "to" (keys (get cmap k)))
                                            (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                                            (dissoc cmap k))))))
        redis-listener     (car/with-new-pubsub-listener
                                 (:spec redis)
                                 {topic (partial kv-message-cb publisher)}
                                 (car/subscribe topic))]
        (->Redis-KV-Listener topic publisher redis-listener)))

(defn init!
  ([host port]
     (let [redis   {:pool {} :spec {:host host :port port}}
           kvl     (kv-listener redis "CALCS")]
       (wcar redis (car/flushdb))
       (reset! girder.grid/back-end (->Redis-Backend redis kvl))))
  ([] (init! "localhost" 6379)))
