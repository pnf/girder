(ns girder.grid.redis
"Implementation of KV-Listener-Manager and Girder-Backend using Redis."
  (:use girder.grid.async girder.grid.back-end)
  (:require [taoensso.carmine :as car :refer (wcar)]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]
             [taoensso.timbre :as timbre]
             digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]])
  (:import [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(timbre/refer-timbre)

(defn queue-key [nodeid queue-type] (str (name queue-type) "-queue-" nodeid))
(defn set-key [nodeid set-type] (str (name set-type) "-set-" nodeid))
(defn vols-key [nodeid] (str "vol-queue-" nodeid))
(defn val-key [reqid val-type ] (str (name val-type)  "-val-" reqid))

(defrecord Redis-KV-Listener [topic publisher listener])

(defrecord Redis-Backend [redis kvl]

  Girder-Backend

  (crpush [this key queue-type]
    (let [redis  (:redis this)
          key (queue-key key queue-type)
          in (lchan (str "crpush-" key))]
      (async/go-loop []
        (let [val (<! in)]
          (if val
            (do
              (wcar redis (car/rpush key val))
              (recur))
            (debug "crpush" key queue-type "shutting down"))))
      in))

  (rpush-many [this key queue-type vals]
    (wcar (:redis this) (apply car/rpush (queue-key key queue-type) vals)))

  (rpush [this key queue-type val]
    (wcar (:redis this) (car/rpush (queue-key key queue-type) val)))

  (rpush-and-set [this
                  qkey queue-type qval
                  vkey val-type vval]
    (let [qkey (queue-key qkey queue-type)
          vkey (val-key   vkey val-type)
          r    (if (nil? vval)
                 (wcar (:redis this)
                       (car/multi)
                       (car/del vkey)
                       (car/rpush qkey qval)
                       (car/exec))
                 (wcar (:redis this)
                       (car/multi)
                       (car/set vkey vval)
                       (car/rpush qkey qval)
                       (car/exec)))]
      (trace "rpush-and-set" qkey qval vkey vval r)))

  (clpop [this key queue-type]
    (let [key   (queue-key key queue-type)
          out   (lchan (str "clpop-" key))]
      (async/go-loop []
        (trace "Calling blpop" key)
        (let [[qkey val] (wcar (:redis this) (car/blpop key 60))]
          (trace "clpop" key "got" val "from redis list" qkey)
          (if (still-open? out)
            (do 
              (trace "clpop" key "still running")
              (when val 
                (debug "Pushing" val "onto" out)
                (>! out val))
              (recur))
            (debug "clpop" key queue-type "shutting down"))))
      out))

  (get-val [this key val-type] (wcar (:redis this) (car/get (val-key key val-type))))
  (set-val [this key val-type val] 
    (let [k (val-key key val-type)]
      (if (nil? val)
        (-> (wcar (:redis this) (car/multi) (car/get k) (car/del k) (car/exec))
            (nth 3)
            first)
        (wcar (:redis this) (car/getset k val)))))

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
            c (lchan (str "kv-listen" k))]
      ;(debug "Here we are in kv-listen" a l c)
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

  #_(enqueue-listen
    [this
     nodeid reqid
     queue-type val-type
     enqueue-pred done-pred done-extract]
    (trace "enqeueue-listen at " nodeid " received: " reqid)
    ;; nested wcar - supposed to keep same connection
    (let [redis (assoc (:redis this) :reqid reqid :nodeid nodeid)
      qkey (queue-key nodeid queue-type)
      vkey (val-key reqid val-type)
      c    (kv-listen this reqid)]
      (car/atomic redis 1
       (let [_    (car/watch vkey)
             v    (car/get vkey)
             _     (trace "enqeueue-listen at" nodeid "found state of" reqid "=" v c)]
         (cond
          (done-pred v)  (let [v (done-extract v)]
                           (trace "enqeue-listen" reqid "already done, publishing" v)
                           (go (>! c v) (close! c)))
          ;; If the following transaction fails, state must of changed to :running
          ;; or :done, in which case someone else has or will soon have published
          ;; the result.
          (enqueue-pred v) (let [r     (do (car/multi)
                                           (car/rpush qkey reqid)
                                           (car/exec))]
                             (trace "enqueue-listen enqueueing" reqid r))
          :else           (trace "enqeue-listen" reqid "state already" v))
         (car/unwatch)
         ))
      c))

(enqueue-listen
    [this
     nodeid reqid
     queue-type val-type
     enqueue-pred done-pred done-extract]
    (trace "enqeueue-listen at " nodeid " received: " reqid)
    ;; nested wcar - supposed to keep same connection
  (let [redis (assoc (:redis this) :reqid reqid :nodeid nodeid)  ;  :single-conn true
      qkey (queue-key nodeid queue-type)
      vkey (val-key reqid val-type)
      c    (kv-listen this reqid)]
      (wcar redis
       (let [v  (second (protocol/with-replies* ; wcar redis  ;
                          (car/watch vkey)
                          (car/get vkey)))
             _     (trace "enqeueue-listen at" nodeid "found state of" reqid "=" v c)]
         (cond
          (done-pred v)  (let [v (done-extract v)]
                           (trace "enqeue-listen" reqid "already done, publishing" v)
                           (go (>! c v) (close! c)))
          (enqueue-pred v) (let [r (protocol/with-replies* ; wcar redis  ;; will fail if vkey has been messed with.
                                         (car/multi)
                                         (car/rpush qkey reqid)
                                         (car/exec))]
                             (trace "enqueue-listen enqueueing" reqid r))
          :else           (trace "enqeue-listen" reqid "state already" v))
         (wcar redis (car/unwatch))
         ))
      c))

  (clean-all [this] 
    (trace "Flushing redis" (:redis this))
    (wcar (:redis this) (car/flushdb)))

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
                             (trace "kv-message-cb message" (pr-str msg))
                             (when (and (= etype "message") (vector? val))
                               (let [[k v] val]
                                 (trace "kv-message-cb k=" (pr-str k) "v=" (pr-str v))
                                 (swap! a (fn [cmap]
                                            ;; Notify all channels subscribed to this topic and close them.
                                            (trace "kv-message-cb publishing" v "to" (keys (get cmap k)))
                                            (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                                            (dissoc cmap k))))))
        redis-listener     (car/with-new-pubsub-listener
                                 (:spec redis)
                                 {topic (partial kv-message-cb publisher)}
                                 (car/subscribe topic))]
        (->Redis-KV-Listener topic publisher redis-listener)))

(def pool-defaults {:when-exhausted-action GenericKeyedObjectPool/WHEN_EXHAUSTED_BLOCK
                    :max-wait  -1})

(defn init!
  ([host port]
     (let [redis   {:pool {}
                    :spec {:host host :port port}}
           kvl     (kv-listener redis "CALCS")]
       (reset! girder.grid/back-end (->Redis-Backend redis kvl))))
  ([] (init! "localhost" 6379)))
