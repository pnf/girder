(ns acyclic.girder.grid.redis
"Implementation of KV-Listener-Manager and Girder-Backend using Redis."
  (:use acyclic.girder.grid.async acyclic.girder.grid.back-end)
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
 )

(timbre/refer-timbre)

(def ^:dynamic *do-hash-key* false)

(defn queue-key [nodeid queue-type] (str (name queue-type) "-queue-" nodeid))
(defn queue-bak-key [nodeid queue-type] (str (name queue-type) "-queue-bak-" nodeid))
(defn set-key [nodeid set-type] (str (name set-type) "-set-" nodeid))
(defn vols-key [nodeid] (str "vol-queue-" nodeid))
(defn tag-key [reqid val-type] (str (name val-type)  "-tag-" reqid))

(defn val-key [reqid val-type] 
  (let [k (str (name val-type)  "-val-" reqid)]
    (if (and *do-hash-key* (> (.length k) 32))
      (digest/md5 k)
      k)))

(defrecord Redis-KV-Listener [topic subs listener])

(defrecord Redis-Backend [redis kvl]

  Girder-Backend

  (clpush [this key queue-type]
    (let [redis  (:redis this)
          key (queue-key key queue-type)
          in (lchan (str "clpush-" key))]
      (async/go-loop []
        (let [val (<! in)]
          (if val
            (do
              (wcar redis (car/lpush key val))
              (recur))
            (debug "clpush" key queue-type "shutting down"))))
      in))

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

  (lpush-many [this key queue-type vals]
    (wcar (:redis this) (apply car/lpush (queue-key key queue-type) vals)))

  (trim [this key queue-type n]
    (let [redis  (:redis this)
          key (queue-key key queue-type)
          res (wcar redis 
                    (car/multi)
                    (car/lrange key n -1)
                    (if (pos? n) (car/ltrim key 0 (dec n))
                        (car/del key))
                    (car/exec))            ;; ["OK" "QUEUED" "QUEUED" [["4" "3"] "OK"]]
          res       (and (vector? res)
                         (= (count res) 4)
                         (last res))
          res       (and (vector? res)
                         (= (count res) 2)
                         (= (last res "OK"))
                         (first res))]
      (and (vector? res) res)))

  (lpush [this key queue-type val]
    (wcar (:redis this) (car/lpush (queue-key key queue-type) val)))

  (lpush-and-set-tag [this
                      qkey queue-type qval
                      vkey tag-type vval]
    (let [qkey (queue-key qkey queue-type)
          vkey (tag-key vkey tag-type)
          r    (if (nil? vval)
                 (wcar (:redis this)
                       (car/multi)
                       (car/del vkey)
                       (car/lpush qkey qval)
                       (car/exec))
                 (wcar (:redis this)
                       (car/multi)
                       (car/set vkey vval)
                       (car/lpush qkey qval)
                       (car/exec)))]
      (trace "lpush-and-set-tag" qkey qval vkey vval r)))

  (clear-bak [this qkeys-qtypes]
    (wcar (:redis this)
          (doseq [[qkey queue-type] (partition 2 qkeys-qtypes)]
            (car/del (queue-bak-key qkey queue-type)))))

  (crpop [this key queue-type]
    (let [qkey   (queue-key key queue-type)
          bkey  (queue-bak-key key queue-type)
          out   (lchan (str "crpop-" key queue-type))]
      (async/go-loop []
        (trace "Calling brpoplpush" key)
        (let [val (wcar (:redis this) (car/brpoplpush qkey bkey 60))]
          (trace "crpop" key "got" val "from redis list" qkey)
          (if (still-open? out)
            (do 
              (trace "crpop" key "still running")
              (when val 
                (debug "Pushing" val "onto" out)
                (>! out val))
              (recur))
            (debug "crpop" key queue-type "shutting down"))))
      out))


  (get-val [this key val-type] (wcar (:redis this) (car/get (val-key key val-type))))
  (set-val [this key val-type val] 
    (let [k (val-key key val-type)]
      (if (nil? val)
        (-> (wcar (:redis this) (car/multi) (car/get k) (car/del k) (car/exec))
            (nth 3)
            first)
        (wcar (:redis this) (car/getset k val)))))

  (get-tag [this key tag-type] (wcar (:redis this) (car/get (tag-key key tag-type))))
  (set-tag [this key tag-type tag] 
    (let [k (tag-key key tag-type)]
      (if (nil? tag)
        (-> (wcar (:redis this) (car/multi) (car/get k) (car/del k) (car/exec))
            (nth 3)
            first)
        (wcar (:redis this) (car/getset k tag)))))



  (qall [this key queue-type] (wcar (:redis this) (car/lrange (queue-key key queue-type) 0 -1)))

  (add-member [this key set-type val]
    (wcar (:redis this) (car/sadd (set-key key set-type) val)))

  (remove-member [this key set-type val]
    (wcar (:redis this) (car/srem (set-key key set-type) val)))

  (get-members [this key set-type]
    (wcar (:redis this) (car/smembers (set-key key set-type))))

  (kv-listen [this k deb]
    (let [{{a :subs
            l :listener} :kvl} this
            c (lchan (str "kv-listen-" k "-" deb) (async/dropping-buffer 1))]
      (swap! a assoc-in [k c] 1)
      c))

  (kv-publish [this k v]
    (let [{redis :redis
           {topic :topic
            a     :subs} :kvl} this]
      (swap! a (fn [subs]
                 (debug "kv-publish" k (get subs k))
                 (go (doseq [c (keys (get subs k))] (>! c v) (close! c)))
                 (dissoc subs k)))

      #_(go (doseq [c (keys (get @a k))] (>! c v) (close! c))
          (swap! a dissoc k))

      (wcar redis
            (car/publish topic [k v]))))

  (kv-close [this]
    (let [{redis  :redis
           {listener :listener} :kvl} this]
      (wcar redis (car/close-listener listener))))

(enqueue-listen
    [this
     nodeid reqid
     queue-type
     debug-info]
  (trace "enqueue-listen at " nodeid "enqueuing" reqid)
  (let [redis (assoc (:redis this) :reqid reqid :nodeid nodeid)
        qkey (queue-key nodeid queue-type)
        c    (kv-listen this reqid debug-info)
        r    (wcar redis (car/rpush qkey reqid))]
      c))

#_(enqueue-listen
    [this
     nodeid reqid
     queue-type val-type
     enqueue-pred done-pred done-extract deb]
    (trace "enqueue-listen at " nodeid " received: " reqid)
    ;; nested wcar - supposed to keep same connection
  (let [redis (assoc (:redis this) :reqid reqid :nodeid nodeid)  ;  :single-conn true
      qkey (queue-key nodeid queue-type)
      vkey (val-key reqid val-type)
      c    (kv-listen this reqid deb)]
      (wcar redis
       (let [v  (second (protocol/with-replies* ; wcar redis  ;
                          (car/watch vkey)
                          (car/get vkey)))
             _     (trace "enqueue-listen at" nodeid "found state of" reqid "=" v c)]
         (cond
          (done-pred v)  (let [v (done-extract v)]
                           (trace "enqueue-listen" reqid "already done, publishing" v)
                           (go (>! c v) (close! c)))
          (enqueue-pred v) (let [r (protocol/with-replies* ; wcar redis  ;; will fail if vkey has been messed with.
                                         (car/multi)
                                         (car/lpush qkey reqid)
                                         (car/exec))]
                             (trace "enqueue-listen enqueueing" reqid r))
          :else           (trace "enqueue-listen" reqid "state already" v))
         (car/unwatch)))
      c))


  (clean-all [this] 
    (trace "Flushing redis" (:redis this))
    (wcar (:redis this) (car/flushdb))))


;; In this version, the subs field is actually a map atom containing
;;     {reqid1    {chana 1
;;                 chanb 1}
;;      reqid2    {chanc 1
;;                {chand 1}}
;; The callback we register with redis/carmine receives values like
;;             [etype topic [reqid value]]
;; If etype is "message", then atomically notify all the subscribed channels and close them,
;; then remove from the map.

(defn- kv-listener [redis topic]
  (let [subs          (atom {}) ;; { key {c1 r1, c2 r2}}
        kv-message-cb  (fn [a [etype _ val :as msg]]
                             (trace "kv-message-cb message" (pr-str msg))
                             (when (and (= etype "message") (vector? val))
                               (let [[k v] val]
                                 (trace "kv-message-cb k=" (pr-str k) "v=" (pr-str v))
                                 (swap! a (fn [cmap]
                                            ;; Notify all channels subscribed to this topic and close them.
                                            (trace "kv-message-cb publishing" k v "to" (keys (get cmap k)))
                                            (doseq [c (keys (get cmap k))] (go (>! c v) #_(close! c)))
                                            (dissoc cmap k))))))
        redis-listener     (car/with-new-pubsub-listener
                             (:spec redis)
                             {topic (partial kv-message-cb subs)}
                             (car/subscribe topic))]
    (->Redis-KV-Listener topic subs redis-listener)))


(defn init!
  ([host port]
     (let [redis   {:pool {}
                    :spec {:host host :port port}}
           kvl     (kv-listener redis "CALCS")]
       (alter-var-root (var acyclic.girder.grid/back-end) #(do % (->Redis-Backend redis kvl)))))
  ([] (init! "localhost" 6379)))
