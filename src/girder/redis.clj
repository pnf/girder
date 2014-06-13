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

(defn push [redis key val]
  (wcar redis (car/rpush key val)))

(defn clpop [redis key]
  (let [out   (chan)]
    (async/go-loop []
      (let [[_ val] (wcar redis (car/blpop key 10))]
        (when val
          (when-not (closed? out)
                (>! out val)
                (recur)))))
    out))

(defn- kv-message-listener [a [etype topic val :as msg]]
  (prn  msg)
  (when (and (= etype "message") (vector? val))
    (let [[k v] val]
      (prn k v)
      (swap! a (fn [cmap]
                 (doseq [c (keys (get cmap k))] (go (>! c v) (close! c)))
                 (dissoc cmap k))))))


;; Listen on topic for [x y] messages and publish
(defn kv-listener [redis topic]
  (let [a    (atom {})   ;; { key {c1 r1, c2 r2}} -- rs currently ignored
        l    (car/with-new-pubsub-listener
               (:spec redis)
               {topic (partial kv-message-listener a)}
               (car/subscribe topic)) ]
    [redis topic a l]))

(defn kv-listen [kvl k]
  (let [[_ _ a l] kvl
         c       (chan)]
    (swap! a (fn [cmap] (assoc-in cmap [k c] 1)))
    c))

(defn kv-publish [kvl k v]
  (let [[redis topic _ _] kvl]
    (wcar redis (car/publish topic [k v]))))

(defn kv-close [kvl]
  (let [[redis _ _ listener] kvl] 
    (wcar redis (car/close-listener listener))))

(defn- test-kv-listener []
  (let [kvl   (kv-listener redis-local-default "BLEH")
        [r t a l] kvl
        c     (kv-listen kvl "foo")]
    (println kvl)
    (go (println "foo got some" (<! c)))
    (println kvl)
    (kv-publish kvl "foo" "bar")
    (println kvl)
    (kv-close kvl))
  nil)

(defn rpush
  ([redis key val & vals]
     (wcar redis (apply car/rpush key val vals)))
  ([redis key val]
     (wcar redis (car/rpush key val))))

(defn get-members [redis key]
  (wcar redis (car/smembers key)))

(defn get-scalar [redis key] (wcar redis (car/get key)))

(defn lall [redis key] (wcar redis (car/lrange key 0 -1)))

(defn watch [redis key]
  (wcar redis (car/watch key)))

(defn unwatch [redis key]
  (wcar redis (car/unwatch key)))

(defn sadd [redis key val]
  (wcar redis (car/sadd key val)))

(defn srem [redis key val]
  (wcar redis (car/srem key val)))


(defn enqueue-listen
  "Enqueue request rkey at qkey, and listen for result, which will be simultaneously
published to pkey and written to vkey.  The two predicates determine, based on
the value before enqueuing, whether to enqueue at all and whether we're actually
done."
  [redis kvl 
   qkey rkey pkey vkey 
   enqueue-pred done-pred]
  (let [[_ v] (wcar redis
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
    c))
