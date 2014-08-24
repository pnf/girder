(ns acyclic.girder.grid.async
  (:require [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [acyclic.utils.log :as ulog]
            [clojure.core.cache :as cache]
            [clojure.core.async.impl.protocols :as pimpl :refer [Buffer]]))
(timbre/refer-timbre)

(deftype LoggingBuffer [name buf]
  Buffer
  (full? [this] 
    (let [f (.full? buf)]
      (trace name "full?" f)
      f))
  (remove! [this]
    (trace name "remove!")
    (let [v (.remove! buf)]
      (trace name "removed" (pr-str v))
      v))
  (add! [this item]
    (trace name "adding" (pr-str item))
    (.add! buf item))
  clojure.lang.Counted
  (count [this]
    (let [n (count buf)]
      (trace name "count" n)
      n)))


(defn closed? [c] (pimpl/closed? c))

(defn still-open? [& cs] (not-any? pimpl/closed? cs))

(defn close-all! [& cs] (doall (map async/close! cs)))

(def lchans (atom (cache/weak-cache-factory {})))

#_(defn lchan [name & [buf]]
  (if-not (timbre/level-sufficient? :trace nil)
    (chan buf)
    (let [name (if (fn? name) (name) name)
          buf  (or buf 1)
          buf  (if (number? buf) (async/buffer buf) buf)
          buf (->LoggingBuffer name buf)
          c    (chan buf)]
      (trace "Channel" name "created:" c)
      (swap! lchans #(assoc % c name))
      c)))

(defn lchan [name & [buf]]
  (if-not (timbre/level-sufficient? :trace nil) (chan buf)
          (let [name (if (fn? name) (name) name)
                c    (async/map> #(do (trace "Channel" name "receiving" %) %)
                                 (async/map< #(do (trace "Channel" name "delivering" %) %) (chan buf)))]
            (trace "Channel" name "created:" c)
            (swap! lchans #(assoc % c name))
            c)))

(defn log-chan
  "Add logging to pre-existing channel"
  [name c]
  (if-not (timbre/level-sufficient? :trace nil) c
          (let [name (if (fn? name) (name) name)
                c2 (async/map> #(do (trace "Channel" name "receiving" %) %)
                              (async/map< #(do (trace "Channel" name "delivering" %) %) c))]
            (trace "Channel" name "created:" c2 "logging" c)
            (swap! lchans #(assoc % c2 name))
            c2)))

(defn- remove-if-closed [m c]
  (if-not (closed? c) m
          (do 
            (trace "Channel closed" (get m c) c)
            (dissoc m c))))

(defn lchans-cleaner [msec]
  (trace "Starting up lchans cleanup loop")
  (let [ctl (chan)]
    (async/go-loop []
      (let [t     (async/timeout msec)
            [v c] (async/alts! [ctl t])]
        (if (= c ctl)
          (trace "Shutting down lchans cleanup loop")
          (do (swap! lchans (fn [m] (reduce remove-if-closed m (keys m))))
              (recur)))))
    ctl))

(def lchans-cleanup-channel (atom nil))

#_(defn lchans-start-cleanup []
  (swap! lchans-cleanup-channel #(or % (lchans-cleaner 1000))))

(defn lchans-close-all []
  (swap! lchans-cleanup-channel (fn [c] (when c (async/close! c)) nil))
  (swap! lchans
        (fn [m]
          (doseq [c (keys m)] 
            (async/close! c)
            )
          {})))

