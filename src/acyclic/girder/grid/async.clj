(ns acyclic.girder.grid.async
  (:require [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [acyclic.utils.log :as ulog]
            [acyclic.girder.grid.cache :as cache]
            [clojure.core.async.impl.protocols :as pimpl :refer [Buffer]])
  (:import (java.util LinkedList)))
(timbre/refer-timbre)


(defn ordered-merge [cs]
  (let [out (chan)]
    (go  (doseq [c cs] (>! out (<! c)))
         (close! out))
    out))

(defn chan-to-seq
  "Drains a channel onto a lazy sequence.  Blocks internally."
  [c & [terminate?]]
  (lazy-seq
   (when-let [v (if terminate?
                  (first (async/alts!! [c] :default nil))
                  (<!! c))]
     (cons v (chan-to-seq c terminate?)))))


(defn chan-to-vec-chan
  "Returns a channel which returns a vector, onto which the the channel has been drained."
  [c]
  (async/go-loop [acc []]
    (if-let [v (<! c)]
      (recur (conj acc v))
      acc)))

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
      (swap! lchans #(assoc % name c))
      c)))

(defn lchan [name & [buf]]
  (if-not (timbre/level-sufficient? :trace nil) (chan buf)
          (let [name (if (fn? name) (name) name)
                c    (async/map> #(do (trace "Channel" name "receiving" %) %)
                                 (async/map< #(do (trace "Channel" name "delivering" %) %) (chan buf)))]
            (trace "Channel" name "created:" c)
            (swap! lchans #(assoc % name c))
            c)))

(defn log-chan
  "Add logging to pre-existing channel"
  [name c]
  (if-not (timbre/level-sufficient? :trace nil) c
          (let [name (if (fn? name) (name) name)
                c2 (async/map> #(do (trace "Channel" name "receiving" %) %)
                              (async/map< #(do (trace "Channel" name "delivering" %) %) c))]
            (trace "Channel" name "created:" c2 "logging" c)
            (swap! lchans #(assoc % name c2))
            c2)))

(defn c-stack  [& [id]]
  (let [s-push  (lchan (str id "-stack-push"))
        s-pop   (lchan (str id "-stack-pop"))
        s-steal (lchan (str id "-stack-steal"))
        s-count (lchan (str id "-stack-count"))
        s       (java.util.LinkedList.)]
    (async/go-loop []
      (trace "Stack" id s)
      (if (seq s)
        (async/alt!
          s-push                ([v _] (.push s v))
          [[s-pop   (first s)]] (.removeFirst s)
          [[s-steal (last s)]]  (.removeLast s)
          [[s-count (count s)]] :count)
        (async/alt!
          s-push                ([v _] (.push s v))
          [[s-count (count s)]] :count))
        (recur))
    [s-push s-pop s-steal s-count]))



        #_(let [[v c] (async/alts! [[s-pop (first s)] [s-steal [(last s) (dec  (count s))]] s-push])]
          (cond
           (nil? v) nil
           (= c s-push)  (do (.push s v))
           (= c s-pop)   (do (.removeFirst s))
           (= c s-steal) (do (.removeLast s))))

        #_(let [v (<! s-push)]
          (.push s v))


(defn lchans-close-all []
  (swap! lchans
        (fn [m]
          (doseq [c (vals m)] 
            (async/close! (.get  c)))
          (cache/weak-cache-factory {}))))
