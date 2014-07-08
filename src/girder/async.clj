(ns girder.async
  (:require [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [girder.utils]
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
      (trace name "removed" v)
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


(def lchans (atom {}))

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

(defn fn->chan
  "Apply function to arguments, sending the result to a channel, which is returned." 
  [f & args]
  (let [c (chan)]
    (go (>! c (apply f args))
        (close! c))
    c))

(defn fn->lchan
  "Apply function to arguments, sending the result to a channel, which is returned.
If timbre debug level is :trace, then this channel will be instrumented." 
  [f & args]
  (if-not (timbre/level-sufficient? :trace nil)
    (apply fn->chan f args)
    (let  [name   (str "(" (girder.utils/fname f) " " (clojure.string/join args " "))
           c     (lchan name)]
          (go (>! c (apply f args))
              (close! c))
          c)))

(defmacro c-apply
  "Takes a function and arguments, returning a channel, which will receive
{:value <result of applying the function to the arguments>} or {:error <stack trace>}."
  [f & args]
  `(let [c# (lchan #(str "( "~f ~@args ")"))]
     (go (>! c# (try
                 {:value  (~f ~@args)}
                 (catch Exception e#
                   {:error (girder.utils/stack-trace e#)})))
         (close! c#))
     c#))

(defn c-fn
  "Takes a function and returns a function that returns a channel that will receive {:value <result of applying the function to the arguments>} or {:error <stack trace>}."
  [f & args]
  (fn [& more-args]
    (let [c (lchan #(str f ":" args))]
      (go (>! c (try
                  {:value (apply f (concat args more-args))}
                  (catch Exception e
                    {:error (girder.utils/stack-trace e)})))
          (close! c))
      c)))


(defmacro cdefn
  "Defines a function and returns a function that immediately returns a logged channel that will receive
{:value <result of applying the function to the arguments>} or {:error <stack trace>}.  The forms will be executed within a go
block, so they may appropriately use single-! functions in core.async.  Limitation: this macro doesn't do argument
destructuring properly (or at all); it only works for boring argument lists."
[fun args & forms]
  `(defn ~fun ~args 
     (let [c# (lchan (str ~(str fun) (str [~@args])))]
       (go
         (>! c#
             (try {:value  (do ~@forms)}
                  (catch Exception e# {:error (girder.utils/stack-trace e#)})))
         (close! c#))
       c#)))



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

(def  lchans-cleanup-channel (atom nil))
(defn lchans-start-cleanup []
  (swap! lchans-cleanup-channel #(or % (lchans-cleaner 1000))))

(defn lchans-close-all []
  (swap! lchans-cleanup-channel (fn [c] (when c (async/close! c)) nil))
  (swap! lchans
        (fn [m]
          (doseq [c (keys m)] 
            (async/close! c)
            )
          {})))

