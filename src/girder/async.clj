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
          (let [name (if (fn?) #(name) name)
                c    (async/map> #(do (trace "Channel" name "receiving" %) %)
                                 (async/map< #(do (trace "Channel" name "delivering" %) %) (chan buf)))]
            (trace "Channel" name "created:" c)
            (swap! lchans #(assoc % c name))
            c)))


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


#_(defn lchan [name & [buf]]
  (if-not (timbre/level-sufficient? :debug nil)
    (async/chan buf)
    (let [buf    (or buf 0)
          buf    (if (number? buf) (async/buffer buf) buf)
          buf    (LoggingBuffer. name buf)
          c (async/chan buf)]
      (lchans-start-cleanup)
      (swap! lchans #(assoc-in % [name c] nil))
      c)))

#_(def wctl (async/chan))

#_(defn wstate []
  (let [wc (chan 100)]
    (async/go-loop [cs {wctl ["control" wctl (LoggingBuffer. "control" (async/buffer 0))]}]
      (let [[v ct]  (async/alts! (vec (keys cs)))]
        (cond
         (and (= ct wctl) (:stop v))  "stopped"
         (and (= ct wctl) (:new v))   (let [[ch ct name buf] (:new v)]
                                        (trace "New channel" name ch buf)
                                        (recur (assoc cs ct [name ch buf])))
         (and (= ct wctl) (:dump v)) (let [c   (:dump v)
                                           res (map (fn [[ct [name ch buf]]] [ch name (-> buf .buf .buf)]) cs)]
                                       (if (= (type wctl) (type c)) (>! c res) (info "Channel dump:" res))
                                       (recur cs))
         (nil? v)   (let [[name ch buf] (get cs ct)]
                      (trace "Closed" name ch)
                      (recur (dissoc cs ct)))
         :else      (let [[name ch buf] (get cs ct)]
                      (trace name ch "received:" v "buffer:" (.buf buf))
                      (recur cs)))))
    wctl))

#_(defn wchan [name & [buf]]
  (if-not (timbre/level-sufficient? :debug nil)
    (async/chan buf)
    (let [buf    (or buf 0)
          buf    (if (number? buf) (async/buffer buf) buf)
          lbuf   (LoggingBuffer. name buf)
          ch (async/chan lbuf)
          mu (async/mult ch)
          ct (chan)
          _  (async/tap mu ct)]
      (>!! wctl {:new [ch ct name lbuf]})
      ch)))
