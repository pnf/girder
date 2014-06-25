(ns girder.async
  (:require [clojure.core.async :as async]
            [taoensso.timbre :as timbre]
            [clojure.core.async.impl.protocols :as pimpl :refer [Buffer]]))
(timbre/refer-timbre)

(deftype LoggingBuffer [name buf]
  Buffer
  (full? [this] (.full? buf))
  (remove! [this]
    (trace name "removing")
    (.remove! buf))
  (add! [this item]
    (trace name "adding" (pr-str item))
    (.add! buf item))
  clojure.lang.Counted
  (count [this] (count buf)))

(defn logging-buffer
  ([name] (LoggingBuffer. name (async/buffer 1)))
  ([name buf] (LoggingBuffer. name buf)))

(defn lchan
  ([name] (async/chan (logging-buffer name)))
  ([name buf] (async/chan (logging-buffer name buf))))

(defn still-open? [& cs]
  (if (not-any? pimpl/closed? cs) true
      (do 
        (doall (map async/close! cs))
        nil)))
