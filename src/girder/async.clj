(ns girder.async
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as pimpl :refer [Buffer]]))

(deftype LoggingBuffer [name buf log]
  Buffer
  (full? [this] (.full? buf))
  (remove! [this]
    (log name "removing")
    (.remove! buf))
  (add! [this item]
    (log name "adding" item)
    (.add! buf item))
  clojure.lang.Counted
  (count [this] (count buf)))

(defn logging-buffer
  ([name log] (LoggingBuffer. name (async/buffer 1) log))
  ([name buf log] (LoggingBuffer. name buf log))
)

(defn printing-buffer
  ([name] (logging-buffer name (async/buffer 1) prn))
  ([name buf] (LoggingBuffer. name buf prn)))

(defn closed? [c] (pimpl/closed? c))
(def close-all! [cs] (doseq [c cs] (close! c)))
