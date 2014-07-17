(ns acyclic.girder.back-end-test
  (:use 
   acyclic.girder.back-end
   acyclic.girder.redis)

(:require [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))

(defn test-kv-listener [bend]
    (let [kvl       (kv-listener bend "BLEH")
          [r t a l] kvl

          c     (kv-listen kvl "foo")]
      (println kvl)
      (go (println "foo got some" (<! c)))
      (println kvl)
      (kv-publish kvl "foo" "bar")
      (println kvl)
      (kv-close kvl))
    nil)
