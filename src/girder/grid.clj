(ns girder.grid
  (:use girder.redis)
  (:require  digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))

(def be redis-local-default)

;; Also consider filtering out requests in progress here.
(defn distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid]
  (let [ctl        (chan)
        reqs       (clpop be (str "req-queue-" nodeid))
        vols       (clpop be (str "vol-queue-"  nodeid))
        req+vols   (async/map vector reqs vols)]
    (async/go-loop []
      (let [[reqid volid] (async/alts! [req+vols ctl])
            vq            (str "req-queue-" volid)]
        (if (and reqid volid)
          (do 
            (rpush be vq reqid)
            (recur))
          (close-all! [reqs vols req+vols ctl]))))
    ctl))

;; Or would it be more sensible to enqueue at parent immediately?  I'd like to avoid it
;; to keep strategy hidden from worker.
(defn helper
  "Copy reqs from our team."
  [nodeid]
  (let [nq       (str "req-queue-" nodeid)
        nm       (str "members-" nodeid)
        mids     (smembers be nm)
        mqs      (map #(str "req-queue-" %) mids)
        ctl      (chan)]
    (go-loop []
      (let [existing   (set (lall redis nq))
            candidates (apply clojure.set/union 
                              (map #(set (lall redis %)) mqs))
            admitted   (clojure.set/difference candidates existing)]
        (wcar be (apply rpush nq admitted)))
      (when-not (closed? ctl)
        (<! (timeout 1000))
        (recur)))
    ctl)))))


;; (def topology ["commons" 0
;;                ["group1" 1000 ["worker1a" 100] ["worker1b" 100]]
;;                ["group2" ["worker2a" 100] ["worker2b" 100]]])

;; (defn add-member [[node migration-time & members] parent]
;;   (prn node migration-time members parent)
;;   (wcar*  (car/hset "queue-parents" node parent)
;;           (car/hset "queue-migration-times" node migration-time)
;;           (car/del (str "queue-" node)))
;;   (doseq [m members] (add-member m node)))

;; (defn define-toplogy [topo]
;;   (wcar* (car/del "queue-parents" "queue-migration-times"))
;;   (add-member topo "-"))

;; (defn enqueue [node reqid]
;;   (let [c         (chan)
;;         qloc      (str "queue-" node)
;;         sloc      (str "req-state-" reqid)
;;         _         (wcar* (car/watch sloc))
;;         listener  (car/with-new-pubsub-listener (:spec redis-pool)
;;                     {sloc #(go (>! c %) (close! c))}
;;                     (car/subscribe sloc))
;;         [_ state] (wcar* (car/get sloc))]
;;     (condp = state
;;       "DONE"      (go (>! c "DONE"))
;;       nil         (wcar* (car/multi)                 ; will fail if state changed
;;                          (car/rpush qloc reqid)
;;                          (car/publish qloc reqid)
;;                          (car/exec)))
;;     (wcar* (car/unwatch))
;;     c))

;; (defn enqueue-many [node reqids] (async/merge (map (partial enqueue) reqids)))




;; (defn test []
;;   (let [c  (chan)
;;         m  (async/mult c)
;;         c1 (chan)
;;         c2 (chan)]
;;     (async/tap m c1)
;;     (async/tap m c2)
;;     (go (println "c1" (<! c1)))
;;     (go (println "c2" (<! c2)))
;;     ;(go (>! c "yipee"))
;;     (close! c)
;;     ))

