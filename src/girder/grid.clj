(ns girder.grid
  (:use girder.back-end
        girder.redis
        girder.async)
  (:require  digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))
(timbre/refer-timbre)

(def back-end (local-back-end))
(def kvl (kv-listener back-end))


(defn register-member [nodeid memberid]
  (let [node-mems-key (mems-key nodeid)]
    (sadd back-end node-mems-key memberid)))

(defn unregister-member [nodeid memberid]
  (let [node-mems-key (mems-key nodeid)]
    (srem back-end node-mems-key memberid)))

(defn enqueue [nodeid reqid]
  (enqueue-listen back-end kvl
                  nodeid reqid
                  nil?
                  #(= "DONE" %)))

(defn request-stuff
  "Make one or more requests to be processed, returning a channel to deliver a vector of results."
  [nodeid reqids process-fn]
  (let [results   (async/map vector (map  #(enqueue nodeid %) reqids))
        reqs      (clpop back-end (req-queue-key nodeid))
        out       (lchan (str "request-stuff " reqids))]
    (async/go-loop []
      (let [[c v] (async/alts! [results reqs])]
        (if (= c results)
          (>! out v)
          (do (process-fn v)
              (recur)))))
    out))

(defn unclaimed [reqid] 
  (let [status (get-state back-end reqid)]
    (or (nil? status) (= "NEW" status))))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid]
  (let [ctl        (lchan (str  "launch-distributor " nodeid))
        reqs       (async/filter< unclaimed
                                  (clpop back-end (req-queue-key nodeid)))
        vols       (clpop back-end (vols-key nodeid))
        reqs+vols   (async/map vector [reqs vols])]
    (async/go-loop []
      (let [[v c] (async/alts! [reqs+vols ctl])]
        (debug nodeid "got" v c)
        (when (and (still-open? reqs vols reqs+vols ctl) v (= c reqs+vols))
          (let [[reqid volid] v
                work-queue    (req-queue-key volid)]
            (debug nodeid "pushing" reqid "to" work-queue "for" volid)
            (rpush back-end work-queue reqid)
            (recur)))))
    ctl))

;; TODO (maybe) use sets on back-end
(defn launch-helper
  "Copy reqs from our team."
  [nodeid cycle-msec]
  (let [our-queue          (req-queue-key nodeid)
        member-nodeids     (get-members back-end (mems-key nodeid))
        member-queues      (map req-queue-key member-nodeids)
        ctl                (lchan ("launch-helper " nodeid))]
    (async/go-loop []
      (let [in-our-queue     (set (lall back-end our-queue))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (lall back-end %)) member-queues))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        (apply rpush back-end our-queue additions))
      (when (still-open? ctl)
        (<! (timeout cycle-msec))
        (recur)))
    ctl))

(defn launch-worker
  [nodeid poolid process-fn]
  (register-member poolid nodeid)
  (let [our-queue (clpop back-end (req-queue-key nodeid))
        volunteer (crpush back-end (vols-key poolid))
        ctl       (lchan (str "launch-worker " nodeid))]
    (async/go-loop []
      (debug "worker" nodeid "pushing onto" volunteer "queue for" poolid)
      (>! volunteer nodeid)
      (let [req (<! our-queue)]
        (debug "Worker" nodeid "received" req)
        (if (and req (still-open? our-queue volunteer ctl))
          (do (process-fn req)
              (recur))
          (do (debug "Closing" nodeid)
              (unregister-member poolid nodeid)))))
    ctl))


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


;; (defn enqueue-many [node reqids] (async/merge (map (partial enqueue) reqids)))




;; (defn test []
;;   (let [c  (chan)
;;         m  (async/mult c)
;;         c1 (chan)
;;         c2 (chan)]
;;     (async/tap m c1)
;;     (async/tap m c2)
;;     (go (debug "c1" (<! c1)))
;;     (go (debug "c2" (<! c2)))
;;     ;(go (>! c "yipee"))
;;     (close! c)
;;     ))

