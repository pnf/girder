(ns girder.grid
  (:use girder.redis
        girder.async)
  (:require  digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))

(def back-end redis-local-default)
(def kvl (kv-listener back-end "STATE"))


(defn req-queue-key [nodeid] (str "req-queue-" nodeid))
(defn req-set-key   [nodeid] (str "req-set-" nodeid))
(defn vols-key [nodeid] (str "vol-queue-" nodeid))
(defn mems-key [nodeid] (str "mem-set-" nodeid))
(defn state-key [reqid] (str "status-" reqid))

(defn register-member [nodeid memberid]
  (let [node-mems-key (mems-key nodeid)]
    (sadd back-end node-mems-key memberid)))

(defn unregister-member [nodeid memberid]
  (let [node-mems-key (mems-key nodeid)]
    (srem back-end node-mems-key memberid)))

(defn enqueue [nodeid reqid]
  (enqueue-listen back-end kvl
                  (req-queue-key nodeid)
                  reqid
                  reqid
                  (state-key reqid)
                  nil?
                  #(= "DONE" %)))

(defn request-stuff
  "Make one or more requests to be processed, returning a channel to deliver a vector of results."
  [nodeid reqids process-fn]
  (let [results   (async/map vector (map  #(enqueue nodeid %) reqids))
        reqs      (clpop back-end (req-queue-key nodeid))
        out       (chan)]
    (async/go-loop []
      (let [[c v] (async/alts! [results reqs])]
        (if (= c results)
          (>! out v)
          (do (process-fn v)
              (recur)))))
    out))

(defn unclaimed [reqid] 
  (let [status (get-scalar (state-key reqid))]
    (some-> status nil? (= "NEW"))))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid]
  (let [ctl        (chan)
        reqs       (async/filter< unclaimed
                                  (clpop back-end (req-queue-key nodeid)))
        vols       (clpop back-end (vols-key nodeid))
        req+vols   (async/map vector reqs vols)]
    (async/go-loop []
      (let [[reqid volid] (async/alts! [req+vols ctl])
            vq            (vols-key volid)]
        (if (and reqid volid) ; one or both will be null on cancellation
          (do 
            (rpush back-end vq reqid)
            (recur))
          (close-all! [reqs vols req+vols ctl]))))
    ctl))

;; TODO (maybe) use sets on back-end
(defn launch-helper
  "Copy reqs from our team."
  [nodeid cycle-msec]
  (let [our-queue          (req-queue-key nodeid)
        member-nodeids     (get-members back-end (mems-key nodeid))
        member-queues      (map req-queue-key member-nodeids)
        ctl                (chan)]
    (async/go-loop []
      (let [in-our-queue     (set (lall back-end our-queue))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (lall back-end %)) member-queues))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        (apply rpush back-end our-queue additions))
      (when-not (closed? ctl)
        (<! (timeout cycle-msec))
        (recur)))
    ctl))

(defn launch-worker
  [nodeid poolid process-fn]
  (register-member poolid nodeid)
  (let [our-queue (clpop back-end (req-queue-key nodeid))
        volunteer (crpush back-end (vols-key poolid))
        reqs      (clpop back-end)
        ctl       (chan)]
    (async/go-loop []
      (>! volunteer nodeid)
      (let [req (<! our-queue)]
        (process-fn req))
      (if (closed? ctl)
        (unregister-member poolid nodeid)
        (recur)))))


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
;;     (go (println "c1" (<! c1)))
;;     (go (println "c2" (<! c2)))
;;     ;(go (>! c "yipee"))
;;     (close! c)
;;     ))

