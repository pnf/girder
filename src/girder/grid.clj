(ns girder.grid
  (:use girder.back-end
        girder.async)
  (:require  digest
             [girder.utils]
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))
(timbre/refer-timbre)

;;(timbre/set-level! :debug)
;(girder.redis/init!)

(def back-end (atom nil))

(defn req->reqid [[f & args]]
  (pr-str (concat [(girder.utils/fname f)] args)))

(defn reqid->req [reqid]
  (let [[f & args]  (read-string reqid)
        f           (resolve (symbol f))]
    (apply vector f args)))

(def ^:dynamic *nodeid*)
(def ^:dynamic *reqchan*)

;; State can be nil, :running or :done
;; Don't be paranoid about duplicates.  Worst that can happen is a multiple publish.
;; Even over-writing a done with a :running is OK, since nobody is supposed to read state directly.
;; A result will be a map keyed by :value &| :error.
(defn- process-reqid [nodeid reqchan reqid]
  (let [c           (lchan #(str "process-reqid" nodeid reqid))
        [state val] (get-val  @back-end reqid :state)]
    (go (binding [*nodeid*  nodeid
                  *reqchan* reqchan]
          (>! c
              (condp = state
                :running          (do  (debug "process-reqid" reqid "already running" val) :running)
                :done             (do (debug "process-reqid" reqid "already done" val)
                                      (kv-publish @back-end reqid val)
                                      :done)
                nil               (let [state1     (set-val @back-end reqid :state [:running nodeid])
                                        _          (debug "process-reqid" reqid state1 "->" :running)
                                        req        (reqid->req reqid)
                                        _ (debug req 1111111)
                                        [f & args] req
                                        _ (debug f args 222222222)
                                        cres       (try (apply f args)
                                                        (catch Exception e {:error (girder.utils/stack-trace e)}))
                                        res        (<! cres)
                                        _ (debug req res 333333)
                                        _          (close! cres) ;; to be careful
                                        state2     (set-val @back-end reqid :state [:done res])
                                        _          (debug "process-reqid" reqid state1 "->" state2 "->" :done res)]
                                    (kv-publish @back-end reqid res)
                                    :running))))
        (close! c))
    c))


(defn enqueue [nodeid req]
  (enqueue-listen @back-end
                  nodeid (req->reqid req)
                  :requests :state
                  nil?
                  (fn [[s _]] (= s :done))
                  (fn [[_ v]] v)))

(defn enqueue-reentrant
  "Make one or more requests to be processed, returning a channel to deliver a vector of results."
  [reqs]
  (let [nodeid    *nodeid*
        reqchan   *reqchan*
        out       (lchan #(str "enqeueue-reentrant results " reqs))]
    (debug "enqueue-reentrant" nodeid reqs)
    (go 
      (if-not (seq reqs) (>! out [])
              (let [results   (async/map vector (map #(enqueue nodeid %) reqs))
                    results   (log-chan #(str "internal reentrant results:") results)]
                (async/go-loop []
                  (let [[v c] (async/alts! [results reqchan])]
                    (if (= c results)
                      (do 
                        (trace "enqueue-reentrant got final results: " v)
                        (>! out v)
                        (close! c))
                      (do
                        (trace "enqueue-reentrant handling: " v)
                        (<! (process-reqid nodeid reqchan v))
                        (recur))))))))
    out))

(defn unclaimed [reqid] (nil? (get-val  @back-end reqid :state)))
(defn not-busy [nodeid] (nil? (get-val @back-end nodeid :busy)))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid & [poolid]]
  (let [ctl        (lchan (str  "launch-distributor " nodeid))
        allreqs    (clpop @back-end nodeid :requests)
        reqs       (async/filter< unclaimed allreqs)
        allvols    (clpop @back-end nodeid :volunteers)
        vols       (async/filter< not-busy allvols)
        reqs+vols  (async/map vector [reqs vols])]
    (async/go-loop [volunteering false]
      (debug "distributor" nodeid "waiting for requests and volunteers")
      (let [[v c]       (if volunteering
                          (async/alts! [reqs+vols ctl])
                          (async/alts! [reqs+vols ctl] :default :empty))]
        (debug "distributor" nodeid "got" v c)
        (cond
         (= c ctl)    (do 
                        (debug "Closing distributor" nodeid)
                        (close-all! reqs allreqs vols reqs+vols ctl))
         (= v :empty) (do (when poolid
                            (debug "distributor" nodeid "is bored and volunteering with" poolid)
                            (rpush-and-set @back-end
                                           poolid :volunteers nodeid
                                           nodeid :busy nil))
                          (debug "distributor" nodeid "recurring")
                          (recur true))
         :else        (let [[reqid volid] v]
                        (debug "distributor" nodeid "pushing" reqid "to request queue for" volid)
                        (when poolid (set-val @back-end nodeid :busy true))
                        (rpush @back-end volid :requests reqid)
                        (recur false)))))
    ctl))


;; 1. set busy flag when working
;; 2. Worst case race condition is that we get a single extra job
;; 3. Volunteering should atomically clear the busy flag.

(defn launch-worker
  [nodeid poolid]
  (add-member @back-end poolid :volunteers nodeid)
  (let [ctl       (lchan (str "launch-worker " nodeid))
        allreqs   (clpop @back-end nodeid :requests)
        reqs      (async/filter< unclaimed allreqs)]
    (async/go-loop [volunteering false]
      (debug "worker" nodeid "volunteering state=" volunteering)
      (let [[reqid ch]   (if volunteering
                         (async/alts! [reqs ctl])
                         (async/alts! [reqs ctl] :default :empty))]
        (debug "Worker" nodeid "received" reqid ch)
        (cond
         (= reqid :empty) (do
                          (debug "worker" nodeid "is bored and volunteering with" poolid)
                          (rpush-and-set @back-end
                                         poolid :volunteers nodeid
                                         nodeid :busy nil)
                          (recur true))
         (= ch ctl)  (do (debug "Closing worker" nodeid)
                         (remove-member @back-end poolid :volunteers nodeid)
                         (close-all! reqs allreqs ctl))
         :else        (when reqid
                        (debug "Worker" nodeid " nodeid will now process" reqid)
                        (set-val @back-end nodeid :busy true)
                        (<! (process-reqid nodeid reqs reqid))
                        (recur false)))))
    ctl))

(defn launch-helper
  "Copy reqs from our team."
  [nodeid cycle-msec]
  (let [ctl                (lchan (str "launch-helper nodeid"))]
    (async/go-loop []
      (let [member-nodeids   (get-members @back-end  nodeid :volunteers)
            in-our-queue     (set (qall @back-end nodeid :requests))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (qall @back-end % :requests)) member-nodeids))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        ;(trace "helper" nodeid member-nodeids in-our-queue in-member-queues)
        (when (seq additions)
          (debug "Helper" nodeid in-our-queue in-member-queues "liflting requests" additions)
          (rpush-many @back-end nodeid :requests (vec additions))))
      (if (closed? ctl)
        (debug "Closing helper")
        (do 
          (trace "helper" nodeid "waiting" cycle-msec)
          (<! (timeout cycle-msec))
          (recur))))
    ctl))

(defn cleanup [] (clean-all @back-end))




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

