(ns girder.grid
  (:use girder.back-end
        girder.async)
  (:require  digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]]))
(timbre/refer-timbre)

;;(timbre/set-level! :debug)
;(girder.redis/init!)

(def back-end (atom nil))

(defn beck [x] (prn x) x)


(defn fname
  "Extract the qualified name of a clojure function as a string."
  [f]
  (-> (str f)
      (clojure.string/replace-first "$" "/")
      (clojure.string/replace #"@\w+$" "")
      (clojure.string/replace #"_" "-")))

(defn req->reqid [[f & args]]
  (pr-str (concat [(fname f)] args)))

(defn reqid->req [reqid]
  (let [[f & args]  (read-string reqid)
        f           (resolve (symbol f))]
    (apply vector f args)))


(defn- eval-req [req]
  (try 
    (let [[f & args] req]
      {:value (apply f args)})
    (catch Exception e
      {:error (-> e .getMessage)})))


;; State can be nil, :running or :done
;; Don't be paranoid about duplicates.  Worst that can happen is a multiple publish.
;; Even over-writing a done with a :running is OK, since nobody is supposed to read state directly.
;; A result will be a map keyed by :value &| :error.
(defn- process-req [reqid]
  (let [[state val]     (get-state @back-end reqid)
        _               (debug "process-req" (pr-str reqid) [state val])]
    (condp = state
      :running          (debug "process-req" reqid "already running" val)
      :done             (do (debug "process-req" reqid "already done" val)
                            (kv-publish @back-end reqid val))
      nil               (let [state1 (set-state @back-end reqid :running)
                              _      (debug "process-req" reqid state1 "->" :running)
                              res    (eval-req (reqid->req reqid))
                              state2 (set-state @back-end reqid :done)
                              _      (debug "process-req" reqid state1 "->" state2 "->" :done res)]
                         (kv-publish @back-end reqid res)))))

(defn enqueue [nodeid req]
  (enqueue-listen @back-end
                  nodeid
                  (req->reqid req)
                  nil?
                  #(= "DONE" %)))

(defn enqueue-reentrant
  "Make one or more requests to be processed, returning a channel to deliver a vector of results."
  [nodeid reqs]
  (let [results   (async/map vector (map  #(enqueue nodeid %) reqs))
        reqs      (clpop @back-end nodeid :requests)
        out       (lchan (str "request-stuff " reqs))]
    (async/go-loop []
      (let [[c v] (async/alts! [results reqs])]
        (if (= c results)
          (>! out v)
          (do (process-req v)
              (recur)))))
    out))

(defn unclaimed [reqid] 
  (let [status (get-state @back-end reqid)]
    (or (nil? status) (= "NEW" status))))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid & [poolid]]
  (let [ctl        (lchan (str  "launch-distributor " nodeid))
        allreqs    (clpop @back-end nodeid :requests)
        reqs       (async/filter< unclaimed allreqs)
        vols       (clpop @back-end nodeid :volunteers)
        reqs+vols  (async/map vector [reqs vols])
        volunteer  (and poolid (crpush @back-end poolid :volunteers))]
    (async/go-loop []
      (when volunteer
        (debug "distributor" nodeid "volunteering onto" volunteer "queue for" poolid)
        (>! volunteer nodeid))
      (let [[v c] (async/alts! [reqs+vols ctl])]
        (debug nodeid "got" v c)
        (if-not (and (still-open? allreqs reqs vols reqs+vols ctl) v (= c reqs+vols))
          (do 
            (debug "Closing distributor" nodeid)
            (close-all! reqs allreqs vols reqs+vols ctl))
          (let [[reqid volid] v]
            (debug nodeid "pushing" reqid "to request queue for" volid)
            (rpush @back-end volid :requests reqid)
            (recur)))))
    ctl))

(defn launch-worker
  [nodeid poolid]
  (add-member @back-end poolid :volunteers nodeid)
  (let [ctl       (lchan (str "launch-worker " nodeid))
        allreqs   (clpop @back-end nodeid :requests)
        reqs      (async/filter< unclaimed allreqs)
        volunteer (crpush @back-end poolid :volunteers)
        ]
    (async/go-loop []
      (debug "worker" nodeid "volunteering onto" volunteer "queue for" poolid)
      (>! volunteer nodeid)
      (let [[req ch]  (async/alts! [reqs ctl])]
        (debug "Worker" nodeid "received" req ch)
        (if-not (still-open? reqs allreqs volunteer ctl)
          (do (debug "Closing worker" nodeid)
              (remove-member @back-end poolid :volunteers nodeid)
              (close-all! reqs allreqs volunteer ctl))
          (when req 
            (process-req req)
            (recur)))))
    ctl))

(defn launch-helper
  "Copy reqs from our team."
  [nodeid cycle-msec]
  (let [member-nodeids     (get-members @back-end :volunteers nodeid)
        ctl                (lchan ("launch-helper " nodeid))]
    (async/go-loop []
      (let [in-our-queue     (set (qall @back-end nodeid :requests))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (qall @back-end % :requests)) member-nodeids))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        (apply rpush @back-end nodeid :requests  additions))
      (if-not (still-open? ctl)
        (close! ctl)
        (do 
          (println "Closing helper" nodeid)
          (<! (timeout cycle-msec))
          (recur))))
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

