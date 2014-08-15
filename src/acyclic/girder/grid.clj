(ns acyclic.girder.grid
  (:use acyclic.girder.grid.back-end
        acyclic.girder.grid.async)
  (:require  digest
             [acyclic.utils.log :as ulog]
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]])
  (:import [java.net InetAddress])
)
(timbre/refer-timbre)
;;(timbre/set-level! :debug)

(def back-end (atom nil))

(def ^:dynamic *nodeid* nil)
(def ^:dynamic *reqchan* nil)
(def ^:dynamic *current-req* nil)
(def ^:dynamic *current-reqid* nil)
;(def ^:dynamic *trail* ())

(defn req->reqid [[f & args]]
  (pr-str (concat [(ulog/fname f)] args)))

(defn format-exception [e]
  (let [m {:req *current-reqid*
           :msg (.getMessage e)
           :stack (ulog/stack-trace e)}
        i  (ex-data e)]
    (if i (assoc m :info i) m)))


(defn- cfn [f]
  (fn [& args]
    (let [c (lchan #(str f ":" args))]
      (go (>! c (try
                  {:value (apply f args)}
                  (catch Exception e
                    {:error (format-exception e)})))
          (close! c))
      c)))


(defn reqid->req [reqid]
  (let [[f & args]  (read-string reqid)
        f           (resolve (symbol f))
        g           (:girded (meta f))
        _           (debug "Girded=" g)
        f           (if g f (cfn f))]
    (apply vector f args)))



(defmacro cdefn
  "Defines a function and returns a function that immediately returns a logged channel that will receive
{:value <result of applying the function to the arguments>} or {:error <stack trace>}.  The forms will be executed within a go
block, so they may appropriately use single-! functions in core.async.  Limitation: this macro doesn't do argument
destructuring properly (or at all); it only works for boring argument lists."
[fun args & forms]
`(defn ~(vary-meta fun assoc :girded true)  ~args 
   (let [c# (lchan (str ~(str fun) (str [~@args])))]
     (go
       (>! c#
           (try {:value  (do ~@forms)}
                (catch Exception e# {:error (format-exception e#)})))
       (close! c#))
     c#)))

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
                                          _          (debug "req=" req)
                                          [f & args] req
                                          _          (debug "meta f" (meta f))
                                          cres       (binding [*current-req*   req
                                                               *current-reqid* reqid]
                                                       (try (apply f args)
                                                            (catch Exception e {:error (ulog/stack-trace e)})))
                                          res        (<! cres)
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



(defn ex-aggregate-errors [reqs res]
  (let [r+e (filter second (map #(vector %1 (:error %2)) reqs res))
        m   (reduce #(assoc %1 (req->reqid (first %2)) (second %2)) {} r+e)]
    (ex-info "Error from request" m)))

(defmacro call-reentrant [reqs] 
  `(let [res# (<! (enqueue-reentrant ~reqs))]
     (if (some :error res#)
       (throw (ex-aggregate-errors ~reqs res#))
       (map :value res#))))


(defn- vecify [form] (if (list? form) (vec form) form))

(defmacro request 
  ([nodeid form]
     `(let [req# ~(vecify form)
            res#  (<!! (enqueue ~nodeid req#))]
        (if-not (:error res#) (:value res#)
                (throw (ex-info "Error during grid execution" res#)))))
  ([form]
     `(first (call-reentrant [~(vecify form)]))))


(defmacro requests 
  ([nodeid reqs]
     `(let [res#  (<!! (async/map vector (map #(enqueue ~nodeid %) ~reqs)))
            errs# (filter :error res#)]
        (if (seq errs#)
          (throw (ex-info "Error during grid execution" {:errors errs#}))
          (map :value res#))))
  ([reqs]
     `(call-reentrant ~reqs)))


;(defmacro req [form] (apply vector form))
;(defmacro reqs [& forms] (vec  (map #(apply vector %) forms)))

(defn unclaimed [reqid] (nil? (get-val  @back-end reqid :state)))
(defn not-busy [nodeid] (nil? (get-val @back-end nodeid :busy)))

(defn- register
  "Register our node and type, returning nil if it was already registered."
  [nodeid  nodetype]
  (let [v (get-val @back-end nodeid :register)
        l  (.getHostAddress (InetAddress/getLocalHost))
        l  (str nodetype "-" l)]
    (debug "Starting" nodeid nodetype "at" l)
    (if v
      (do 
        (error "Pre-existing" nodeid v)
        nil)
      (do  (set-val @back-end nodeid :register l)
           true))))

(defn- un-register [nodeid poolid]
  (debug "Terminating" nodeid)
  (set-val @back-end nodeid :register nil)
  (when poolid (remove-member @back-end poolid :volunteers nodeid)))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid & [poolid]]
  (and (register nodeid "distributor")
       (let [ctl        (lchan (str  "launch-distributor " nodeid))
             allreqs    (crpop @back-end nodeid :requests)
             reqs       (async/filter< unclaimed allreqs)
             allvols    (crpop @back-end nodeid :volunteers)
             vols       (async/filter< not-busy allvols)
             reqs+vols  (async/map vector [reqs vols])]
         (debug "distributor" nodeid "starting")
         (when poolid  (add-member @back-end poolid :volunteers nodeid))
         (async/go-loop [volunteering false]
           (trace "distributor" nodeid "waiting for requests and volunteers")
           (let [[v c]       (if volunteering
                               (async/alts! [reqs+vols ctl])
                               (async/alts! [reqs+vols ctl] :default :empty))]
             (debug "distributor" nodeid "got" v c)
             (cond
              (= c ctl)    (do 
                             (un-register nodeid poolid)
                             (close-all! reqs allreqs vols reqs+vols ctl))
              (= v :empty) (do (when poolid
                                 (debug "distributor" nodeid "is bored and volunteering with" poolid)
                                 (lpush-and-set @back-end
                                                poolid :volunteers nodeid
                                                nodeid :busy nil))
                               (trace "distributor" nodeid "recurring")
                               (recur true))
              :else        (let [[reqid volid] v]
                             (debug "distributor" nodeid "pushing" reqid "to request queue for" volid)
                             (when poolid (set-val @back-end nodeid :busy true))
                             (lpush @back-end volid :requests reqid)
                             (clear-bak @back-end [nodeid :volunteers nodeid :requests])
                             (recur false)))))
         ctl)))


;; 1. set busy flag when working
;; 2. Worst case race condition is that we get a single extra job
;; 3. Volunteering should atomically clear the busy flag.

(defn launch-worker
  [nodeid poolid]
  (add-member @back-end poolid :volunteers nodeid)
  (let [ctl       (lchan (str "launch-worker " nodeid))
        allreqs   (crpop @back-end nodeid :requests)
        reqs      (async/filter< unclaimed allreqs)]
    (debug "worker" nodeid "starting")
    (async/go-loop [volunteering false]
      (trace "worker" nodeid "volunteering state=" volunteering)
      (let [[reqid ch]   (if volunteering
                         (async/alts! [reqs ctl])
                         (async/alts! [reqs ctl] :default :empty))]
        (debug "Worker" nodeid "received" reqid ch)
        (cond
         (= reqid :empty) (do
                          (debug "worker" nodeid "is bored and volunteering with" poolid)
                          (clear-bak @back-end [nodeid :requests])
                          (lpush-and-set @back-end
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
    (debug "worker" nodeid "starting")
    (async/go-loop []
      (let [member-nodeids   (get-members @back-end  nodeid :volunteers)
            in-our-queue     (set (qall @back-end nodeid :requests))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (qall @back-end % :requests)) member-nodeids))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        ;(trace "helper" nodeid member-nodeids in-our-queue in-member-queues)
        (when (seq additions)
          (debug "Helper" nodeid in-our-queue in-member-queues "lifting requests" additions)
          (lpush-many @back-end nodeid :requests (vec additions))))
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

