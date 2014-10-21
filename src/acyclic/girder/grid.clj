(ns acyclic.girder.grid
  (:use acyclic.girder.grid.back-end
        acyclic.girder.grid.async)
  (:require  digest
             [acyclic.utils.log :as ulog :refer [iid]]
             [clojure.core.cache :as cache]
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [ clojure.core.async :as async 
              :refer [<! >! <!! >!! timeout chan alt!! go close!]])
  (:import [java.net InetAddress]))
(timbre/refer-timbre)
;;(timbre/set-level! :debug)

(def back-end nil)

(def ^:dynamic *nodeid* nil)
(def ^:dynamic *reqchan* nil)
(def ^:dynamic *current-reqid* nil)
(def ^:dynamic *baton* nil)

(def ^:private do-stack? true)


(defn ->reqid [req]
  (if (string? req)
    req
    (let [[f & args] req] 
      (pr-str (concat [(ulog/fname f)] args)))))

(defn format-exception [e]
  (let [m {:req *current-reqid*
           :msg (.getMessage e)
           :stack (ulog/stack-trace e)}
        i  (ex-data e)]
    (if i (assoc m :info i) m)))


(defn cfn [f]
  (fn [& args]
    (let [c (lchan #(str f ":" args))]
      (go (>! c (try
                  {:value (apply f args)}
                  (catch Exception e
                    {:error (format-exception e)})))
          #_(close! c))
      c)))


(defn ->req [reqid]
  (if (string? reqid)
    (let [[f & args]  (read-string reqid)
          f           (resolve (symbol f))
          f           (if (:girded (meta f)) f (cfn f))]
      (apply vector f args))
    reqid))

(defn req+id [r] [(->req r) (->reqid r)])


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


(defmacro pass-baton [cmt bc bv]
  `(go
    (trace "Passing baton:" ~cmt ~bv)
    (>! ~bc ~bv)))

(defmacro grab-baton [cmt bc]
  `(do
    (trace "Reaching for baton:"  ~cmt)
    (trace "Got baton:" ~cmt  (<! ~bc))))


(defn enqueue [nodeid req deb]
  (let [id    (iid deb "ENQ")
        reqid (->reqid req)]
    (trace "enqueue" nodeid reqid do-stack? id)
    (enqueue-listen back-end nodeid reqid :requests id)))

(defn seq-reentrant
  "Make one or more requests to be processed, returning a channel that delivers results in sequence,
closing when complete."
  [reqs nodeid reqchan]
  (let [id        (iid "SQR")
        reqids    (map ->reqid reqs)
        out       (lchan #(str "enqeueue-reentrant out" id nodeid))]
    (debug "seq-reentrant" id nodeid reqids)
    (if-not (seq reqs)
      (close! out)
      (let [_   (async/onto-chan reqchan reqids false)
            rcs (map #(enqueue nodeid % id) reqids)]
        (async/go-loop [[rc & rcs] rcs]
          (if-not rc
            (close! out)
            (do 
              (>! out (<! rc))
              (recur rcs))))))
    out))


(defn enqueue-reentrant
  "Make one or more requests to be processed, returning a channel to deliver a vector of results."
  [reqs nodeid reqchan]
  (chan-to-vec-chan (seq-reentrant reqs nodeid reqchan)))


(defn ex-aggregate-errors [reqs res]
  (let [r+e (filter second (map #(vector %1 (:error %2)) reqs res))
        m   (reduce #(assoc %1 (->reqid (first %2)) (second %2)) {} r+e)]
    (ex-info "Error from request" m)))


(defmacro call-reentrant [reqs baton nodeid] 
  `(let [rc#  (enqueue-reentrant ~reqs ~nodeid *reqchan*)
         _#   (pass-baton "call-reentrant " ~baton [:call-reentrant ~nodeid *current-reqid*])
         res# (<! rc#)
         _#   (trace "call-reentrant got results" ~reqs res#)
         _#   (grab-baton "call-reentrant" ~baton)]
     (if (some :error res#)
       (throw (ex-aggregate-errors ~reqs res#))
       (map :value res#))))


(defn- vecify [form] (if (list? form) (vec form) form))

(defmacro request 
  ([nodeid form [& sec]]
     `(let [req# ~(vecify form)
            id#  (iid "REQ")
            rc# (enqueue ~nodeid req# id#)
            sec (or sec (* 24 3600))  ;; 1 day is long enough!
            to# (timeout (* 1000 sec))
            [res# c#]  (async/alts!! [rc# to#])]
        (if (= c# to#)
          (throw (ex-info "Timeout during grid execution" {:sec sec}))
          (if-not (:error res#) (:value res#)
                  (throw (ex-info "Error during grid execution" res#))))))
  ([form]
     `(first (call-reentrant [~(vecify form)] *baton*  *nodeid*))))

(defmacro requests
  ([nodeid reqs]
     `(let [id#   (iid "REQS")
            res#  (<!! (async/map vector (map #(enqueue ~nodeid % id#) ~reqs)))
            errs# (filter :error res#)]
        (if (seq errs#)
          (throw (ex-info "Error during grid execution" {:error errs#}))
          (map :value res#))))
  ([reqs]
     `(call-reentrant ~reqs *baton* *nodeid*)))

(defn extract-value
  "Extract wrapped value if available, throwing error if set."
  [res]
  (if (:error res)
    (throw (ex-info "Error during grid execution" {:error res}))
           (:value res)))

(defmacro seq-request
  ([nodeid reqs]
     `(let [id# (iid "SREQ")
            rc# (ordered-merge (map #(enqueue ~nodeid % id#) ~reqs))]
        (map extract-value (chan-to-seq rc#))))
  ([reqs]
     `(let [rc# (seq-reentrant ~reqs *nodeid*)]
        (map extract-value (chan-to-seq rc#)))))

(defn not-busy [nodeid] (nil? (get-tag back-end nodeid :busy)))

(defn- register
  "Register our node and type, returning nil if it was already registered."
  [nodeid  nodetype]
  (let [v (get-tag back-end nodeid :register)
        l  (.getHostAddress (InetAddress/getLocalHost))
        l  (str nodetype "-" l)]
    (debug "Starting" nodeid nodetype "at" l)
    (if v
      (do 
        (error "Pre-existing" nodeid v)
        nil)
      (do  (set-tag back-end nodeid :register l)
           true))))

(defn- un-register [nodeid poolid]
  (debug "Terminating" nodeid)
  (set-tag back-end nodeid :register nil)
  (when poolid (remove-member back-end poolid :volunteers nodeid)))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid & [poolid]]
  (and (register nodeid "distributor")
       (let [ctl        (lchan (str  "launch-distributor " nodeid))
             reqs       (crpop back-end nodeid :requests)
             allvols    (crpop back-end nodeid :volunteers)
             vols       (async/filter< not-busy allvols)
             reqs+vols  (async/map vector [reqs vols])]
         (debug "distributor" nodeid "starting")
         (when poolid  (add-member back-end poolid :volunteers nodeid))
         (async/go-loop [volunteering false]
           (trace "distributor" nodeid "waiting for requests and volunteers")
           (let [[v c]       (if volunteering
                               (async/alts! [reqs+vols ctl])
                               (async/alts! [reqs+vols ctl] :default :empty))]
             (debug "distributor" nodeid "got" v c)
             (cond
              (= c ctl)    (do 
                             (un-register nodeid poolid)
                             (close-all! reqs reqs vols reqs+vols ctl))
              (= v :empty) (do (when poolid
                                 (debug "distributor" nodeid "is bored and volunteering with" poolid)
                                 (lpush-and-set-tag back-end
                                                    poolid :volunteers nodeid
                                                    nodeid :busy nil))
                               (trace "distributor" nodeid "recurring")
                               (recur true))
              :else        (let [[reqid volid] v]
                             (debug "distributor" nodeid "pushing" reqid "to request queue for" volid)
                             (when poolid (set-tag back-end nodeid :busy true))
                             (lpush back-end volid :requests reqid)
                             (clear-bak back-end [nodeid :volunteers nodeid :requests])
                             (recur false)))))
         ctl)))


  ;; State can be nil, :running or :done
  ;; Don't be paranoid about duplicates.  Worst that can happen is a multiple publish.
  ;; Even over-writing a done with a :running is OK, since nobody is supposed to read state directly.
  ;; A result will be a map keyed by :value &| :error.
(def local-cache (atom (cache/soft-cache-factory {})))
(defn- process-reqid [nodeid baton reqchan req-or-id & [deb]]
  (let [id          (iid deb "PRQ")
        [req reqid] (req+id req-or-id)
        res         (get @local-cache reqid)]
    (go
      (if res
        (do  
          (debug "process-reqid found cached value" id nodeid reqid)
          (pass-baton "process-reqid" baton [:cached res id])
          (kv-publish back-end reqid res))
        (let [[state val] (get-val back-end reqid :state)
              bval (condp = state
                     :running (do 
                                (debug "process-reqid" id nodeid reqid "already running" val)
                                [:running val])
                     :done    (do (debug "process-reqid" id nodeid reqid "already done" val)
                                  (kv-publish back-end reqid val)
                                  [:done val id])
                     nil     (let [state1     (set-val back-end reqid :state [:running nodeid])
                                   _          (debug "process-reqid" id nodeid reqid state1 "->" :running)
                                   [f & args] req
                                   cres       (binding [*nodeid*        nodeid
                                                        *reqchan*       reqchan
                                                        *current-reqid* (->reqid reqid)
                                                        *baton*         baton]
                                                (apply f args))
                                   res        (<! cres) ;; blocks
                                   state2     (set-val back-end reqid :state [:done res])]
                               (swap! local-cache assoc reqid res)
                               (debug "process-reqid" id nodeid reqid state1 "->" state2 "->" :done res)
                               (kv-publish back-end reqid res)
                               [:calcd res id]))]
          (pass-baton id baton bval))))))

(defn launch-worker
  [nodeid poolid]
  (add-member back-end poolid :volunteers nodeid)
  (let [id          (str "worker-" nodeid)
        ctl         (lchan (str id "-ctl"))
        remote-reqs (crpop back-end nodeid :requests)
        local-reqs  (lchan (str id "-local") (async/sliding-buffer 100))
        baton       (lchan (str "baton-" nodeid))]
    (debug "worker" nodeid "starting")
    (async/go-loop [volunteering false]
      (let [_            (pass-baton id baton [:listening id])
            _            (trace id "listening" volunteering)
            [reqid ch]   (if volunteering
                           (async/alts! [remote-reqs ctl])
                           (async/alts! [local-reqs ctl] :default :empty))
            _            (debug id "received" reqid ch)
            _            (grab-baton id baton)]
        (cond
         (= reqid :empty) (do
                          (debug "worker" nodeid "is bored and volunteering with" poolid)
                          (clear-bak back-end [nodeid :requests])
                          (lpush-and-set-tag back-end
                                             poolid :volunteers nodeid
                                             nodeid :busy nil)
                          (recur true))
         (= ch ctl)  (do (debug "Closing worker" nodeid)
                         (remove-member back-end poolid :volunteers nodeid)
                         (close-all! remote-reqs local-reqs ctl))
         :else        (do
                        (debug id "will now process" reqid "from" (if (= ch local-reqs) "local" "remote"))
                        (set-tag back-end nodeid :busy reqid)
                        (process-reqid nodeid  baton local-reqs reqid nodeid)
                        ;; blocks until process-reqid waits
                        (grab-baton id baton)
                        (recur false)))))
    ctl))


(defn launch-helper
  "Copy reqs from our team."
  [nodeid cycle-msec]
  (let [ctl                (lchan (str "launch-helper nodeid"))]
    (debug "helper" nodeid "starting")
    (async/go-loop []
      (let [member-nodeids   (get-members back-end  nodeid :volunteers)
            in-our-queue     (set (qall back-end nodeid :requests))
            in-member-queues (apply clojure.set/union 
                                    (map #(set (qall back-end % :requests)) member-nodeids))
            additions   (clojure.set/difference in-member-queues in-our-queue)]
        ;(trace "helper" nodeid member-nodeids in-our-queue in-member-queues)
        (when (seq additions)
          (debug "Helper" nodeid in-our-queue in-member-queues "lifting requests" additions)
          (lpush-many back-end nodeid :requests (vec additions))))
      (if (closed? ctl)
        (debug "Closing helper")
        (do 
          (trace "helper" nodeid "waiting" cycle-msec)
          (<! (timeout cycle-msec))
          (recur))))
    ctl))

(defn cleanup [] (clean-all back-end))
