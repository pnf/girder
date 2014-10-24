(ns acyclic.girder.grid
  (:use acyclic.girder.grid.back-end
        acyclic.girder.grid.async)
  (:require  digest
             [acyclic.utils.log :as ulog :refer [iid]]
             [clojure.core.cache :as cache]
             ;[clj-time.core :as ct]
             ;[clj-time.coerce :as co]
             [taoensso.timbre :as timbre]
             [clojure.core.async :as async 
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
    (debug "seq-reentrant" id nodeid *current-reqid* ":" reqids)
    (if-not (seq reqs)
      (close! out)
      ;; force evaluation, so all notifications are setup before enqueuing
      (let [rcs (doall (map #(kv-listen back-end % id) reqids))]
        (async/onto-chan reqchan reqids false)
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

(defn- volunteer [nodeid poolid]
  (debug nodeid "is bored and volunteering with" poolid)
  (lpush-and-set-tag back-end
                     poolid :volunteers nodeid
                     nodeid :busy nil))
(defn- un-volunteer [nodeid excuse]
  (set-tag back-end nodeid :busy excuse))

(defn launch-distributor
  "Listen for requests and volunteers.
   When we find one of each, add request to volunteer's queue."
  [nodeid & [poolid msec]]
  (and (register nodeid "distributor")
       (let [id         (str "distributor-" nodeid)
             ctl        (lchan (str  "launch-distributor " nodeid))
             reqs       (crpop back-end nodeid :requests)
             allvols    (crpop back-end nodeid :volunteers)
             vols       (async/filter< not-busy allvols)
             reqs+vols  (async/map vector [reqs vols])]
         (debug  id "starting")
         (when poolid
           (add-member back-end poolid :volunteers nodeid)
           (volunteer nodeid poolid))
         (async/go-loop [local-timer nil
                         n-processed 0]
           (trace id "waiting for requests and volunteers")
           (let [[v c]       (if-not local-timer
                               (async/alts! [reqs+vols ctl])
                               (async/alts! [reqs+vols local-timer ctl] :default :empty))]
             (trace id "got" v c)
             (if (= c local-timer)
               (let [reqs (trim back-end nodeid :requests n-processed)]
                 (when reqs
                   (debug id "timer" reqs)
                   (lpush-many back-end poolid :requests reqs))
                 (recur (timeout msec) 0))
               (cond
                (= c ctl)    (do 
                               (un-register nodeid poolid)
                               (close-all! reqs reqs vols reqs+vols ctl))
                (= v :empty) (do (when poolid
                                   (volunteer nodeid poolid))
                                 (recur nil 0))
                :else        (let [[reqid volid] v]
                               (debug id "pushing" reqid "to request queue for" volid)
                               (when poolid (set-tag back-end nodeid :busy true))
                               (lpush back-end volid :requests reqid)
                               (clear-bak back-end [nodeid :volunteers nodeid :requests])
                               (recur (and msec (timeout msec))
                                      (inc n-processed)))))))
         ctl)))

  ;; State can be nil, :running or :done
  ;; Don't be paranoid about duplicates.  Worst that can happen is a multiple publish.
  ;; Even over-writing a done with a :running is OK, since nobody is supposed to read state directly.
  ;; A result will be a map keyed by :value &| :error.

(def ^:private local-cache (atom (cache/soft-cache-factory {})))
(def ^:private set-state-queue (lchan "set-state" 500))
(def ^:private set-state-thread
  (async/go-loop []
    (let [[reqid state val] (<! set-state-queue)]
      (set-val back-end reqid :state [state val]))
    (recur)))

(defn- process-reqid [nodeid baton reqchan req-or-id & [deb]]
  (go
    (let [id          (iid deb "PRQ")
          [req reqid] (req+id req-or-id)
          [state val] (or (get @local-cache reqid)
                          (get-val back-end reqid :state))
          bval (condp = state
                 :running
                 (do (debug "process-reqid" id nodeid reqid "already running" val)
                     [:running val])
                 :done
                 (do (debug "process-reqid" id nodeid reqid "already done" val)
                     (kv-publish back-end reqid val)
                     [:done val id])
                 nil
                 (do (>! set-state-queue [reqid :running nodeid])
                     (swap! local-cache assoc reqid [:running nodeid])
                     (debug "process-reqid" id nodeid reqid "->" :running)
                     (let [[f & args] req
                           cres       (binding [*nodeid*        nodeid
                                                *reqchan*       reqchan
                                                *current-reqid* reqid
                                                *baton*         baton]
                                        (apply f args))
                           res        (<! cres)] ;; blocks
                       (>! set-state-queue [reqid :done res])
                       (swap! local-cache assoc reqid [:done res])
                       (debug "process-reqid" id nodeid reqid "->" :done res)
                       (kv-publish back-end reqid res)
                       [:calcd res id])))]
      (pass-baton id baton bval))))


;; multi-threading:
;; same queue; different batons?
;; different queues, explicit redistribution to local distributor?
;; FJ style: pull from peers' queues when idle?
;; How to maintain accurate rate estimate?


(defn launch-worker
  [nodeid poolid msec]
  (add-member back-end poolid :volunteers nodeid)
  (let [id          (str "worker-" nodeid)
        ctl         (lchan (str id "-ctl"))
        remote-reqs (crpop back-end nodeid :requests)
        [local-push
         local-pop
         local-steal
         local-count] (c-stack id)
        baton       (lchan (str "baton-" nodeid))]
    (debug "worker" nodeid "starting")
    (volunteer nodeid poolid)
    (async/go-loop [local-timer   nil
                    n-processed  0]
      (pass-baton id baton [:listening id])
      (trace id "listening" (if local-timer "locally" "remotely") n-processed)
      (let [[reqid ch]   (if local-timer
                           (async/alts! [local-pop local-timer ctl] :default :empty)
                           (async/alts! [local-pop remote-reqs ctl]))]
        (if (= ch local-timer)
          (let [n-stack (<! local-count)
                n-steal (- n-stack n-processed)
                reqs    (when (pos? n-steal) (<! (async/into [] (async/take n-steal local-steal))))]
            (debug id "timer" n-processed n-stack n-steal reqs)
            (when reqs
              (lpush-many back-end poolid :requests reqs))
            (recur (timeout msec) 0))
          (do 
            (grab-baton id baton)
            (debug id "received" reqid ch)
            (cond
             (= reqid :empty) (do
                                (clear-bak back-end [nodeid :requests])
                                (volunteer nodeid poolid)
                                (recur nil 0))
             (= ch ctl)  (do (debug "Closing worker" nodeid)
                             (remove-member back-end poolid :volunteers nodeid)
                             (close-all! remote-reqs local-push local-pop ctl))
             :else        (do
                            (debug id "will now process" reqid "from" (if (= ch local-pop) "local" "remote"))
                            (when-not local-timer
                              (un-volunteer nodeid reqid))
                            (process-reqid nodeid  baton local-push reqid nodeid)
                            ;; blocks until process-reqid blocks
                            (grab-baton id baton)
                            (recur (or local-timer (timeout msec))
                                   (inc n-processed)))))

          )))
    ctl))


(defn cleanup [] (clean-all back-end))

