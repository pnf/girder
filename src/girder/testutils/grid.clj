(ns girder.testutils.grid
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [girder.grid :as grid]
            [girder.utils :as gu]
            [girder.grid.async :as ga]
            girder.grid.redis)
  (:gen-class))
(timbre/refer-timbre)

(defmacro timeit
  "Evaluate forms within a try/catch block, returning a map containing :result or :exception as
appropriate, and the elapsed :time in msec."
  [id & forms]
  `(let [t1#  (.getTime (java.util.Date.))
         res# (try {:result (do ~@forms)}
                   (catch Exception e# {:exception (gu/stack-trace e#)}))
         t2#  (.getTime (java.util.Date.))
         dt#  (- t2# t1#)]
     (merge {:time dt# :id ~id} res#)))

(ga/cdefn recbog [msec jobnum reclevel numrecjobs args]
  (let [_ (debug "here we are in recbog")
        reqs (map #(vector recbog msec % (dec reclevel)  numrecjobs args) (range reclevel))
        _   (debug "recbog asking for" reqs)
        cre (grid/enqueue-reentrant reqs)
        res (<! cre)
        _   (debug "recbog got" res)
        vs  (map :value res)]
    (Thread/sleep msec)
    (str "RecBog:" grid/*nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")    ))

(def cli-options
  [[nil "--worker WS" "Launch one or more workers, with comma-delimited names"]
   [nil "--distributor DIST" "Launch a distributor"]
   [nil "--host HOST" "Redis host" :default "localhost"]
   [nil "--port PORT" "Redis port" :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--jobmsecs MSECS" "Amount of time for the job to take" :parse-fn #(Integer/parseInt %) :default 2]
   [nil "--pool POOLID" "Pool ID" :default nil]
   [nil "--helper MSECS" "Launch helper for the designated pool" :parse-fn #(Integer/parseInt %) :default nil]
   [nil "--jobs N" :default 0 :parse-fn #(Integer/parseInt %)]
   [nil "--jobtimeout SECS" "Time to wait." :parse-fn #(Integer/parseInt %) :default 10]
   [nil "--reclevel N" "Number of recursions" :parse-fn #(Integer/parseInt %) :default 0]
   [nil "--cleanup"]
   ["-i" "--id NUM" "Some id number" :default 0 :parse-fn #(Integer/parseInt %)]
   [nil "--log LEVEL" "Debug level" :parse-fn keyword :default :debug]
   ["-o" "--opts OPTS" "EDN string" :default nil :parse-fn read-string]
   [nil "--repl" "Set when running in REPL, so exit isn't called"]
   [nil "--hang SECS" "After launching, hang around for this period before exiting automatically."
    :parse-fn #(Integer/parseInt %) :default 0]
   [nil "--cmt COMMENT" "Some comment to stick into job request ids" :default (str (rand))]
   ["-h" "--help"]])

(defn doit [opts]
  (let [{:keys [reclevel cmt help id pool worker distributor host port jobmsecs jobs jobtimeout log helper cleanup]} opts]
    (timeit id
            {:cleanup
             (when cleanup (grid/cleanup))
             :distributor
             (when distributor
               (let [ds (clojure.string/split distributor #",")]
                 (for [d ds]
                   (grid/launch-distributor d pool))))
             :helper
             (when helper
               (grid/launch-helper pool helper))
             :worker
             (when worker
               (let [ws (clojure.string/split worker #",")]
                 (for [w ws]
                   (grid/launch-worker w pool))))
             :jobs
             (when (and pool jobs (pos? jobs))
               (let [c (async/map vector (map #(grid/enqueue pool [recbog jobmsecs % reclevel jobs cmt]) (range jobs)))
                     t (async/timeout (* 1000 jobtimeout))]
                 (let [[v ch] (async/alts!! [c t])]
                   (or v "timeout"))))}
            )))

(defn -main [& args]
  (let [parsed  (parse-opts args cli-options)
        errs (:errors parsed)
        opts (:options parsed)
        opts (merge (dissoc opts :opts) (:opts opts))]
    (if (:help opts)
      (println opts)
      (do
        (timbre/set-level! (:log opts))
        (timbre/set-config! [:standard-out :error?] true)
        (girder.grid.redis/init! (:host opts) (:port opts))
        (println (pr-str (doit opts)))
        (Thread/sleep (* 1000 (:hang opts)))
        (System/exit 0)))))
