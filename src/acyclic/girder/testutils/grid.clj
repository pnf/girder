(ns acyclic.girder.testutils.grid
  (:require [acyclic.utils.cli :as cli]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [acyclic.girder.grid :as grid]
            [taoensso.timbre :as timbre]
            acyclic.girder.grid.redis)
  (:gen-class))
(timbre/refer-timbre)

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
   [nil "--numrecjobs N" "Number of jobs to launch at each level of recursion"
    :parse-fn #(Integer/parseInt %) :default 2]
   [nil "--cleanup"]
   ["-i" "--id NUM" "Some id number" :default 0 :parse-fn #(Integer/parseInt %)]
   [nil "--log LEVEL" "Debug level" :parse-fn keyword :default nil]
   ["-o" "--opts OPTS" "EDN string" :default nil :parse-fn read-string]
   [nil "--repl" "Set when running in REPL, so exit isn't called"]
   [nil "--cmt COMMENT" "Some comment to stick into job request ids" :default (str (rand))]])

(grid/cdefn recbog [msec jobnum reclevel numrecjobs args]
       (debug "here we are in recbog" grid/*nodeid* msec jobnum reclevel numrecjobs args)
       (if-not (pos? reclevel)
         (let [res (str jobnum)]
           (Thread/sleep msec)
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           res)
         (let [reqs (map #(vector recbog msec % (dec reclevel) numrecjobs args) (range numrecjobs))
               vs  (grid/requests reqs)
               res (str "RecBog:" grid/*nodeid* ":" jobnum ":" msec ":" reclevel ":" args ":[" (clojure.string/join "," (map str vs)) "]")]
           (debug "recbog" msec jobnum reclevel numrecjobs args "returning" res)
           (Thread/sleep msec)
           res
    )))

(defn doit [opts]
  (acyclic.girder.grid.redis/init! (:host opts) (:port opts))
  (let [{:keys [numrecjobs reclevel cmt help pool worker distributor host port jobmsecs jobs jobtimeout helper cleanup]} opts]
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
       (let [c (async/map vector (map #(grid/enqueue pool [recbog jobmsecs % reclevel numrecjobs cmt]) (range jobs)))
             t (async/timeout (* 1000 jobtimeout))]
         (let [[v ch] (async/alts!! [c t])]
           (or v "timeout"))))}
    ))



(defn -main [& args] (cli/edn-app args cli-options doit))
