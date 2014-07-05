(ns girder.gridrunner
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async :refer [<! >! <!! >!! timeout chan alt!! go close!]]
            [taoensso.timbre :as timbre]
            [girder.grid :as grid]
            [girder.utils]
            girder.redis)
  (:gen-class))

(defmacro timeit
  "Evaluate forms within a try/catch block, returning a map containing :result or :exception as
appropriate, and the elapsed :time in msec."
  [id & forms]
  `(let [t1#  (.getTime (java.util.Date.))
         res# (try {:result (do ~@forms)}
                   (catch Exception e# {:exception (girder.utils/stack-trace e#)}))
         t2#  (.getTime (java.util.Date.))
         dt#  (- t2# t1#)]
     (merge {:time dt# :id ~id} res#)))

(defn bogosity [msec jobnum]
  (Thread/sleep msec)
  (str "Bogosity:" grid/*nodeid* ":" jobnum ":" msec))


(def cli-options
  [[nil "--worker WS" "Launch one or more workers, with comma-delimited names"]
   [nil "--distributor DIST" "Launch a distributor"]
   [nil "--host HOST" "Redis host" :default "localhost"]
   [nil "--port PORT" "Redis port" :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--jobsecs SECS" "Amount of time for the job to take" :parse-fn #(Integer/parseInt %) :default 1]
   [nil "--pool POOLID" "Pool ID" :default nil]
   [nil "--helper MSECS" "Launch helper for the designated pool" :parse-fn #(Integer/parseInt %) :default nil]
   [nil "--jobs N" :default 0 :parse-fn #(Integer/parseInt %)]
   [nil "--jobtimeout SECS" "Time to wait." :parse-fn #(Integer/parseInt %) :default 10]
   [nil "--cleanup"]
   ["-i" "--id NUM" "Some id number" :default 0 :parse-fn #(Integer/parseInt %)]
   [nil "--log LEVEL" "Debug level" :parse-fn keyword :default :debug]
   ["-o" "--opts OPTS" "EDN string" :default nil :parse-fn read-string]
   [nil "--repl" "Set when running in REPL, so exit isn't called"]
   [nil "--hang SECS" "After launching, hang around for this period before exiting automatically."
    :parse-fn #(Integer/parseInt %) :default 0]
   ["-h" "--help"]])

(defn doit [opts]
  (let [{:keys [help id pool worker distributor host port jobsecs jobs jobtimeout log helper cleanup]} opts]
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
               (let [c (async/map vector (map #(grid/enqueue pool [bogosity jobsecs %]) (range jobs)))
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
        (girder.redis/init! (:host opts) (:port opts))
        (println (pr-str (doit opts)))
        (Thread/sleep (* 1000 (:hang opts)))
        (System/exit 0)))))
