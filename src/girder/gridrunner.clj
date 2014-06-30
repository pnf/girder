(ns girder.gridrunner
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.core.async :as async])
  (:use girder.grid)
  (:require girder.redis)
  (:gen-class))

(defmacro timeit
  "Evaluate forms within a try/catch block, returning a map containing :result or :exception as
appropriate, and the elapsed :time in msec."
  [& forms]
  `(let [t1#  (.getTime (java.util.Date.))
         res# (try {:result (do ~@forms)}
                   (catch Exception e# {:exception e#}))
         t2#  (.getTime (java.util.Date.))
         dt#  (- t2# t1#)]
     (merge {:time dt#} res#)))

(defn bogosity [nsec jobnum]
  (Thread/sleep (* nsec 1000))
  (str "Bogosity:" jobnum ":" nsec))


(def cli-options
  [[nil "--worker WS" "Launch one or more workers, with comma-delimited names"]
   [nil "--distributor DIST" "Launch a distributor"]
   [nil "--host HOST" "Redis host" :default "localhost"]
   [nil "--port PORT" "Redis port" :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--jobsecs SECS" "Amount of time for the job to take" :parse-fn #(Integer/parseInt %) :default 1]
   [nil "--pool POOLID" "Pool ID" :default nil]
   [nil "--jobs N" :default 1]
   [nil "--timeout SECS" "Time to wait." :parse-fn #(Integer/parseInt %) :default 10]
   ["-o" "--opts OPTS" "EDN string" :default nil]
   [nil "--repl" "Set when running in REPL, so exit isn't called"]])

(defn -main [& args]
  (let [parsed  (parse-opts args cli-options)
        errs (:errors parsed)
        opts (:options parsed)
        opts (merge (dissoc opts :opts) (:opts opts))
        {:keys [pool worker distributor host port jobsecs jobs timeout]} opts]
    (girder.redis/init! port host)
    (timeit 
     (when distributor
       (let [ds (clojure.string/split distributor #",")]
         (doseq [d ds]
           (launch-distributor d pool))))
     (when worker
       (let [ws (clojure.string/split worker #",")]
         (doseq [w ws]
           (launch-worker w pool))))
     (when (and pool (pos? jobs))
       (let [c (async/map vector (map #(enqueue pool [bogosity jobsecs %]) (range jobs)))
             t (async/timeout (* 1000 timeout))]
         (let [[v ch] (async/alts!! [c t])]
           (or v "timeout")))))))

