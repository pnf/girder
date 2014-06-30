(ns girder.gridrunner
  (:require [clojure.tools.cli :refer [parse-opts]]
            girder.redis)
  (:use girder.grid)
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
  (str jobnum ":" nsec))


(def cli-options
  [[nil "--worker WS" "Launch one or more workers, with comma-delimited names"]
   [nil "--distributor DIST" "Launch a distributor"]
   [nil "--host HOST" "Redis host" :default "localhost"]
   [nil "--port PORT" "Redis port" :default 6379 :parse-fn #(Integer/parseInt %)]
   [nil "--jobsecs SECS" "Amount of time for the job to take" :default 1]
   [nil "--jobs N" :default 0]
   ["-o" "--opts OPTS" "EDN string" :default nil]
   [nil "--repl" "Set when running in REPL, so exit isn't called"]])


(defn -main [& args]
  (let [parsed  (parse-opts args cli-options)
        errs (:errors parsed)
        opts (:options parsed)
        opts (merge (dissoc opts :opts) (:opts opts))
        {:keys [worker distributor port jobsecs jobs]} opts]
    (println worker distributor port jobsecs jobs)))
