(ns girder.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [girder.bitemp :as bt])
  (:gen-class))


(def commands #{"recreate" "insert-lots" "query-lots"})

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

(def cli-options
  [[nil "--repl" "Set when running in REPL, so exit isn't called"]
   ["-o" "--opts OPTS" "Multiple options as EDN string, overridden by other parameters on command line."
    :default nil
    :parse-fn read-string]
   ["-c" "--command CMD" "Command"
    :default nil
    :parse-fn str]
   ["-u" "--uri URI" "URI"
    :default bt/uri
    :parse-fn str]
   ["-k" "--k0 NUM" "Starting key" :default 0 :parse-fn #(Integer/parseInt %)]
   ["-K" "--nKeys NUM" "Number of transactions keys"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--nTt NUM" "Number of transaction times"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-v" "--nTv NUM" "Number of value times"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-s" "--Tts TIMES" "List of transaction times"
    :default [(java.util.Date.)]
    :parse-fn read-string
    ;; :validate  [(fn [tts]
    ;;    (and (seq tts)
    ;;         (every? #(= java.util.Date (type %)) tts)))
    ;;  "TIMES must be a sequence of clojure insts"]
    ]
   ["-q" "--num NUM" "Number of queries"
    :default 1
    :parse-fn #(Integer/parseInt %)]
   ["-i" "--id NUM" "Some id string"
    :default 0
    :parse-fn #(Integer/parseInt %)]])


(defn -main [& args]
  (let [parsed  (parse-opts args cli-options)
        errs (:errors parsed)
        opts (:options parsed)
        opts (merge (dissoc opts :opts) (:opts opts))
        uri  (:uri opts)
        res  (if errs parsed
               (timeit
                (condp = (:command opts)
                  "recreate"    (bt/recreate-db uri)
                  "insert-lots" (apply bt/insert-lots (bt/connect uri) (map opts [:k0 :nKeys :nTv :nTt]))
                  "query-lots"  (apply bt/query-lots (bt/connect uri) (map opts [:k0 :nKeys :nTv :Tts :num])))))
        res (assoc res :id (:id opts))]
    (if (:repl opts) res
        (do (println (pr-str res)) (System/exit 0)))))
