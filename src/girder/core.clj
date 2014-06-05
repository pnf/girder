(ns girder.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [girder.bitemp :as bt])
  (:gen-class)

)

#_(def cli-options
  [;; First three strings describe a short-option, long-option with optional
   ;; example argument description, and a description. All three are optional
   ;; and positional.
   ["-p" "--port PORT" "Port number"
    :default 80
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ["-H" "--hostname HOST" "Remote host"
    :default (InetAddress/getByName "localhost")
    ;; Specify a string to output in the default column in the options summary
    ;; if the default value's string representation is very ugly
    :default-desc "localhost"
    :parse-fn #(InetAddress/getByName %)]
   ;; If no required argument description is given, the option is assumed to
   ;; be a boolean option defaulting to nil
   [nil "--detach" "Detach from controlling process"]
   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    ;; If no long-option is specified, an option :id must be given
    :id :verbosity
    :default 0
    ;; Use assoc-fn to create non-idempotent options
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ["-h" "--help"]])

#_(def cli-options
  ;; An option with a required argument
  [["-p" "--port PORT" "Port number"
    :default 80
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   ;; A non-idempotent option
   ["-v" nil "Verbosity level"
    :id :verbosity
    :default 0
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ;; A boolean option defaulting to nil
   ["-h" "--help"]])

(def commands #{"recreate" "insert-lots" "query-lots"})

(defmacro timeit [& forms]
  `(let [t1#  (.getTime (java.util.Date.))
         res# (try {:result (do ~@forms)}
                   (catch Exception e# {:exception e#}))
         t2#  (.getTime (java.util.Date.))
         dt#  (- t2# t1#)]
     (merge {:time dt#} res#)))

(def cli-options
  [[nil "--repl" "When running in REPL"]
   ["-o" "--opts OPTS" "All options as EDN string"
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

(defn- extract-default [[_ l _ & args]]
  (let [s (->> l (re-matches #".*--(\w+).*") second keyword)
        d (and args (:default (apply hash-map args)))]
    [s d]))
(def defaults (into {} (map extract-default cli-options)))

(defn -main [& args]
  (let [parsed  (parse-opts args cli-options)
        errs (:errors parsed)
        opts (:options parsed)
        opts (if (:opts opts) (merge defaults (:opts opts)) opts)
        uri  (:uri opts)
        conn (bt/connect uri)
        res  (if errs parsed
               (timeit
                (condp = (:command opts)
                  "recreate"    (bt/recreate-db uri)
                  "insert-lots" (apply bt/insert-lots conn (map opts [:k0 :nKeys :nTv :nTt]))
                  "query-lots"  (apply bt/query-lots conn (map opts [:k0 :nKeys :nTv :Tts :num])))))
        res (assoc res :id (:id opts))]
    (if (:repl opts) res
        (do (println (pr-str res)) (System/exit 0)))))
