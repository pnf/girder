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

(def cli-options
  [["-c" "--command CMD" "Command"
    :default nil
    :parse-fn str
    :validate [#(contains? commands %) (str "Must be one of " commands)]
    ]
   ["-ntt" "--nTt NUM" "Number of transaction times"
    :parse-fn #(Integer/parseInt %)]
   ["-ntv" "--nTv NUM" "Number of value times"
    :parse-fn #(Integer/parseInt %)]
   ["-nk" "--nKeys NUM" "Number of transactions keys"
    :parse-fn #(Integer/parseInt %)]
])

(defn -main [& args]
  (let [opts (parse-opts args cli-options)]
    (println opts)
    (System/exit 0)))
