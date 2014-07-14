(ns girder.utils
  (:require
   [taoensso.timbre :as timbre]))
(timbre/refer-timbre)

(defn stack-trace [e]
  (let [sw (java.io.StringWriter.)
        pw (java.io.PrintWriter. sw)]
    (.printStackTrace e pw)
    (.toString sw)))

(defn fname
  "Extract the qualified name of a clojure function as a string."
  [f]
  (-> (str f)
      (clojure.string/replace-first "$" "/")
      (clojure.string/replace #"@\w+$" "")
      (clojure.string/replace #"_" "-")))

(def log-atom (atom []))

(defn set-logging! [level]
  (timbre/set-config! [:appenders] {:accrue
                                    {:min-level nil :enabled? true :async? false :rate-limit nil
                                     :fn (fn [log-entry]
                                           (swap! log-atom (fn [log-vec] conj log-vec log-entry)))}
                                    :stderr
                                    {:min-level nil :enabled? true :async? false :rate-limit nil
                                     :fn (fn [{:keys [error? output]}] ; Can use any appender args
                                           (binding [*out* *err*]
                                             (timbre/str-println output)))}})
  (timbre/set-level! (or level :info)))

(defn with-accrued-log [m] (assoc m :log @log-atom))
