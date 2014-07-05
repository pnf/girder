(ns girder.utils)

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
