(ns girder.tests
  (:require [ clojure.core.async :as async 
             :refer [<! >! <!! >!! timeout chan alt!! go close! take!]])
  (:use [girder.aws-script]))


(def n 2)
(def nKeys 100)
(def nTv 10)
(def nTt 10)
(def runjar ["java" "-jar" "girder.jar" "--opts"])
(def insert-opts {:command "insert-lots" :nTt nTt :nTv nTv :nKeys nKeys}) ; :k0 :id
(def query-opts {:command "query-lots" :nTv nTv :nKeys nKeys :num 10000});  :k0 :Tts

;(def hosts ["ec2-54-204-184-134.compute-1.amazonaws.com" "ec2-com-50-16-77-47.compute-1.amazonaws.com" "ec2-50-19-35-58.compute-1.amazonaws.com" "ec2-54-242-2-125.compute-1.amazonaws.com" "ec2-54-211-168-144.compute-1.amazonaws.com"])

(defn test1 [sessions]
                    (let [n      (count sessions)
                          k0s    (map #(* nKeys %) (range n))
                          cmds   (map #(conj runjar (assoc insert-opts :k0 %1 :id %2)) k0s (range n))
                          cs     (map ex-async sessions cmds)
                          c      (async/into [] (async/merge cs))]
                      c)) ; channel contains single sequence of ssh output maps

(defn exs->tss [exs] (map :result (sort-by :id (map (comp read-string :out) exs))) )


(defn test2 [sessions tss]
                    (let [n      (count sessions)
                          k0s    (map #(* nKeys %) (range n))
                          cmds   (map #(conj runjar (assoc query-opts :k0 %1 :Tts %2 :id %3)) k0s tss (range n))
                          cs     (map ex-async sessions cmds)
                          c      (async/into [] (async/merge cs))]
                      c))



(defn download-jar [sessions]
  (async/into []  (async/merge (map #(ex-async % "aws s3 cp s3://dist-ec2/girder.jar .") sessions))))

                                        ;(defn block)



                  ;; (comment

                  ;; (def out (ex (first sessions) ["java" "-jar" "girder.jar" "--opts" {:command "insert-lots" :nTt 10 :nTv 10 :nKeys 10}]))

                  ;; (def out (ex (first sessions) ["java" "-jar" "girder.jar" "--opts" {:command "insert-lots" :nTt 10 :nTv 10 :nKeys 10 :uri girder.bitemp/uri}]))


                  ;; (def ts (:result (read-string (:out out))))




                  ;; (def rs make-requests)
                  ;; (request-status rs)
                  ;; (def is request-instances rs)
                  ;; (def hosts dns-names is)
                  ;; (def sessions ssh-sessions hosts)


                  ;; (def c (chan))



                  ;; (doseq [[i s] (map vector (range n) sessions)]
                  ;;   (ex-async s ["java" "-jar" "girder.jar" "--opts" (assoc insert-opts :k0 (* nKeys i))] c)))


                  ;; ;(map disconnect ss)
                  ;; ;(describe-spot-instance-requests :spot-instance-request-ids r)
                  ;; ; (filter #(= "sir-b7586249" (:spot-instance-request-id %)) (get-in d [:spot-instance-requests]))

