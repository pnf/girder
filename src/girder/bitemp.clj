(ns girder.bitemp
  (:use [datomic.api :only [q db] :as d]
        [clojure.pprint])
  (:require  digest
             [clj-time.core :as ct]
             [clj-time.coerce :as co]
             ;[clj-time.format :as cf]
             ))

#_(def uri "datomic:free://localhost:4334/bitemp")
(def uri "datomic:ddb://us-east-1/your-system-name/bitemp")

#_(def conn (d/connect uri))

(def schema-tx 
    [{:db/id #db/id[:db.part/db]
      :db/ident :bitemp/k
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "Non-temporal part of the key"
      :db.install/_attribute :db.part/db}

     {:db/id #db/id[:db.part/db]
     :db/ident :bitemp/tv
     :db/valueType :db.type/instant
     :db/cardinality :db.cardinality/one
     :db/doc "Time for which data is relevant"
     :db/index true
     :db.install/_attribute :db.part/db}

     {:db/id #db/id[:db.part/db]
      :db/ident :bitemp/index
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "concatenated k and tv"
      :db/unique :db.unique/identity
      :db.install/_attribute :db.part/db}

     {:db/id #db/id[:db.part/db]
      :db/ident :bitemp/value
      :db/valueType :db.type/string
      :db/cardinality :db.cardinality/one
      :db/doc "The value"
      :db.install/_attribute :db.part/db}
     ]
)


(defn recreate-db [uri]
  (d/delete-database uri)
  (d/create-database uri)
  (let [conn (d/connect uri)]
    @(d/sync conn)
    @(d/transact conn schema-tx)
    @(d/transact conn [{:db/id (d/tempid :db.part/db)
                        :db/ident :bitemp 
                        :db.install/_partition :db.part/db}])
    conn))

(defn connect [uri]
  (let [conn (d/connect uri)]
    @(d/sync conn)
    conn))

(defn k-hash [k] (digest/md5 k))
(defn long->str [l] 
  (let [li (- 100000000000000 l)]
    (apply str (map #(->> % (bit-shift-right li) (bit-and 0xFF) char) [56 48 40 32 24 16 8 0]))))
(defn t-format [t] (-> t .getTime long->str))
(def idx-sep "-")
(defn idxid [k tv] (str (k-hash k) idx-sep (t-format tv)))

(defn  jd 
  ([tv]
     (condp = (type tv)
       java.lang.Long (java.util.Date. tv)
       java.lang.Integer (java.util.Date. (long tv))
       java.util.Date tv
       java.lang.String (-> tv co/from-string co/to-date)))
  ([] (java.util.Date.)))



(defn insert-tx [k tv value] 
  (let [tv (jd tv)
      idx (idxid k tv)]
    {:db/id (d/tempid :bitemp) ;#db/id[:user.part/users]
                          :bitemp/index idx
                          :bitemp/k k
                          :bitemp/tv tv
                          :bitemp/value value}))

; implicit upsert
(defn insert-value [conn k tv value] 
  "Insert a value at tv and return time of insert, and return the transaction time"
  (-> @(d/transact conn [(insert-tx k tv value)])
      :tx-data first  .v))

(defn insert-values [conn k-tv-values] 
  "Insert multiple values at [[k1 tv1 v1] [k2 tv2 v2] ...] and return the transaction time"
  (-> @(d/transact conn (map (partial apply insert-tx) k-tv-values))
      :tx-data first  .v))


(defn print-hist
  ([conn]
     (doseq [t  (sort (q '[:find ?when :where [_ :db/txInstant ?when]] (db conn)))]
       (let [tx (first t)
             r  (q '[:find ?e ?k ?tv ?i ?v :where [?e :bitemp/k ?k] [?e :bitemp/tv ?tv] [?e :bitemp/index ?i] [?e :bitemp/value ?v] ] (-> conn db (d/as-of tx)))
             ]
         (println t (count  r) r))))
  ([conn k tv]
     (let [a    (:id (d/attribute (db conn) :bitemp/value))
           _    (println a)
           idx  (idxid k tv)
           id   (ffirst (q `[:find ?id :where
                           [?id :bitemp/index ~idx]] (db conn)))
           txs  (sort-by first (q `[:find ?t ?tx ?v
                       :where [~id ~a ?v ?tx true]
                              [?tx :db/txInstant ?t]]
                     (d/history (db conn))))]
       (doseq [tx txs] (println tx)))))


(defn get-at [conn k tv & tt]
  (let [tvf  (t-format (jd tv))
        kh  (k-hash k)
        idx (str kh idx-sep tvf)
        db  (if tt (-> conn db (d/as-of (first  tt))) (db conn))
        es  (some-> (d/index-range db :bitemp/index idx nil)
                    seq first .v)]
    (or  (and es
              (.startsWith es kh)
              (first (q '[:find ?tv ?v
                          :in $ ?i ?k
                          :where
                          [?e :bitemp/index ?i]
                          [?e :bitemp/k   ?k]
                          [?e :bitemp/value ?v]
                          [?e :bitemp/tv     ?tv]
                          ]
                        db es k)))
         nil)))

(defn dbtime [conn] 
  (let [db (db conn)]
      (-> (d/entity db (d/t->tx (d/basis-t db))) :db/txInstant)))

(defn test-val [k i j] (str "k" k "v" i "r" j))

(defn insert-lots [conn k0 nKeys nTv nTt]
  "Insert lots of stuff.  nKeys unique keys, nTv tv's, nTt nt's in batches of nKeys.
Returns a list of transaction times"
  (let [tts (for [i (range nTt)]
              (last  (for [j   (range nTv)]
                       (do  (insert-values conn
                                           (for [k (range k0 (+ k0  nKeys))]
                                             (let [k  (str "Thing" k)
                                                   tv (jd (* 10  j))
                                                   v  (str k "v" j "t" i)]
                                               ;(println "Inserting" k "(" tv ") = " v)
                                               [k tv v]))))))) ]
    (doall tts)                              ;[(first txs) (last txs)]
    ))

(defn query-lots [conn k0 nKeys nTv tts n]
  (let [tts (vec tts)
        nTt  (count tts)]
    (dotimes [_ n]
      (let [i  (rand-int nTt)
            tt (get tts i)
            j  (rand-int nTv)
            tv (jd  (* 10 j))
            k  (str "Thing" (+ k0  (rand-int nKeys)))
            v  (second (get-at conn k tv tt))
            ve (str k "v" j "t" i)
            msg (str "Querying " k "(" tv "," (pr-str tt) ") = " v "=?=" ve)]
        (when (not= v ve) (throw (Exception. msg)))
        ))))



