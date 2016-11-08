(ns postgres-tools.jdbc.clojure-jdbc
  (:require
    [postgres-tools.jdbc.impl :as impl]
    [jdbc.core :as jdbc-core]))


;; Funcool clojure.jdbc version of insert, update and upsert

(defn- execute-sql [dbspec sqlvec returning-columns options]
  (println (prn-str sqlvec))
  (with-open [conn (jdbc-core/connection dbspec)]
    (jdbc-core/atomic conn
      (if returning-columns
        (let [options (merge {:identifiers impl/identifier-from} options)]
          (jdbc-core/fetch conn sqlvec options))
        (jdbc-core/execute conn sqlvec options)))))

(alter-var-root #'impl/*execute-sql-fn* (constantly execute-sql))

;;
;; Public api
;;

(def insert! impl/insert!)
(def update! impl/update!)
(def upsert! impl/upsert!)

(defn query
  ([dbspec sql-statement] (query dbspec sql-statement nil))
  ([dbspec sql-statement options]
   (let [options (merge {:identifiers impl/identifier-from} options)]
     (with-open [conn (jdbc-core/connection dbspec)]
       (jdbc-core/fetch conn sql-statement options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
   (with-open [conn (jdbc-core/connection dbspec)]
     (jdbc-core/atomic conn
       (doseq [s sql-statements]
         (jdbc-core/execute conn s options))))))

