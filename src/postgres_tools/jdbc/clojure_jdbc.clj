(ns postgres-tools.jdbc.clojure-jdbc
  (:require
    [postgres-tools.jdbc.impl :as impl]
    [jdbc.core :as jdbc]))


;; Add protocol for connection
;; connection for

;;
;; Funcool clojure.jdbc version
;;

(defn- execute-sql [dbspec sqlvec returns-data? options]
  (println (prn-str sqlvec))
  (with-open [conn (jdbc/connection dbspec)]
    (jdbc/atomic conn
      (if returns-data?
        (jdbc/fetch conn sqlvec (impl/default-result-options options))
        (jdbc/execute conn sqlvec options)))))

;;
;; Public api
;;

(defn insert!
  {:doc impl/insert-doc}
  ([dbspec table-name data]
   (impl/insert! execute-sql dbspec table-name data nil nil))
  ([dbspec table-name data returning-columns]
   (impl/insert! execute-sql dbspec table-name data returning-columns nil))
  ([dbspec table-name data returning-columns options]
   (impl/insert! execute-sql dbspec table-name data returning-columns options)))

(defn update!
  {:doc impl/update-doc}
  ([dbspec table-name data-map identity-map]
   (impl/update! execute-sql dbspec table-name data-map identity-map nil nil))
  ([dbspec table-name data-map identity-map returning-columns]
   (impl/update! execute-sql dbspec table-name data-map identity-map returning-columns nil))
  ([dbspec table-name data-map identity-map returning-columns options]
   (impl/update! execute-sql dbspec table-name data-map identity-map returning-columns options)))

(defn upsert!
  {:doc impl/update-doc}
  ([dbspec table-name data identity-columns]
   (impl/upsert! execute-sql dbspec table-name data identity-columns nil nil))
  ([dbspec table-name data identity-columns returning-columns]
   (impl/upsert! execute-sql dbspec table-name data identity-columns returning-columns nil))
   ([dbspec table-name data identity-columns returning-columns options]
     (impl/upsert! execute-sql dbspec table-name data identity-columns returning-columns options)))

(defn query
  ([dbspec sql-statement]
   (query dbspec sql-statement nil))
  ([dbspec sql-statement options]
   (with-open [conn (jdbc/connection dbspec)]
     (jdbc/fetch conn sql-statement (impl/default-result-options options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
   (with-open [conn (jdbc/connection dbspec)]
     (jdbc/atomic conn
       (doseq [s sql-statements]
         (jdbc/execute conn s options))))))

