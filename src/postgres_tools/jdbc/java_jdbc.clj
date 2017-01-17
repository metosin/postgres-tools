(ns postgres-tools.jdbc.java-jdbc
  (:require
    [postgres-tools.jdbc.impl :as impl]
    [clojure.java.jdbc :as jdbc])
  (:import (javax.sql DataSource)))

(defprotocol Connection
  "Handle dbspec -> connection wrapping"
  (connection [x]))

(extend-protocol Connection
  DataSource
  (connection [ds] {:datasource ds})
  Object
  (connection [dbspec] dbspec))

;;
;; Java jdbc version
;;

(defn- execute-sql [dbspec sqlvec returns-data? options]
  (println (prn-str sqlvec))
  (jdbc/with-db-transaction [conn (connection dbspec)]
    (if returns-data?
      (jdbc/query conn sqlvec (impl/default-result-options options))
      (first (jdbc/execute! conn sqlvec options)))))



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
   (jdbc/query (connection dbspec)
               sql-statement
               (impl/default-result-options options))))

(defn execute!
  ([dbspec sql-statements]
   (execute! dbspec sql-statements nil))
  ([dbspec sql-statements _]
   (jdbc/with-db-transaction [conn (connection dbspec)]
     (doseq [s sql-statements]
       (jdbc/execute! conn s)))))
