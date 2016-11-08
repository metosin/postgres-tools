(ns postgres-tools.jdbc.java-jdbc
  (:require
    [postgres-tools.jdbc.impl :as util]
    [clojure.java.jdbc :as java-jdbc]))


;;
;; Java jdbc version of insert, update and upsert
;;



(defn- fetch-or-execute [dbspec sqlvec returning-columns options]
  (println (prn-str sqlvec))
    (java-jdbc/db-do-prepared dbspec transaction? sqlvec {:multi? true})

#_(with-open [conn (jdbc-core/connection dbspec)]
    (jdbc-core/atomic conn
      (if returning-columns
        (let [options (merge {:identifiers util/identifier-from} options)]
          (jdbc-core/fetch conn sqlvec options))
        (jdbc-core/execute conn sqlvec options)))))

;;
;; Public api
;;

(defn insert!
  "Jdbc insert. Data is list of maps. Returned columns
  can be provided as a vector of column names."
  ([dbspec table-name data]
   (insert! dbspec table-name data nil nil))
  ([dbspec table-name data returning-columns]
   (insert! dbspec table-name data returning-columns nil))
  ([dbspec table-name data returning-columns options]
   {:pre [(sequential? data) (map? (first data))]}

   (util/insert*! dbspec
                  table-name
                  data
                  returning-columns
                  options
                  fetch-or-execute)))

(defn update!
  "Jdbc update. Data is a map of new column values and
  identity is a map of column values on which row(s) should be updated.
  Returned columns can be provided as a vector of column names."
  ([dbspec table-name data-map identity-map]
   (update! dbspec table-name data-map identity-map nil nil))
  ([dbspec table-name data-map identity-map returning-columns]
   (update! dbspec table-name data-map identity-map returning-columns nil))
  ([dbspec table-name data-map identity-map returning-columns options]
   {:pre [(map? data-map) (map? identity-map)]}

   (util/update*! dbspec
                  table-name
                  data-map
                  identity-map
                  returning-columns
                  options
                  fetch-or-execute)))

(defn upsert!
  "Inserts using Postgresql insert with upsert semantics.
  Identity lists the columns to check against the existences of the row.
  Identity columns need to have unique index on them.
  Data rows get either updated or inserted based on identity search.
  Returned columns can be provided as a vector of column names."
  ([dbspec table-name data identity-columns]
   (upsert! dbspec table-name data identity-columns nil nil))
  ([dbspec table-name data identity-columns returning-columns]
   (upsert! dbspec table-name data identity-columns returning-columns nil))
  ([dbspec table-name data identity-columns returning-columns options]
   {:pre [(sequential? data) (map? (first data))]}

   (util/upsert*! dbspec
                  table-name
                  data
                  identity-columns
                  returning-columns
                  options
                  fetch-or-execute)))

(defn query
  ([dbspec sql-statement] (query dbspec sql-statement nil))
  ([dbspec sql-statement options]
   (let [options (merge {:identifiers util/identifier-from} options)]
     (with-open [conn (jdbc-core/connection dbspec)]
       (jdbc-core/fetch conn sql-statement options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
   (with-open [conn (jdbc-core/connection dbspec)]
     (jdbc-core/atomic conn
       (doseq [s sql-statements]
         (jdbc-core/execute conn s options))))))
