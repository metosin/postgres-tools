(ns postgres-tools.clojure.jdbc.core
  (:require
    [postgres-tools.jdbc.impl :as impl]
    [jdbc.core :as jdbc]))


;; TODO make java.jdbc version of this

(defn- fetch-or-execute [dbspec sqlvec returning-columns options]
  (println (prn-str sqlvec))
  (with-open [conn (jdbc/connection dbspec)]
    (jdbc/atomic conn
      (if returning-columns
        (let [options (merge {:identifiers impl/identifier-from} options)]
          (jdbc/fetch conn sqlvec options))
        (jdbc/execute conn sqlvec options)))))

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

   (impl/insert*! dbspec
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

   (impl/update*! dbspec
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

   (impl/upsert*! dbspec
                  table-name
                  data
                  identity-columns
                  returning-columns
                  options
                  fetch-or-execute)))

(defn query
  ([dbspec sql-statement] (query dbspec sql-statement nil))
  ([dbspec sql-statement options]
   (let [options (merge {:identifiers impl/identifier-from} options)]
     (with-open [conn (jdbc/connection dbspec)]
       (jdbc/fetch conn sql-statement options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
   (with-open [conn (jdbc/connection dbspec)]
     (jdbc/atomic conn
       (doseq [s sql-statements]
         (jdbc/execute conn s options))))))
