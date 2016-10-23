(ns postgres-tools.core
  (:require
    [clojure.string :as str]
    [jdbc.core :as jdbc]))

;;
;; Private functions
;;

(defn- underscore [x] (str/replace x #"-" "_"))
(defn- upperscore [x] (str/replace x #"_" "-"))

(defn- identifier-to
  "Table and column names to Postgresql names"
  [x] (-> x name underscore))

(defn- identifier-from
  "Postgresql names back to userland names"
  [x] (-> x str/lower-case upperscore))


(defn- column-eq-param [param column-name]
  (-> column-name identifier-to (str " = " param)))

(defn- insert-sql [table columns ret-columns data-rows-count upsert-sql]
  (let [values-row (str "(" (str/join ", " (repeat (count columns) "?")) ")")]
    (str "INSERT INTO " (-> table identifier-to) " ("
         (str/join ", " (map identifier-to columns))
         ") VALUES "
         (str/join ", " (repeat data-rows-count values-row))
         upsert-sql
         (when ret-columns
           (str " RETURNING "
                (str/join ", " (map identifier-to ret-columns)))))))

(defn- update-sql [table value-columns identity-columns ret-columns]
  (str "UPDATE " (-> table identifier-to) " SET "
       (str/join ", " (map (partial column-eq-param "?") value-columns))
       " WHERE "
       (str/join ", " (map (partial column-eq-param "?") identity-columns))
       (when ret-columns
         (str " RETURNING "
              (str/join ", " (map identifier-to ret-columns))))))

(defn- upsert-sql [identity-columns value-columns]
  (str " ON CONFLICT ("
       (str/join ", " (map identifier-to identity-columns))
       ") DO UPDATE SET "
       (str/join ", " (map (fn [column]
                             (column-eq-param
                               (str "EXCLUDED." (identifier-to column))
                               column))
                           value-columns))))

(defn- fetch-or-execute [dbspec sqlvec returning-columns options]
  (println (prn-str sqlvec))
  (with-open [conn (jdbc/connection dbspec)]
    (jdbc/atomic conn
      (if returning-columns
        (let [options (merge {:identifiers identifier-from} options)]
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

   (let [columns (keys (first data))
         sql (insert-sql table-name
                         columns
                         returning-columns
                         (count data)
                         nil)
         sqlvec (into [sql] (for [row data
                                  k columns]
                              (get row k)))]
     (fetch-or-execute dbspec sqlvec returning-columns options))))

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

   (let [sql (update-sql table-name
                         (keys data-map)
                         (keys identity-map)
                         returning-columns)
         sqlvec (into [sql] (concat (vals data-map) (vals identity-map)))]
     (fetch-or-execute dbspec sqlvec returning-columns options))))

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

   (let [columns (keys (first data))
         sql (insert-sql table-name
                         columns
                         returning-columns
                         (count data)
                         (upsert-sql identity-columns
                                     columns))
         sqlvec (into [sql] (for [row data
                                  k columns]
                              (get row k)))]
     (fetch-or-execute dbspec sqlvec returning-columns options))))

(defn query
  ([dbspec sql-statement] (query dbspec sql-statement nil))
  ([dbspec sql-statement options]
   (let [options (merge {:identifiers identifier-from} options)]
     (with-open [conn (jdbc/connection dbspec)]
       (jdbc/fetch conn sql-statement options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
   (with-open [conn (jdbc/connection dbspec)]
     (jdbc/atomic conn
       (doseq [s sql-statements]
         (jdbc/execute conn s))))))
