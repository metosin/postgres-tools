(ns postgres-tools.jdbc.impl
  (:require [clojure.string :as str]))


;;
;; Implementations for common insert, update and upsert functionality
;;

(defn- underscore [x] (str/replace x #"-" "_"))
(defn- upperscore [x] (str/replace x #"_" "-"))

(defn identifier-to
  "Table and column names to Postgresql names"
  [x] (-> x name underscore))

(defn identifier-from
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

(defn insert*!
  [dbspec table-name data returning-columns options execute-fn]
  {:pre [(sequential? data) (map? (first data))]}

  (let [columns (keys (first data))
        sql     (insert-sql table-name
                            columns
                            returning-columns
                            (count data)
                            nil)
        sqlvec  (into [sql] (for [row data
                                  k   columns]
                              (get row k)))]
    (execute-fn dbspec sqlvec returning-columns options)))

(defn update*!
  [dbspec table-name data-map identity-map returning-columns options execute-fn]
  {:pre [(map? data-map) (map? identity-map)]}

  (let [sql    (update-sql table-name
                           (keys data-map)
                           (keys identity-map)
                           returning-columns)
        sqlvec (into [sql] (concat (vals data-map) (vals identity-map)))]
    (execute-fn dbspec sqlvec returning-columns options)))

(defn upsert*!
  [dbspec table-name data identity-columns returning-columns options execute-fn]
  {:pre [(sequential? data) (map? (first data))]}

  (let [columns (keys (first data))
        sql     (insert-sql table-name
                            columns
                            returning-columns
                            (count data)
                            (upsert-sql identity-columns
                                        columns))
        sqlvec  (into [sql] (for [row data
                                  k   columns]
                              (get row k)))]
    (execute-fn dbspec sqlvec returning-columns options)))