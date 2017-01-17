(ns postgres-tools.conversion.core
  (:require [clojure.spec :as s]
            [jdbc.proto :as proto])
  (:import (clojure.lang IPersistentMap)
           (org.postgresql.util PGobject)))


;; Create a new type SpecValue which connects value with it's spec
(defrecord SpecValue [spec value])
(defn specval [spec value] (SpecValue. spec value))

;; User data specs
(s/def :house/config map?)
(s/def :house/material #{:brick :wood})
(s/def :house/doorsize #{1 2 3})

;; Conversion functions

(defn as-pg-object [type-name]
  (fn [v] (doto (PGobject.) (.setType type-name) (.setValue v))))

(defn map->pg-jsonb [m])
(defn kw->pg-enum [enum-type] (fn [x]))
(defn int->pg-enum [enum-type] (fn [x]))

(defn pg-value->map [pgobj _])
(defn pg-value->kw [pgobj _])
(defn pg-value->int [pgobj _])

;; Conversions config
;; - one direction at the time

(defn pg-type? [type-name]
  (fn [x] (= (.getType ^PGobject x) type-name)))

(def to-db-config
  ;; [value-class-slot optional-spec-slot conversion-func-slot]
  [[SpecValue :house/material (kw->pg-enum "MATERIAL")]
   [SpecValue :house/doorsize (int->pg-enum "DOORSIZE")]
   [IPersistentMap map->pg-jsonb]])

(def from-db-config
  ;; [value-class-slot checker-func-slot conversion-func-slot]
  [[PGobject (pg-type? "JSONB") pg-value->map]
   [PGobject (pg-type? "MATERIAL") pg-value->kw]
   [PGobject (pg-type? "DOORSIZE") pg-value->int]])

(def testdata
  {:config   {:a 1 :b 2}
   :material (specval :house/material :brick)
   :doorsize (specval :house/doorsize 2)})

;; THIS SHOULD WORK ON QUERY PARAMETERS TOO!!

;; And finally create what is necessary to create conversions

;; (create-to-db-conversions to-db-config)
;; (create-from-db conversions from-db-config)


;; SO all of the above will generate something like this

;; For clojure.jdbc

(extend-protocol proto/ISQLType
  SpecValue
  (as-sql-type [v]
    (case (:spec v)
      :house/material ('fn1 (:value v))
      :house/doorsize ('fn2 (:value v))
      v))
  IPersistentMap
  (as-sql-type [v] ('fn4 v)))

(extend-protocol proto/ISQLResultSetReadColumn
  PGobject
  (from-sql-type [v _ metadata _]
    (cond
      ('checkf1 v) ('fn5 v metadata)
      ('checkf2 v) ('fn6 v metadata)
      ('checkf3 v) ('fn7 v metadata)
      :default v)))

;; For java.jdbc

;; Käytä näitä...

;; Validointi...

;; Spec generointi taulujen pohjalta?

#_(defn get-tables [db]
  (with-open [conn (jdbc/get-connection db)]
    (->> (.getTables (.getMetaData conn) nil nil "%" (into-array String ["TABLE"]))
         resultset-seq
         (map (fn [{:keys [table_schem table_name remarks]}]
                {:schema table_schem
                 :name table_name
                 :description remarks})))))

#_(defn get-columns [db schema table]
  (with-open [conn (jdbc/get-connection db)]
    (->> (.getColumns (.getMetaData conn) nil schema table nil)
         resultset-seq
         (map (fn [{:keys [column_name type_name column_size is_nullable is_autoincrement]}]
                {:size column_size
                 :name column_name
                 :type type_name
                 :nillable? (= "YES" is_nullable)
                 :auto-increment? (= "YES" is_autoincrement)})))))





;; Testing spec coersion with conformer

(defn str->int [x]
  (cond
    (integer? x) x
    (string? x) (Integer/parseInt x)
    :else :clojure.spec/invalid))

(def c-integer? (s/conformer str->int))

(s/def ::a integer?)
(s/def ::a1 c-integer?)

(s/conform ::a "1")
(s/conform ::a1 "1")
(s/conform ::a1 1)




