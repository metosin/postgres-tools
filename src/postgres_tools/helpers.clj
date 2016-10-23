(ns postgres-tools.helpers)

(ns db.helpers
  (:require
    [honeysql.core :as sql]
    [cheshire.core :as json]
    [honeysql.format :as fmt]
    [jdbc.proto :as proto])
  (:import (org.postgresql.util PGobject)
           (clojure.lang IPersistentMap Keyword)
           (java.sql PreparedStatement)))

(defn ->PGobject [type value]
  (doto (PGobject.)
    (.setType type)
    (.setValue value)))

;;Que, toimiiko tää, eli
;; jollain defpgtype makrolla
;; luotas näitä ja sit nää hookkais
;; ISQLType ja jdbc.proto/ISQLResultSetReadColumn
(defprotocol IPostgresType
  (to-pgobject [_])
  (from-pbobject [_ val]))

(defrecord MyType [type val]
  IPostgresType
  (to-pgobject [_] (->PGobject type val))
  (from-pbobject [_ val] (println "from " val)))


;; extending 'real' types
;; and postgres types
(extend-protocol proto/ISQLType
  MyType
  (as-sql-type [this _]
    (to-pgobject this))
  (set-stmt-parameter! [this conn ^PreparedStatement stmt ^Long index]
    (.setObject stmt index (proto/as-sql-type this conn)))

  IPersistentMap
  (as-sql-type [this _]
    (->PGobject "jsonb" (json/generate-string this)))
  (set-stmt-parameter! [this conn ^PreparedStatement stmt ^Long index]
    (.setObject stmt index (proto/as-sql-type this conn))))


;;
;; For making queries
;;

(defn enum [e]
  "`e` is namespaced keyword, where namespace will
  be the type of enum"
  (->PGobject (namespace e) (name e)))

(defn jsonb
  "Turns `v` to Postgresql jsonb value"
  [v]
  (sql/raw (str "'" (json/generate-string v) "'::jsonb")))

(defn tsquery
  "Turns `v` to Postgresql tsquery value"
  [v]
  (sql/raw (str "'" v "'::tsquery")))


;; Adds Postgres handler for jsonb containment '@>' operator.
;; Example usage: '{:where [:contains? :data (jsonb {:tags [1 2]})]}'
(defmethod fmt/fn-handler "contains?"
  [_ left right]
  (str (fmt/to-sql left) " @> " (fmt/to-sql right)))

(defmethod fmt/fn-handler "=fulltext"
  [_ left right]
  (str (fmt/to-sql left) " @@ " (fmt/to-sql right)))

;;
;; Loading data from database
;; TODO This will need improving
;;

(defn from-enum [e]
  (if (instance? PGobject e)
    (keyword (.getValue ^PGobject e))
    ;; TODO This should not be needed, but sometimes
    ;; enum values are strings, not PGobjects
    ;; Would be nice to know why this can happen
    (keyword e)))
