(ns postgres-tools.jdbc.java-jdbc
  (:require
    [postgres-tools.jdbc.impl :as impl]
    [clojure.java.jdbc :as java-jdbc]))


;;
;; Java jdbc version of insert, update and upsert
;;

;; TODO Fix this
(defn- execute-sql [dbspec sqlvec returning-columns options]
  (println (prn-str sqlvec))
  (java-jdbc/db-do-prepared dbspec transaction? sqlvec {:multi? true}))


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
     ;; TODO
     #_(with-open [conn (jdbc-core/connection dbspec)]
         (jdbc-core/fetch conn sql-statement options)))))

(defn execute!
  ([dbspec sql-statements] (execute! dbspec sql-statements nil))
  ([dbspec sql-statements options]
    ;; TODO
    #_(with-open [conn (jdbc-core/connection dbspec)]
        (jdbc-core/atomic conn
                          (doseq [s sql-statements]
                            (jdbc-core/execute conn s options))))))
