(ns postgres-tools.jdbc-test
  (:require
    [clojure.test :refer :all]
    [mount.core :as mount]
    [postgres-tools.test.conn :refer [db]]
    [postgres-tools.jdbc.clojure-jdbc :as cj]
    [postgres-tools.jdbc.java-jdbc :as jj]
    [juxt.iota :refer [given]]))

;; Define tested methods through map so we can
;; test both libraries with same test functions

(def cjdbc {:insert! cj/insert!
            :update! cj/update!
            :upsert! cj/upsert!
            :query cj/query
            :execute! cj/execute!})

(def jjdbc {:insert! jj/insert!
            :update! jj/update!
            :upsert! jj/upsert!
            :query jj/query
            :execute! jj/execute!})

(use-fixtures :once (fn [tests]
                      (mount/start)
                      (tests)
                      (mount/stop)))

(defn reset-db [execute!]
  (println "Clearing database...")
  (execute! db
            ["DROP TABLE IF EXISTS house"
             "DROP TYPE IF EXISTS materials"
             "CREATE TYPE materials AS ENUM ('brick', 'wood')"
             (str "CREATE TABLE house ("
                  "  id SERIAL PRIMARY KEY,"
                  "  ext_house_id TEXT UNIQUE,"
                  "  name TEXT,"
                  "  material materials,"
                  "  data JSONB"
                  ")")]))

(defn basic-insert-test [{:keys [insert! execute!]}]
  (reset-db execute!)

  ;; Return is just number of rows inserted
  (given (insert! db :house [{:name "a"} {:name "a1"}])
         identity := 2)

  ;; Or return can be the actual id of the inserted row
  (given (insert! db :house [{:name "b"}] [:id])
         first := {:id 3})

  ;; Works for many rows too
  (given (insert! db :house [{:name "b1"} {:name "b2"}] [:id :name])
         set := #{{:id 4 :name "b1"} {:id 5 :name "b2"}})

  (given (insert! db :house [{:name "c"}] [:id :name])
         first :> {:name "c"}
         [first :id] :? integer?)
  )

(deftest basic-insert
  (basic-insert-test cjdbc)
  (basic-insert-test jjdbc))


(defn basic-update-test [{:keys [insert! update! execute!]}]
  (reset-db execute!)

  (given (insert! db :house [{:name "a"} {:name "a1"}])
         identity := 2)

  ;; Get just the regular updated rows
  (given (update! db :house {:name "b1"} {:id 1})
         identity := 1)

  ;; Return data from updated row
  (given (update! db :house {:name "b2"} {:id 1} [:id :name])
         first := {:id 1 :name "b2"})

  )

(deftest basic-update
  (basic-update-test cjdbc)
  (basic-update-test jjdbc))

(defn basic-upsert-test [{:keys [upsert! query execute!]}]
  (reset-db execute!)

  ;; Should be normal insert as there is no id
  (given (upsert! db :house [{:name "a"}] [:id])
         identity := 1)

  ;; Likewise with returning the new id
  (given (upsert! db :house [{:name "a"}] [:id] [:id])
         first := {:id 2})

  ;; Insert using another unique index
  (given (upsert! db :house [{:name "b" :ext-house-id "b1"}]
                  [:ext-house-id] [:id :ext-house-id :name])
         first := {:id 3 :ext-house-id "b1" :name "b"})

  ;; Update using the ext-key
  (given (upsert! db :house [{:name "b1" :ext-house-id "b1"}]
                  [:ext-house-id] [:id :ext-house-id :name])
         first := {:id 3 :ext-house-id "b1" :name "b1"})

  ;; Make sure right number of houses exist
  (given (query db "SELECT COUNT(*) AS houses FROM house")
         first := {:houses 3})

  ;; Insert and update at same time through ext-key
  (given (->> (upsert! db :house [{:name "b2" :ext-house-id "b1"}
                                  {:name "c" :ext-house-id "c1"}]
                       [:ext-house-id] [:id :ext-house-id :name])
              (sort-by :id))
         first := {:id 3 :ext-house-id "b1" :name "b2"}
         ;; Upsert seems to increase sequence count on insert tries
         ;; so id number is actually 6
         second :> {:ext-house-id "c1" :name "c"})

  ;; Count still is right
  (given (query db "SELECT COUNT(*) AS houses FROM house")
         first := {:houses 4})

  )
(deftest basic-upsert
  (basic-upsert-test cjdbc)
  (basic-upsert-test jjdbc))
