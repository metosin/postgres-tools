(ns postgres-tools.core-test
  (:require
    [clojure.test :refer :all]
    [jdbc.core :as jdbc]
    [mount.core :as mount]
    [postgres-tools.test.conn :refer [db]]
    [postgres-tools.core :refer :all]
    [juxt.iota :refer [given]]))


(use-fixtures :once (fn [tests] (mount/start) (tests) (mount/stop)))

(defn reset-db []
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

(deftest basic-insert
  (reset-db)

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

(deftest basic-update
  (reset-db)

  (given (insert! db :house [{:name "a"} {:name "a1"}])
         identity := 2)

  ;; Get just the regular updated rows
  (given (update! db :house {:name "b1"} {:id 1})
         identity := 1)

  ;; Return data from updated row
  (given (update! db :house {:name "b2"} {:id 1} [:id :name])
         first := {:id 1 :name "b2"})

  )

(deftest basic-upsert
  (reset-db)

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

;; TODO direct datatypes like map <-> jsonb
;; TODO keyword datatypes like xxx.v1 <-> pg enum type
;; TODO make it possible to insert and update

