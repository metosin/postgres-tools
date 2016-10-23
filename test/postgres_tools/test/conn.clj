(ns postgres-tools.test.conn
  (:require
    [mount.core :refer [defstate]]
    [hikari-cp.core :as hikari]
    [clojure.edn :as edn]
    [clojure.java.io :as io]))

(defn read-res-edn [s] (-> s (io/resource) slurp edn/read-string))

(defstate config
  :start (read-res-edn "test-config.edn"))

(defstate db
  :start (hikari/make-datasource (:pooled-db config))
  :stop (hikari/close-datasource db))
