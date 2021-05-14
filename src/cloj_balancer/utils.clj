(ns cloj-balancer.utils
  (:require [clojure.string :as s])
  (:import [java.net InetAddress])
  (:gen-class))


(defonce http-statuses
  {:ok          [200 "OK"]
   :bad-request [400 "Bad Request"]
   :not-found   [404, "Not Found"]
   :error       [500 "Internal Server Error"]})

(defn- xor [x y]
  (not (identical? x y)))

(defn exit [status msg]
  (println msg)
  (System/exit status))