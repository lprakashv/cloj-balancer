(ns cloj-balancer.cli
  (:require [clojure.string :as s]
            [clojure.data.json :as json]
            [clojure.tools.cli :refer [parse-opts]])
  (:import [java.net InetAddress]))

(def cli-options
  [[nil
    "--lb-port PORT"
    "Load Balancer Port number"
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   [nil
    "--ms-port PORT"
    "Management Server Port number"
    :parse-fn #(Integer/parseInt %)
    :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]]
   [nil
    "--algo ALGORITHM"
    "Load Balancing Algorithm"
    :default  "round-robin"
    :parse-fn #(if (. #{"round-robin" "random"} contains %) % nil)]
   [nil
    "--lb-host"
    "Host of the LB server"
    :default      (.getHostAddress (InetAddress/getByName "localhost"))
    :default-desc "localhost"
    :parse-fn     #(.getHostAddress (InetAddress/getByName %))]
   [nil
    "--ms-host"
    "Host of the management server"
    :default      (.getHostAddress (InetAddress/getByName "localhost"))
    :default-desc "localhost"
    :parse-fn     #(.getHostAddress (InetAddress/getByName %))]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->>
   ["Cloj-Balancer : A Load Balancer in Clojure \u00AF\\_(\u30C4)_/\u00AF"
    ""
    "Usage: java -jar cloj-balancer --lb-host HOST --lb-port PORT --ms-host HOST2 --ms-port PORT2 --algo ALGO"
    ""
    "Options:"
    options-summary
    ""
    "Algorithms:"
    "  round-robin"
    "  random"
    ""]
   (s/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (s/join \newline errors)))

(defn validate-args
  "Validate command line arguments. Either return a map indicating the program
  should exit (with a error message, and optional ok status), or a map
  indicating the action the program should take and the options provided."
  [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) ; help => exit OK with usage summary
      {:exit-message (usage summary) :ok? true}

      errors ; errors => exit with description of errors
      {:exit-message (error-msg errors)}

      ;; custom validation on arguments
      (and
       (:lb-port options)
       (:ms-port options))
      {:options options}

      :else ; failed custom validation => exit with usage summary
      {:exit-message (usage summary)})))