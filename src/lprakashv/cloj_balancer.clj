(ns lprakashv.cloj-balancer
  (:require [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [clojure.core.async :as async]
            [clojure.string :as s]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.data.json :as json])
  (:import [java
            net.Socket
            net.ServerSocket
            net.InetAddress
            io.BufferedInputStream
            io.BufferedOutputStream
            util.Date
            nio.ByteBuffer
            nio.charset.Charset])
  (:gen-class))

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

(defn- xor [x y]
  (not (identical? x y)))

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

(defn exit [status msg]
  (println msg)
  (System/exit status))

;; === global variables and states:

(defonce http-statuses
  {:ok          [200 "OK"]
   :bad-request [400 "Bad Request"]
   :not-found   [404, "Not Found"]
   :error       [500 "Internal Server Error"]})

(defonce props (System/getProperties))
(defonce backend-addrs (atom #{}))
(defonce server-running (atom false))
(defonce read-buf-size (* 16 1024))

;; for round robin load balancing policy
(defonce rr-index (atom -1))

;; === private methods:

(defn- add-backend-server! [addr]
  ; very basic host:port regex match to validate the address
  (if
    (not (. @backend-addrs contains addr))
    (do
      (swap! backend-addrs #(cons addr %))
      (println "Added address = " addr " to backends for load balancing")
      true)
    (do
      (println "address already present!")
      false)))

(defn- remove-backend-server! [addr]
  (swap! backend-addrs #(remove (partial = addr) %))
  (println "Removed backend address = " addr " from backends")
  true)

(defn- get-backend-addr
  [lb-algorithm]
  (case
    (keyword lb-algorithm)
    :round-robin
    (let [rr-idx (swap! rr-index
                        (comp #(mod % (count @backend-addrs)) inc))]
      (-> @backend-addrs (nth rr-idx)))

    :random
    (let [rand-idx (rand-int (count @backend-addrs))]
      (->> rand-idx (nth @backend-addrs)))))

;; main functions:

(defn start-lb-server!
  [^ServerSocket server-socket
   ^Integer buffer-size
   algorithm]
  (swap! server-running #(or % true))
  (let [arr    (byte-array buffer-size)]
    (println
     (str "Started LB server at port = " (. server-socket getLocalPort) " with policy = " algorithm))
    (loop [continue? (and @server-running (not (empty? @backend-addrs)))]
      (when continue?
        (try
          (println "Trying to open resources...")
          (with-open [client-conn       ^Socket (. server-socket accept)
                      backend-conn      ^Socket (let [backend-addr (get-backend-addr algorithm)
                                                      host         (:host backend-addr)
                                                      port         (:port backend-addr)]
                                        (Socket. host port))
                      input-stream      ^BufferedInputStream (BufferedInputStream. (. client-conn getInputStream))
                      output-stream     ^BufferedOutputStream (BufferedOutputStream. (. backend-conn getOutputStream))
                      rev-input-stream  ^BufferedInputStream (BufferedInputStream. (. backend-conn getInputStream))
                      rev-output-stream ^BufferedOutputStream (BufferedOutputStream. (. client-conn getOutputStream))]
            (println "Resourced opened!")

            (println "starting to read-write server->backend...")
            (loop [bytes-read
                   (if (pos? (.available input-stream)) (. input-stream read arr) -1)]
              (when (pos? bytes-read)
                (. output-stream write arr 0 bytes-read)
                (. output-stream flush)
                (recur
                  (if (pos? (.available input-stream)) (. input-stream read arr) -1))))

            (println "starting to read-write backend->server...")
            (loop [bytes-read
                   (if (pos? (.available rev-input-stream)) (. rev-input-stream read arr) -1)]
              (when (pos? bytes-read)
                (. rev-output-stream write arr 0 bytes-read)
                (. rev-output-stream flush)
                (recur
                  (if (pos? (.available rev-input-stream)) (. rev-input-stream read arr) -1)))))
          (catch Throwable e
            (println (str "Exception occurred:\n" (Throwable->map e))))))
      ; keep going with the main loop
      (recur (and @server-running (not (empty? @backend-addrs)))))))

;; management API HTTP responses

(defn common-message [status]
  (let [date                 (Date.)
        dtfmt                (.toString date)
        [status status-name] (status http-statuses)]
    [(str "HTTP/1.1 " status " " status-name)
     (str "Date: " dtfmt)
     "Content-Type: application/json; charset=UTF-8"
     (str "Server: " "Java/Cloj-Balancer")]))

(defn ^String ok-message [msg]
  (->>
   (concat (common-message :ok)
           [(str "Content-Length: " (count msg))
            ""
            msg])
   (s/join "\r\n")))

(defn ^String failure-message [msg]
  (->>
   (concat (common-message :bad-request)
           [(str "Content-Length: " (count msg))
            ""
            (str msg)])
   (s/join "\r\n")))

(defn ^String notfound-message [msg]
  (->>
   (concat (common-message :not-found)
           [(str "Content-Length: " (count msg))
            ""
            (str msg)])
   (s/join "\r\n")))

(defn start-ms! [^ServerSocket server-socket ^Integer buffer-size]
  (println
   (str "Started Management server at port = " (. server-socket getLocalPort)))
  (loop [continue? @server-running]
    (when continue?
      (try
        (with-open [client-conn   ^Socket (.accept server-socket)
                    output-stream ^BufferedOutputStream (BufferedOutputStream. (. client-conn getOutputStream))
                    input-stream  ^BufferedInputStream (BufferedInputStream. (. client-conn getInputStream))]
          (let [arr             (byte-array buffer-size)
                sb              (StringBuilder. "")]
            (loop [bytes-read (if (pos? (.available input-stream)) (. input-stream read arr) -1)]
              (when (pos? bytes-read)
                (let [to-append (.toString
                                  (. (Charset/forName "UTF-8") decode (ByteBuffer/wrap arr 0 bytes-read)))]
                  (.append sb to-append)
                  (recur
                    (if (pos? (.available input-stream)) (. input-stream read arr) -1)))))

            (let [sval     (.toString sb)
                  path     ^String (->> (s/split-lines sval)
                                        (filter #(s/starts-with? % "GET"))
                                        (first)
                                        (#(if % (s/split % #"\s+") ["" ""]))
                                        (second))]
              (cond
                (s/starts-with? path "/mgmt/add/")
                (let [addr     (s/split (. path substring 10) #":")
                      host     (first addr)
                      port     (Integer/parseInt (second addr))
                      hp       {:host host :port port}
                      success? (add-backend-server! hp)
                      content  (if success?
                                 (ok-message (json/write-str {:status :SUCCESS}))
                                 (failure-message (json/write-str {:status :FAILED})))]
                  (. output-stream write (.getBytes content) 0 (count content)))

                (s/starts-with? path "/mgmt/remove/")
                (let [addr     (s/split (. path substring 13) #":")
                      host     (first addr)
                      port     (Integer/parseInt (second addr))
                      hp       {:host host :port port}
                      success? (remove-backend-server! hp)
                      content  (if success?
                                 (ok-message {json/write-str {:status :SUCCESS}})
                                 (failure-message {json/write-str {:status :FAILED}}))]
                  (. output-stream write (.getBytes content) 0 (count content)))

                (s/starts-with? path "/mgmt/list/")
                (let [content (ok-message (json/write-str {:backends @backend-addrs}))]
                  (. output-stream write (.getBytes content) 0 (count content)))

                :else
                (let [content (json/write-str {:error (str "Path: \"" path "\" is invalid")})]
                  (. output-stream write
                    (.getBytes (notfound-message content))
                    0
                    (count content)))))
            (. output-stream flush)))
        (catch Throwable e
          (println (str "Exception occurred:\n" (Throwable->map e))))))
    (recur @server-running)))

(defn -main [& args]
  (let [{:keys [options exit-message ok?]} (validate-args args)]
    (if exit-message
      (exit (if ok? 0 1) exit-message)
      (let [lb-port                                      (:lb-port options)
            lb-host                                      (:lb-host options)
            algo                                         (:algo options)
            lb-server-socket                             (ServerSocket. lb-port 100 (InetAddress/getByName lb-host))
            ms-port                                      (:ms-port options)
            ms-host                                      (:ms-host options)
            ms-socket                                    (ServerSocket. ms-port 100 (InetAddress/getByName ms-host))
            lb-server-channel                            (async/go (start-lb-server! lb-server-socket read-buf-size algo))
            ms-channel                                   (async/go (start-ms! ms-socket read-buf-size))]
        (. (Runtime/getRuntime) addShutdownHook
          (Thread.
            ^Runnable #(do (swap! server-running not)
                        (println "Quitting...")
                        (Thread/sleep 1000)
                        (println "Okay Bye!"))))
        (async/alt!! ms-channel lb-server-channel)))))
