(ns lprakashv.cloj-balancer
  (:require [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [clojure.core.async :as async]
            [clojure.string :as s]
            [clojure.core.server :as server])
  (:import [java
            net.Socket
            net.ServerSocket
            net.InetAddress
            net.SocketInputStream
            net.SocketOutputStream
            io.BufferedWriter
            io.BufferedReader
            io.InputStreamReader
            io.OutputStreamWriter
            io.DataInputStream
            io.DataOutputStream
            io.BufferedInputStream
            io.BufferedOutputStream
            util.stream.Stream
            util.stream.Collectors
            util.Date
            nio.ByteBuffer
            nio.charset.Charset])
  (:gen-class))

(defonce policies #{:round-robin :random})
(defonce backend-addrs (atom []))

(defonce server-running (atom false))
(defonce server-resource-alive (atom false))

(defonce backend-socket-backlog 100)
(defonce server-socket-backlog 100)

(defn add-backend-server! [^String addr]
  ; very basic host:port regex match to validate the address
  (cond
    (empty? (re-find #".+:[0-9]+" addr))  (do
                                            (println "Invalid address passed : " addr)
                                            false)
    (not (.contains @backend-addrs addr)) (do
                                            (swap! backend-addrs #(cons addr %))
                                            (println "Added address = " addr " to backends for load balancing")
                                            true)
    :else                                 (do
                                            (println "address already present!")
                                            false)))

(defn remove-backend-server! [addr]
  (swap! backend-addrs #(remove (partial = addr) %))
  (println "Removed backend address = " addr " from backends")
  true)

;; for round robin load balancing policy
(defonce rr-index (atom -1))

(defn get-backend-addr
  [policy]
  (case
    policy
    :round-robin
    (let [rr-idx (swap! rr-index
                        (comp #(mod % (count @backend-addrs)) inc))]
      (-> @backend-addrs (nth rr-idx)))

    :random
    (let [rand-idx (rand-int (count @backend-addrs))]
      (->> rand-idx (nth @backend-addrs)))))

(defn start-lb-server-loop
  [^ServerSocket server-socket
   ^Integer buffer-size
   ^String policy-type]
  (swap! server-running #(or % true))
  (swap! server-resource-alive #(or % true))
  (def policy
    (if (contains? policies (keyword policy-type))
      (keyword policy-type)
      (throw (.RuntimeException "Invalid policy type passed : " policy-type))))
  (println
   (str "Started LB server at port = " (.getLocalPort server-socket) " with policy = " policy))
  (loop []
    (when (and @server-running (not (empty? @backend-addrs)))
      (with-open [client-conn           ^Socket (.accept server-socket)
                  backend-conn          ^Socket (let [[host port & _]
                                                      (s/split (get-backend-addr policy) #":")]
                                        (Socket. host (Integer/parseInt port)))
                  input-stream          ^BufferedInputStream (BufferedInputStream. (.getInputStream client-conn))
                  output-stream         ^BufferedOutputStream (BufferedOutputStream. (.getOutputStream backend-conn))
                  rev-input-stream      ^BufferedInputStream (BufferedInputStream. (.getInputStream backend-conn))
                  rev-output-stream     ^BufferedOutputStream (BufferedOutputStream. (.getOutputStream client-conn))]
        (def arr (byte-array buffer-size))
        (println "strating to read-write server->backend")
        (loop [bytes-read (if (pos? (.available input-stream))
                            (.read input-stream arr 0 buffer-size)
                            -1)]
          (when (pos? bytes-read)
            (.write output-stream arr 0 bytes-read)
            (recur
              (if (pos? (.available input-stream))
                (.read input-stream arr 0 buffer-size)
                -1))))
        (.flush output-stream)
        (println "strating to read-write backend->server")
        (loop [bytes-read (if (pos? (.available rev-input-stream))
                            (.read rev-input-stream arr 0 buffer-size)
                            -1)]
          (when (pos? bytes-read)
            (.write rev-output-stream arr 0 bytes-read)
            (recur
              (if (pos? (.available rev-input-stream))
                (.read rev-input-stream arr 0 buffer-size)
                -1))))
        (.flush rev-output-stream)))
    ; keep going with the main loop
    (if @server-resource-alive
      (recur)
      (println "Shutting down the LB server loop..."))))

(defonce ^String ok-message
  (let [date  (Date.)
        dtfmt (.toString date)
        msg   "SUCCESS"]
    (str "HTTP/1.1 200 OK\r\n"
         "Date: " dtfmt "\r\n"
         "Content-Length: " (count msg) "\r\n"
         "\r\n"
         msg)))

(defonce ^String failure-message
  (let [date  (Date.)
        dtfmt (.toString date)
        msg   "FAILED"]
    (str "HTTP/1.1 400 OK\r\n"
         "Date: " dtfmt "\r\n"
         "Content-Length: " (count msg) "\r\n"
         "\r\n"
         msg)))

(defn start-management-server [^ServerSocket server-socket ^Integer buffer-size]
  (println
   (str "Started Management server at port = " (.getLocalPort server-socket)))
  (def lock (Object.))
  (loop []
    (locking lock
      (with-open [client-conn            ^Socket (.accept server-socket)
                  output-stream          ^BufferedOutputStream (BufferedOutputStream. (.getOutputStream client-conn))
                  input-stream           ^BufferedInputStream (BufferedInputStream. (.getInputStream client-conn))]
        (let [arr             (byte-array buffer-size)
              sb              (StringBuilder. "")]
          (loop [bytes-read (if (pos? (.available input-stream))
                              (.read input-stream arr 0 buffer-size)
                              -1)]
            (when (pos? bytes-read)
              (def to-append
                (.toString
                  (.decode (Charset/forName "UTF-8") (ByteBuffer/wrap arr 0 bytes-read))))
              (.append sb to-append)
              (recur
                (if (pos? (.available input-stream))
                  (.read input-stream arr 0 buffer-size)
                  -1))))
          (def sval (.toString sb))

          (def path
            (->> (s/split-lines sval)
                 (drop-while #(s/blank? %))
                 (first)
                 (#(if % (s/split % #"\s+") ["" ""]))
                 (second)))

          (def success?
            (cond
              (s/starts-with? path "/mgmt/add/")    (->> (.substring path 10)
                                                         (add-backend-server!))

              (s/starts-with? path "/mgmt/remove/") (->> (.substring path 13)
                                                         (remove-backend-server!))

              :else                                 false))

          (if success?
            (.write output-stream (.getBytes ok-message) 0 (count ok-message))
            (.write output-stream (.getBytes failure-message) 0 (count failure-message)))
          (.flush output-stream))))
    (recur)))

(defn -main
  ([fst scnd thrd]
   (let [lb-port     (Integer/parseInt fst)
         mgmt-port   (Integer/parseInt scnd)
         lb-server   (ServerSocket. lb-port)
         mgmt-server (ServerSocket. mgmt-port)]
     (async/thread (start-management-server mgmt-server 8192))
     (async/thread (start-lb-server-loop lb-server 8192 thrd))
     (while @server-running)))
  ([fst scnd] (-main fst scnd "round-robin")))
