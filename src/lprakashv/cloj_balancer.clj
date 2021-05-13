(ns lprakashv.cloj-balancer
  (:require [clojure.java.io :as io]
            [clojure.core.reducers :as r]
            [clojure.core.async :as async]
            [clojure.core.async.impl.channels :as cimpl]
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
(defonce backend-addrs (atom #{}))
(defonce server-running (atom false))

(defn add-backend-server! [^String addr]
  ; very basic host:port regex match to validate the address
  (cond
    (empty? (re-find #".+:[0-9]+" addr))   (do
                                             (println "Invalid address passed : " addr)
                                             false)
    (not (. @backend-addrs contains addr)) (do
                                             (swap! backend-addrs #(cons addr %))
                                             (println "Added address = " addr " to backends for load balancing")
                                             true)
    :else                                  (do
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
  (let [policy (if (contains? policies (keyword policy-type))
                 (keyword policy-type)
                 (throw (RuntimeException. "Invalid policy type passed : " policy-type)))
        arr    (byte-array buffer-size)]
    (println
     (str "Started LB server at port = " (. server-socket getLocalPort) " with policy = " policy))
    (loop []
      (when (and @server-running (not (empty? @backend-addrs)))
        (try
          (println "Trying to open resources...")
          (with-open [client-conn       ^Socket (. server-socket accept)
                      backend-conn      ^Socket (let [[host port & _]
                                                      (s/split (get-backend-addr policy) #":")]
                                        (Socket. host (Integer/parseInt port)))
                      input-stream      ^BufferedInputStream (BufferedInputStream. (. client-conn getInputStream))
                      output-stream     ^BufferedOutputStream (BufferedOutputStream. (. backend-conn getOutputStream))
                      rev-input-stream  ^BufferedInputStream (BufferedInputStream. (. backend-conn getInputStream))
                      rev-output-stream ^BufferedOutputStream (BufferedOutputStream. (. client-conn getOutputStream))]
            (println "Resourced opened!")

            (println "starting to read-write server->backend...")
            (loop [bytes-read
                   (. input-stream read arr 0 buffer-size)]
              (when (pos? bytes-read)
                (. output-stream write arr 0 bytes-read)
                (. output-stream flush)
                (recur
                  (if (pos? (. input-stream available))
                    (. input-stream read arr 0 buffer-size)
                    -1))))

            (println "starting to read-write backend->server...")
            (loop [bytes-read
                   (. rev-input-stream read arr 0 buffer-size)]
              (when (pos? bytes-read)
                (. rev-output-stream write arr 0 bytes-read)
                (. rev-output-stream flush)
                (recur
                  (if (pos? (. rev-input-stream available))
                    (. rev-input-stream read arr 0 buffer-size)
                    -1)))))
          (catch Throwable e
            (println (str "Exception occurred:\n" (Throwable->map e))))))
      ; keep going with the main loop
      (do
        (recur)))))

(defn ^String ok-message []
  (let [date  (Date.)
        dtfmt (.toString date)
        msg   "SUCCESS"]
    (str "HTTP/1.1 200 OK\r\n"
         "Date: " dtfmt "\r\n"
         "Content-Length: " (count msg) "\r\n"
         "\r\n"
         msg)))

(defn ^String failure-message []
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
   (str "Started Management server at port = " (. server-socket getLocalPort)))
  ;(def lock (Object.))
  (loop []
    ;(locking lock
    (try
      (with-open [client-conn   ^Socket (.accept server-socket)
                  output-stream ^BufferedOutputStream (BufferedOutputStream. (. client-conn getOutputStream))
                  input-stream  ^BufferedInputStream (BufferedInputStream. (. client-conn getInputStream))]
        (let [arr             (byte-array buffer-size)
              sb              (StringBuilder. "")
              ok-message      (ok-message)
              failure-message (failure-message)]
          (loop [bytes-read (if (pos? (. input-stream available))
                              (. input-stream read arr 0 buffer-size)
                              -1)]
            (when (pos? bytes-read)
              (let [to-append (.toString
                                (. (Charset/forName "UTF-8") decode (ByteBuffer/wrap arr 0 bytes-read)))]
                (.append sb to-append)
                (recur
                  (if (pos? (. input-stream available))
                    (. input-stream read arr 0 buffer-size)
                    -1)))))

          (let [sval     (.toString sb)
                path     ^String (->> (s/split-lines sval)
                                      (drop-while #(s/blank? %))
                                      (first)
                                      (#(if % (s/split % #"\s+") ["" ""]))
                                      (second))
                success? (cond
                           (s/starts-with? path "/mgmt/add/")    (->> (. path substring 10)
                                                                      (add-backend-server!))

                           (s/starts-with? path "/mgmt/remove/") (->> (. path substring 13)
                                                                      (remove-backend-server!))

                           :else                                 (do
                                                                   (println (str "Unable to add { path: " path " }"))
                                                                   false))]
            (if success?
              (.write output-stream (. ok-message getBytes) 0 (count ok-message))
              (.write output-stream (. failure-message getBytes) 0 (count failure-message)))))
        (. output-stream flush))
      (catch Throwable e
        (println (str "Exception occurred:\n" (Throwable->map e)))))
    ;)
    (recur)))

(defn -main
  ([fst scnd thrd]
   (let [lb-port                            (Integer/parseInt fst)
         mgmt-port                          (Integer/parseInt scnd)
         lb-server                          (ServerSocket. lb-port)
         mgmt-server                        (ServerSocket. mgmt-port)
         management-server-channel          (async/go (start-management-server mgmt-server 4096))
         lb-server-channel                  (async/go (start-lb-server-loop lb-server 4096 thrd))]

     (println "(async/alt!! management-server-channel lb-server-channel): "
              (async/alt!! management-server-channel lb-server-channel))))
  ([fst scnd] (-main fst scnd "round-robin")))
