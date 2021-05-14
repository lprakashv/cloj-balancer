(ns cloj-balancer.lb-server
  (:require [clojure.core.async :as async]
            [clojure.string :as s]
            [cloj-balancer.mgmt-server :as mgmt]
            [cloj-balancer.state :as state])
  (:import [java
            net.Socket
            net.ServerSocket
            net.InetAddress
            io.BufferedInputStream
            io.BufferedOutputStream
            nio.ByteBuffer])
  (:gen-class))


(defn start-lb-server!
  [^ServerSocket server-socket
   ^Integer buffer-size
   algorithm]
  (swap! state/server-running #(or % true))
  (let [arr    (byte-array buffer-size)]
    (println
     (str "Started LB server at port = " (. server-socket getLocalPort) " with policy = " algorithm))
    (loop [continue? (and @state/server-running (not (empty? @mgmt/backend-addrs)))]
      (when continue?
        (try
          (println "Trying to open resources...")
          (with-open [client-conn       ^Socket (. server-socket accept)
                      backend-conn      ^Socket (let [backend-addr (mgmt/get-backend-addr algorithm)
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
      (recur (and @state/server-running (not (empty? @mgmt/backend-addrs)))))))