(ns cloj-balancer.mgmt-server
  (:require [cloj-balancer.state :as state]
            [cloj-balancer.utils :refer [http-statuses]]
            [clojure.data.json :as json]
            [clojure.string :as s])
  (:import [java.util Date]
           [java.io BufferedOutputStream BufferedInputStream]
           [java.nio ByteBuffer]
           [java.nio.charset Charset]
           [java.net ServerSocket Socket])
  (:gen-class))

;; for round robin load balancing policy
(defonce rr-index (atom -1))
(defonce backend-addrs (atom #{}))

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

(defn get-backend-addr
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
  (loop [continue? @state/server-running]
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
    (recur @state/server-running)))