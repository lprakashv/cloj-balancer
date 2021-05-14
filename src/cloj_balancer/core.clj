(ns cloj-balancer.core
  (:require [clojure.core.async :as async]
            [clojure.tools.cli :refer [parse-opts]]
            [cloj-balancer.utils :as utils]
            [cloj-balancer.cli :as cli]
            [cloj-balancer.mgmt-server :as mgmt]
            [cloj-balancer.state :as state]
            [cloj-balancer.lb-server :as lb])
  (:import [java net.ServerSocket net.InetAddress])
  (:gen-class))


(defn -main [& args]
  (let [{:keys [options exit-message ok?]} (cli/validate-args args)]
    (if exit-message
      (utils/exit (if ok? 0 1) exit-message)
      (let [lb-port                                      (:lb-port options)
            lb-host                                      (:lb-host options)
            algo                                         (:algo options)
            lb-server-socket                             (ServerSocket. lb-port 100 (InetAddress/getByName lb-host))
            ms-port                                      (:ms-port options)
            ms-host                                      (:ms-host options)
            ms-socket                                    (ServerSocket. ms-port 100 (InetAddress/getByName ms-host))
            lb-server-channel                            (async/go (lb/start-lb-server! lb-server-socket state/read-buf-size algo))
            ms-channel                                   (async/go (mgmt/start-ms! ms-socket state/read-buf-size))]
        (. (Runtime/getRuntime) addShutdownHook
          (Thread.
            ^Runnable #(do (swap! state/server-running not)
                        (println "Quitting...")
                        (Thread/sleep 1000)
                        (println "Okay Bye!"))))
        (async/alt!! ms-channel lb-server-channel)))))
