(ns cloj-balancer.state)

(defonce sys-props (System/getProperties))
(defonce server-running (atom false))
(defonce read-buf-size (* 16 1024))