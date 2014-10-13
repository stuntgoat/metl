(ns metl.core
  (:require [metl.byte-tools.core :refer :all]
            [clj-commons-exec :as exec])
  (:use [clojure.string :only (join)])
  (:import (java.net ServerSocket Socket SocketException)
           (java.io BufferedInputStream)))


(defn- on-thread [f]
  (doto (new Thread f) (.start)))

(defn str-from-metl-buffered-reader [reader]
  "Given a BufferedReader containing 4 bytes containing the length
   of the following string, and the string bytes, return the string

   If the socket is broken we read a negative value from the
   reader and return nil instead of a string."
  (let [len (get-int-from-reader reader)]
    (if (> len 0)
      (get-string-from-reader reader len)
      nil)))

(defn parse-message [reader]
  "Given a BufferedReader parse bucket and message and return
   a map that includes both as well as a time stamp."
  (let [bucket (str-from-metl-buffered-reader reader)]
    (let [message (str-from-metl-buffered-reader reader)]
      (if (string? message)
        {:bucket bucket
         :message message
         :time (System/currentTimeMillis)}
        nil))))

;;;; Maps a bucket keyword to a bucket agent.
(def BUCKETS (agent {}))
(defn BUCKETS-error-handler [ag ex]
  (do
    (binding [*out* *err*]
      (prn "error in BUCKETS agent")
      (prn ex))
    (restart-agent ag @ag)))

(set-error-handler! BUCKETS BUCKETS-error-handler)

;; Holds a vector of flusher programs.
(def bucket-flusher (atom ""))

(def bucket-threshold {:count 100000
                       :size 1.6e7}) ;; 128mb

;; Used within each bucket to hold the current size and count.
(def bucket-size-keyword (keyword "__bucket_size__"))
(def bucket-count-keyword (keyword "__bucket_count__"))

(defn get-empty-bucket []
  {bucket-count-keyword 0
   bucket-size-keyword 0})

(defn value-for-bucket [bucket]
  "Accepts a string bucket name; returns the map associated with the bucket"
  ((keyword bucket) @BUCKETS))

(defn bucket-error-handler [ag ex]
  (do
    (binding [*out* *err*]
      (prn "Error in bucket agent")
      (prn ex))
    (restart-agent ag @ag)))

(defn get-or-create-bucket [bucket]
  "Get respective bucket. If a bucket
   does not exist, create one"
  (let [mkey (keyword bucket)]
    (let [acquired (mkey @BUCKETS)]
      (if (nil? acquired)
        (do
          ;; Create bucket agent
          (send BUCKETS
                (fn [state buck]
                  ;; Initialize an empty bucket.
                  (assoc state mkey (agent (get-empty-bucket)))) bucket)
          (await BUCKETS)
          (set-error-handler! (mkey @BUCKETS) bucket-error-handler)
          (mkey @BUCKETS))
        acquired))))

(defn meets-exceeds-bucket-threshold [count size]
  "Returns a true of false if the count and size exceed
   the bucket-threshold var"
  (or (>= count (:count bucket-threshold))
      (>= size (:size bucket-threshold))))

(defn bucket-data-to-lines [bucket-map]
  "Convert the key/values of a bucket to lines for piping to
   a subprocesses stdin"
  (join "\n" (map (fn [pair]
                    (str (str (second pair)) " " (name (first pair))))
                  (seq bucket-map))))

(defn flush-bucket [bucket bucket-name]
  "Pipe the key/values of the 'data' hashmap to a program
   designed to ingest the lines send as stdin."
  ;; TODO: emit metrics to statsite
  (let [flush-state (apply dissoc bucket [bucket-count-keyword bucket-size-keyword])]
    (binding [*out* *err*]
      (prn @(exec/sh [@bucket-flusher bucket-name] {:in (bucket-data-to-lines flush-state)})))
    ;; Return a new empty bucket.
    (get-empty-bucket)))

(defn update-bucket-with-message [bucket key message]
  (let [updated (assoc bucket key (:time message))]
    (merge-with + updated {bucket-count-keyword 1
                           ;; Adding 8 for the Java long that is the Unix timestamp.
                           bucket-size-keyword (+ 8 (count message))})))

(defn should-flush [bucket-state]
  (meets-exceeds-bucket-threshold (bucket-count-keyword bucket-state)
                                  (bucket-size-keyword bucket-state)))

(defn update-bucket [state message bucket-name]
  "Update the bucket and check the `bucket-threshold` var
   for the size or count of the bucket.

   If the threshold is met or exceeded, call `flush-bucket`"
  (let [key_message (keyword (:message message))]
    (if (contains? state key_message)
      state
      ;; update the size, count and message
      (let [new-state (update-bucket-with-message state key_message message)]
        (if (should-flush new-state)
          ;; If we're flushing, flush and return the state afterwards.
          (flush-bucket new-state bucket-name)
          ;; No need to flush, so return the new state.
          new-state)))))

(defn update-bucket-with-flush [blob]
  "get or create a bucket agent; then set the key/value
   in that bucket agent."
  (let [bucket-name (:bucket blob)]
    (let [bucket (get-or-create-bucket bucket-name)]
      (send bucket update-bucket blob bucket-name))))

(defn collector [in]
  "Reads messages from `in`, an InputStream, and updates the message for the bucket
   name."
  (loop [reader (new BufferedInputStream in)]
    (let [message (parse-message reader)]
      (if (nil? message)
        nil
        (do
          (update-bucket-with-flush message)
          (recur reader))))))

(defn- socket-collector [sock]
  (on-thread #(collector (. sock (getInputStream)))))


(defn create-server-multi
  "Creates and returns a server socket on port; pass the client
   socket to accept-func in a new thread on new connections"
  [accept-func port]
  (let [server_socket (new ServerSocket port)]

    (on-thread
     #(when-not (.isClosed server_socket)
        (try
          (accept-func (.accept server_socket))
          (catch SocketException e))
        (recur)))
    server_socket))

(defn closer [server]
  ;; TODO: keep track of the open connections and close them.
  (fn [] (.close server)))

(defn repl-server [port flusher]
  (swap! bucket-flusher (fn [_] flusher))
  (let [ss (create-server-multi socket-collector 8888)]
    (closer ss)))

(defn -main
  "Start a collector server"
  [& args]
  (binding [*out* *err*]
    (println "Started server with flush command:" (join " " args)))
  (if (not= (count args) 0)
    (do
      (swap! bucket-flusher (fn [_] (join " " args)))
      (create-server-multi socket-collector 8888))
    (binding [*out* *err*]
      (prn "called without flusher"))))
