(ns metl.byte-tools.core
(:import
 (java.io InputStreamReader BufferedReader
           ByteArrayInputStream)
 (java.nio ByteBuffer)))


(defn string-to-bytebuffer [s]
  "returns a bytebuffer for a string"
  ;; getBytes returns bytes of chars, which are 2 bytes long
  (let [bytes (.getBytes s)
        buf (ByteBuffer/allocate (* 2 (count s)))]
    (doseq [b bytes]
      ;; These can be converted to a char and added with putChar
      (.putChar buf (char b))
      )
    (.rewind buf)))

(defn string-from-bytebuffer [bbuf]
  (.toString (.asCharBuffer bbuf)))

(defn int-to-bytebuffer [i]
  "returns a bytebuffer for an int"
  (.rewind (.putInt (ByteBuffer/allocate 4) i)))

(defn int-from-bytebuffer [bbuf]
  (.getInt bbuf))

(defn buffered-reader-from-bytes [someBytes]
  (new BufferedReader
       (new InputStreamReader
            (new ByteArrayInputStream
                 (.array someBytes)))))

(defn merge-bytes [first-bytes second-bytes]
  "returns the concatenated bytes of 2 bytebuffers"
  (let [buf (ByteBuffer/allocate
               (+ (.capacity first-bytes)(.capacity second-bytes)))]
    (.rewind (.put (.put buf first-bytes) second-bytes))))

(defn get-int-from-reader [reader]
  "read 4 bytes and return an integer"
  (let [buf (ByteBuffer/allocate 4)]
    (loop [c 0
           r (.read reader)]
      (.put buf c r)
      (when (< c 3)
        (recur (inc c) (.read reader)))
      )
    (.getInt buf)))


(defn get-string-from-reader [reader numChars]
  "read `numChars` from a reader and return the string"
  (let [numBytes (* 2 numChars)]

    (let [buf (ByteBuffer/allocate numBytes)]
      (loop [c 0
             r (.read reader)]
        (.put buf c r)
        (when (< c (- numBytes 1))
          (recur (inc c) (.read reader)))
        )
      (string-from-bytebuffer buf))))
