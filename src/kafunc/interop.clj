(ns kafunc.interop
  "Namespace for interop with Java/Kafka, to keep core as pure clojure"
  (:import (org.apache.kafka.clients.consumer
             KafkaConsumer ConsumerRecord)))

(defrecord CRecord [key value partition topic timestamp offset checksum])

(defn kafka->crecord
  "Convert a Kafka ConsumerRecord to a Clojure record"
  [^ConsumerRecord record]
  (->CRecord
    (.key record)
    (.value record)
    (.partition record)
    (.topic record)
    (.timestamp record)
    (.offset record)
    (.checksum record)))
