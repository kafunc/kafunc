(ns kafunc.interop
  "Namespace for interop with Java/Kafka, to keep core as pure clojure"
  (:require [kafunc.util :as util])
  (:import (org.apache.kafka.clients.consumer
             KafkaConsumer ConsumerRecord Consumer)
           (org.apache.kafka.clients.producer
             KafkaProducer ProducerRecord Producer)
           (org.apache.kafka.common.serialization
             ByteArraySerializer ByteArrayDeserializer)
           (java.util Properties)))

(def serializer (.getName ByteArraySerializer))
(def deserializer (.getName ByteArrayDeserializer))

(defrecord CRecord [key value partition topic timestamp offset checksum])
(defrecord PRecord [key value partition topic timestamp])

(defn- property-map->properties
  ^Properties [m]
  (let [prop (Properties.)
        m    (util/map->properties m)]
    (dorun (map #(.put prop %1 %2) m))))

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

(defn kafka->precord
  "Convert a Kafka ProducerRecord to a Clojure record"
  [^ProducerRecord record]
  (->PRecord
    (.key record)
    (.value record)
    (.partition record)
    (.topic record)
    (.timestamp record)))

(defn precord->kafka
  "Convert a Clojure record (PRecord) to a Kafka ProducerRecord"
  [record]
  (ProducerRecord.
    (:topic record)
    (:partition record)
    (:timestamp record)
    (:key record)
    (:value record)))

(defn poll
  "Poll a Kafka consumer"
  [^Consumer consumer timeout]
  (when-let [polled (.poll consumer timeout)]
    (map kafka->crecord polled)))

(defn send
  "Send a Kafka ProducerRecord or Clojure PRecord through a Kafka producer

  Returns the future that the producer generates."
  [^Producer producer record]
  (if (map? record)
    (recur producer (precord->kafka record))
    (.send producer record)))
