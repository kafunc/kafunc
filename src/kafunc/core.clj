(ns kafunc.core
  (:require [kafunc.interop :as interop]
            [kafunc.util :as util]))

(def ^:dynamic *kafka-connect*
  "Connection string to bootstrap servers. Each entry is of the form
  address:port, and multiple entries are separated by commas."
  ;; By default, assume a local Kafka server running on the default port
  "localhost:9092")

(def ^:dynamic *consumer-config*
  "The default values to use for consumer configuration. Keys and values have
  the same meaning as those defined by Kafka for consumer configuration."
  ;; By default, use a byte-array deserializer, otherwise use Kafka defaults
  {:key-deserializer   interop/deserializer
   :value-deserializer interop/deserializer})

(def ^:dynamic *deserializer*
  "A function which takes an argument of an object, and returns a byte array.
  The byte array returned must return a similar object when passed to
  *serializer*. The default function introduces no dependencies, but another
  makes no guarantees of efficiency."
  interop/io-deserialize)

(defn make-consumer
  "Create a KafkaConsumer with the given group and/or configuration.

  If config is supplied, those values will override *consumer-config*, and
  *consumer-config* overrides the default values created in this function."
  ([group & [config]]
   (interop/make-consumer
     (merge {:bootstrap-servers *kafka-connect*
             :group-id          group}
            *consumer-config*
            config))))

(defn next-records
  "Retrieves a collection of the next available records.

  See consumer->record-seq for details of what a record contains."
  [consumer & [timeout]]
  (let [timeout (or timeout Long/MAX_VALUE)]
    (interop/poll consumer timeout)))

(defn consumer->record-seq
  "Create an infinite lazy seq which contains records consumed by a consumer.

  The keys in the record are:
    * :topic      - Topic whence the record was consumed
    * :partition  - Partition whence the record was consumed
    * :timestamp  - Timestamp of record
    * :key        - Key of record
    * :value      - Value of record
    * :offset     - Offset whence the record was consumed

  Uses the provided deserializer, or *deserializer* if not specified. This
  value is bound during the creation of the seq, so the binding does not need
  to be maintained for the lifetime of the seq."
  [consumer & [deserializer]]
  (let [deserialize (or deserializer *deserializer*)]
    (lazy-cat
      (-> (next-records consumer)
          (update :key (fnil deserialize nil))
          (update :value (fnil deserialize nil)))
      (consumer->record-seq consumer deserializer))))

(defn consumer->value-seq
  "Creates an infinite lazy seq which contains values consumed by a consumer.

  Useful if topic/partition/key/offset aren't important."
  [consumer & [deserializer]]
  (map :value (consumer->record-seq consumer deserializer)))
