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

(defn make-consumer
  "Create a KafkaConsumer with the given group."
  [group]
  (interop/make-consumer
    (merge {:bootstrap-servers *kafka-connect*
            :group-id          group}
           *consumer-config*)))
