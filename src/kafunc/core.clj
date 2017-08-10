(ns kafunc.core
  (:require [kafunc.impl.interop :as interop]
            [kafunc.impl.util :as util]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Constants and bindings

(def ^:dynamic *kafka-connect*
  "Connection string to bootstrap servers. Each entry is of the form
  address:port, and multiple entries are separated by commas."
  ;; By default, assume a local Kafka server running on the default port
  "localhost:9092")

(def ^:dynamic *consumer-config*
  "The default values to use for consumer configuration. Keys and values have
  the same meaning as those defined by Kafka for consumer configuration."
  ;; By default, use a byte-array deserializer, otherwise use Kafka defaults
  {:key-deserializer   interop/byte-deserializer
   :value-deserializer interop/byte-deserializer})

(def ^:dynamic *producer-config*
  "The default values to use for producer configuration. Keys and values have
  the same meaning as those defined by Kafka for producer configuration."
  ;; By default, use a byte-array serializer, otherwise use Kafka defaults
  {:key-serializer   interop/byte-serializer
   :value-serializer interop/byte-serializer})

(def ^:dynamic *deserializer*
  "A function which takes an argument of an object, and returns a byte array.
  The byte array returned must return a similar object when passed to
  *serializer*. The default function introduces no dependencies, but also makes
  no guarantees of efficiency."
  interop/io-deserialize)

(def ^:dynamic *serializer*
  "A function which takes an argument of a byte array, and returns an object.
  The object returned must return a similar byte array when passed to
  *deserializer*. The default function introduces no dependencies, but also
  makes no guarantees of efficiency."
  interop/io-serialize)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;; Functions

(defn- update-record-kv
  [f record]
  (-> record
      (update :key f)
      (update :value f)))

;;; Consumers

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

(defn subscribe
  "Subscribe a KafkaConsumer to the given topics.

  Any previous subscriptions will be lost!"
  [consumer topics]
  (let [topics (if (coll? topics) topics [topics])]
    (interop/subscribe consumer topics)))

(defn subscriptions
  "Retrieve a set of the current subscriptions, or nil if there are no
  subscriptions for the consumer."
  [consumer]
  (interop/consumer-subscriptions consumer))

(defn deserialize-records
  "Deserialize keys and values for all records in a collection."
  [xs & [deserializer]]
  (let [deserialize (fnil (or deserializer *deserializer* identity) nil)]
    (map (partial update-record-kv deserialize) xs)))

(defn next-records
  "Retrieves a collection of the next available records. Blocking.

  See consumer->record-seq for details of what a record contains. Optionally
  accepts a timeout value, which will cap the amount of milliseconds that this
  function will block. By default, it should block for Long/MAX_VALUE
  milliseconds, which should effectively not timeout."
  [consumer & {:keys [timeout deserializer]}]
  (let [timeout (or timeout Long/MAX_VALUE)]
    (deserialize-records (interop/poll consumer timeout))))

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
  (lazy-cat
    (next-records consumer :deserializer deserializer)
    (consumer->record-seq consumer deserializer)))

(defn record-seq->value-seq
  "Creates a lazy seq of values contained within the records in record-seq."
  [record-seq]
  (map :value record-seq))

(defn consumer->value-seq
  "Creates an infinite lazy seq which contains values consumed by a consumer.

  Useful if topic/partition/key/offset aren't important."
  [consumer & [deserializer]]
  (-> (consumer->record-seq consumer deserializer)
      (record-seq->value-seq)))

(defn topics->record-seq
  "Create a consumer, subscribe it to the topics, and return a seq of values
  consumed from those topics.

  If a group is not supplied, a unique identifier will be used instead."
  [topics & [group config deserializer]]
  (let [group (or group (interop/unique-string))]
    (-> (make-consumer group config)
        (subscribe topics)
        (consumer->record-seq deserializer))))

;;; Producers

(defn ->producer-record
  "Create a producer-record with the specified entries.

  Entries:
    * topic     - Destination topic.
    * value     - Value contained in record.
    * key       - (optional) Key.
    * partition - (optional) Destination partition of topic.
    * timestamp - (optional) Timestamp of record.

  The returned value will be a Clojure record with these values."
  [topic value & [key partition timestamp]]
  (interop/->PRecord key value partition topic timestamp))

(defn make-producer
  "Create a KafkaProducer, optionally with the given configuration.

  If config is supplied, those values will override *producer-config*, and
  *producer-config* overrides the default values created in this function."
  [& [config]]
  (interop/make-producer
    (merge {:bootstrap-servers *kafka-connect*}
           *producer-config*
           config)))

(defn serialize-records
  "Serialize keys and values for all records in a collection."
  [xs & [serializer]]
  (let [serialize (fnil (or serializer *serializer* identity) nil)]
    (map (partial update-record-kv serialize) xs)))

(defn send-records
  "Send a seq of producer-records to their destinations topic and partition.

  Returns a seq of the records updated with the following values, which are
  populated from the Kafka metadata:
    * :topic
    * :partition
    * :offset
    * :timestamp
    * :checksum

  The sending is eager, but the retreiving of metadata from the results is lazy.
  This allows asynchronous sends to simply ignore this metadata."
  [record-seq & [producer serializer]]
  (let [producer (or producer (make-producer))]
    (map merge
         record-seq
         (map (comp interop/record-meta->map deref)
              (doall (map (partial interop/send producer)
                          (serialize-records record-seq)))))))
