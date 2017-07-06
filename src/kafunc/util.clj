(ns kafunc.util
  (:require [clojure.string :as str]))

(defn keyword->property
  "Convert `:key-words` to `\"key.words\"`"
  [kw]
  (str/join (replace {\- \.} (next (str kw)))))

(defn- update-keys*
  [f args [k v]]
  [(or (apply f k args) k) v])

(defn update-keys
  [m f & args]
  (into (empty m) (map (partial update-keys* f args) m)))

(defn- update-vals*
  [f args [k v]]
  [k (or (apply f v args) v)])

(defn update-vals
  [m f & args]
  (into (empty m) (map (partial update-vals* f args) m)))

(defn map->properties
  [m]
  (-> m
      (update-keys #(if (keyword? %) (keyword->property %) %))
      (update-vals #(if (keyword? %) (keyword->property %) %))))
