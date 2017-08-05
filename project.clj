(defproject kafunc "0.1.0-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.apache.kafka/kafka_2.11 "0.10.2.1"]
                 [org.clojure/clojure "1.8.0"]]
  :repl-options {:init-ns kafunc.core})
