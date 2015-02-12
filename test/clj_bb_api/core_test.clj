(ns clj-bb-api.core-test
  (:import [sra_london.bloomberg_emulator Session SessionOptions CorrelationID Event$EventType])
  (:require [clojure.test :refer :all]
            [clojure.reflect :as r]
            [clj-bb-api.system :as sys]
            [clj-bb-api.core :as c]))

(def constructors
  {:new-session-options (fn [] (SessionOptions.))
   :new-session (fn [session-options] (Session. session-options))
   :new-correlation-id (fn [id] (CorrelationID. id))
   :event-types {:response Event$EventType/RESPONSE
                 :partial-response Event$EventType/PARTIAL_RESPONSE}})

(deftest historic-data
  (let [session (sys/new-session "//blp/refdata"
                             {:host "localhost"
                              :port 8194}
                             constructors)]
    (> (count (c/get-historical-data session "20140101" "20150102" ["TPX Index" "SPX Index"])) 1)))
