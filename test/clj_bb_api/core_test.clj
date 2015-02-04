(ns clj-bb-api.core-test
  (:require [clojure.test :refer :all]
            [clojure.reflect :as r]
            [clj-bb-api.system :as sys]
            [clj-bb-facade.core :as f]
            [clj-bb-api.core :as c]))

(deftest historic-data
  (let [session (sys/new-session "//blp/refdata"
                             {:host "localhost"
                              :port 8194}
                             f/api-constructors)]
    (> (count (c/get-historical-data session "20140101" "20150102" ["TPX Index" "SPX Index"])) 1)))
