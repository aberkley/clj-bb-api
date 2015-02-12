(ns clj-bb-api.core
  (:require [clojure.reflect :as r]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clj-time.local :as l]
            [clj-time.coerce :as ce])
  (:use [clojure.pprint]))

(defn service
  [{:keys [service-name java-object] :as session}]
  (.getService java-object service-name))

(def config {:price-field "LAST_PRICE"
             :periodicity "DAILY"})

(defn- append-element
  [request- field value]
  ;; request- is a java object => stateful
  (.append request- field value)
  request-)

(defn- set-element
  [request field value]
  (.set request field value)
  request)

(defn- append-elements
  [request- name values]
  (reduce #(append-element %1 name %2) request- values))

(defn- append-override
  [request- field value]
  (let [override (.getElement request- "overrides")
        element (.appendElement override)]
    (.setElement element "fieldId" field)
    (.setElement element "value" value)
    request-))

(defn- reference-data-request
  [service- fields securities]
  (-> service-
      (.createRequest "ReferenceDataRequest")
      (append-elements "fields" fields)
      (append-elements "securities" securities)
      (append-override "INCLUDE_EXPIRED_CONTRACTS" "Y")))

(defn- historical-data-request
  [service- start end securities]
  (-> service-
      (.createRequest "HistoricalDataRequest")
      (set-element "periodicitySelection" (:periodicity config))
      (append-element "fields" (:price-field config))
      (set-element "startDate" start)
      (set-element "endDate" end)
      (append-elements "securities" securities)))

(defn- intraday-bar-request
  [service start end security interval]
  (-> service
      (.createRequest "IntradayBarRequest")
      (set-element "startDateTime" start)
      (set-element "endDateTime" end)
      (set-element "interval" interval)
      (set-element "eventType" "TRADE")
      (set-element "security" security)))

(defn- get-synchronous-events
  [session- request- id]
  (let [b (atom true)
        events (atom [])]
    (.sendRequest session- request- id)
    (while @b
      (let [event (.nextEvent session-)]
        (swap! events conj event)
        (reset! b (not (= (.intValue Event$EventType/RESPONSE)
                          (-> event .eventType .intValue))))))
    @events))

(defn- select-responses
  [events]
  (let [responses #{Event$EventType/RESPONSE Event$EventType/PARTIAL_RESPONSE}]
   (->> events
        (filter #(contains? responses (.eventType %))))))

(defn- select-messages
  [response]
  (let [iter (.messageIterator response)
        messages (atom [])]
    (while (.hasNext iter)
      (let [message (.next iter)]
        (swap! messages conj message)))
    @messages))

(defn- parse-bb-datetime
  [dt]
  (t/date-time
   (.year dt)
   (.month dt)
   (.dayOfMonth dt)
   (.hour dt)
   (.minute dt)
   (.second dt)
   (.milliSecond dt)))

(defn- parse-bb-date
  [date]
  (t/local-date
   (.year date)
   (.month date)
   (.dayOfMonth date)))

(defn- select-historical-data
  [message]
  ;; AB: made a change here by removing .asElement from first call, but this might be the emulator API not working properly!
  (let [security-data (-> message (.getElement "securityData"))
        security      (-> security-data (.getElementAsString "security"))
        field-data    (-> security-data (.getElement "fieldData"))]
    (for [i (range (.numValues field-data))]
      (let [d (.getValueAsElement field-data i)]
        {(:price-field config) (try (.getElementAsFloat64 d (:price-field config))
                                    (catch Exception e nil))
         "security" security
         "date"    (f/unparse
                    (f/formatter "yyyy-MM-dd")
                    (ce/to-date-time (parse-bb-date (.getElementAsDatetime d "date"))))}))))

(defn- select-intraday-bars
  "selects close price only"
  [security message]
  (let [bar-data (-> message .asElement (.getElement "barData") (.getElement "barTickData"))]
    (for [i (range (.numValues bar-data))]
      (let [d (.getValueAsElement bar-data i)]
        {"security" security
         "time" (f/unparse (f/formatter "yyyy-MM-dd hh:mm:ss") (parse-bb-datetime (.getElementAsDatetime d "time")))
         "close"     (.getElementAsFloat64 d "close")}))))

(defn- select-field
  [field selector field-data]
  (try (let [e (.getElement field-data field)]
         (selector e))
       (catch Exception e nil)))

(defn- select-field-data
  [message]
  (let [ds (-> message (.getElement "securityData"))]
    (for [i (range (.numValues ds))]
      (let [d  (.getValueAsElement ds i)
            s  (.getElementAsString d "security")
            fds (.getElement d "fieldData")]
        {"fieldData" fds
         "security" s}))))

(defn- select-reference-data
  [fields selectors message]
  {:pre (= (count selectors) (count fields))}
  (let [field-data (select-field-data message)
        f-s (map vector fields selectors)
        data (for [fd field-data [f s] f-s]
               {"security" (get fd "security")
                f (select-field f s (get fd "fieldData"))})]
    (->> data
         flatten
         (group-by #(get % "security"))
         vals
         (map (partial apply merge)))))

(defn- bulk-selector
  [sub-fields]
  #(for [i (range (.numValues %))]
     (let [f (.getValueAsElement % i)]
       (->> sub-fields
            (map (fn [sf] [sf (.getElementAsString f sf)]))
            (into {})))))

(defn get-reference-data
  ([{:keys [api] :as session} fields selectors securities]
     {:pre (= (count fields) (count selectors))}
     (let [{:keys [new-correlation-id]} api
           request- (reference-data-request (service api) fields securities)]
       (->> (get-synchronous-events (session api) request- (new-correlation-id 4))
            select-responses
            (map select-messages)
            flatten
            (map (partial select-reference-data fields selectors))
            flatten)))
  ([api fields securities]
     (let [selectors (repeat (count fields) #(.getValueAsString %))]
      (get-reference-data api fields selectors securities))))

(defn get-bulk-data
  [session fields sub-fields securities]
  {:pre (= (count fields) (count sub-fields))}
  (let [selectors (map bulk-selector sub-fields)]
    (get-reference-data session fields selectors securities)))

;;TODO refactor get-historical-data and get-intraday-bars as they share a lot of common structure

(defn get-historical-data
  [{:keys [api java-object] :as session} start end securities]
  (let [{:keys [new-correlation-id]} api
        request (historical-data-request (service session) start end securities)]
    (->> (get-synchronous-events java-object request (new-correlation-id 3))
         select-responses
         (map select-messages)
         flatten
         (map select-historical-data)
         flatten)))

(defn get-intraday-bars
  [{:keys [java-object api] :as session} start end security interval]
  {:pre (string? security)}
  (let [{:keys [new-correlation-id]} api
        request (intraday-bar-request (service session) start end security interval)]
    (->> (get-synchronous-events session request (new-correlation-id 2))
         select-responses
         (map select-messages)
         flatten
         (map (partial select-intraday-bars security))
         flatten)))

(defn get-historical-data-by-security
  [session start end securities]
  (->> securities
       (map (fn [s] (try (get-historical-data session start end [s])
                        (catch Exception e (throw (Exception. (str s ": " e)))))))
       flatten))
