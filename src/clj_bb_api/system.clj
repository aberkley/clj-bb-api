(ns clj-bb-api.system
  (:require [com.stuartsierra.component :as component]))

(defrecord Api [new-session
                new-session-options
                new-correlation-id]
  component/Lifecycle
  (start [component]
    (let [all-keys? (->> component
                         vals
                         (every? identity))]
      (if all-keys?
        component
        (throw (Exception. "missing API methods")))))
  (stop [component] nil))

(defrecord SessionOptions [java-object host port api]
  component/Lifecycle
  (start [{:keys [port host api] :as component}]
    (let [{:keys [new-session-options]} api
          o (new-session-options)]
      (.setServerHost o host)
      (.setServerPort o port)
      (assoc component :java-object o)))
  (stop [component] nil))

(defrecord Session [session-options java-object service-name api]
  component/Lifecycle
  (start [{:keys [session-options service-name api] :as component}]
    (let [{:keys [java-object]} session-options
          {:keys [new-session]} api
          s (new-session java-object)]
      (.start s)
      (.openService s service-name)
      (assoc component :java-object s)))
  (stop [component]))

(defn new-system [service-name host-options api-constructors]
  (-> (component/system-map
       :session (map->Session {:service-name service-name})
       :session-options (map->SessionOptions host-options)
       :api     (map->Api api-constructors))
      (component/system-using {:session [:session-options :api]
                               :session-options [:api]})))

(defn new-session [service-name host-options api-constructors]
  (-> (new-system service-name host-options api-constructors)
      .start
      :session))
