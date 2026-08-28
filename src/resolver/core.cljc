(ns resolver.core
  "Pure functions: Tranco CSV parsing and DNS-answer -> ledger-row shaping.
  No network, no filesystem, no clock read except where the caller passes one
  in — everything here is `(map ...) -> map`, tested without a network,
  the same split app-hyakka's hyakka.ingest/resident_ingest.cljs draws
  between a connector's I/O and its pure admission logic."
  (:require [clojure.string :as str]))

(defn parse-tranco-csv
  "\"rank,domain\\n...\" -> [{:rank int :domain str} ...], no header row."
  [csv-text]
  (into []
        (keep (fn [line]
                (let [line (str/trim line)]
                  (when-not (str/blank? line)
                    (let [[rank domain] (str/split line #"," 2)]
                      (when (and rank domain)
                        {:rank (js/parseInt rank) :domain (str/lower-case (str/trim domain))}))))))
        (str/split-lines csv-text)))

(defn resolution-rows
  "One {:domain :rank :results [{:type :answer [{:data :TTL} ...]} ...]} ->
  ledger rows. A domain with zero answers across all 4 record types still
  gets ONE row (`:resolution/record-type \"NONE\"`) — evidence floor:
  'checked, nothing there' must not look the same as 'never checked'
  (CLAUDE.md 検査を書く前の 6 問, #1)."
  [{:keys [tranco-date tick-id resolved-at sha256]} {:keys [domain rank results]}]
  (let [rows (for [{:keys [type answer]} results
                    row answer
                    :let [data (some-> (:data row) str (str/replace #"\.$" ""))]
                    :when (not (str/blank? data))]
                {:resolution/id (str "res/" domain "|" type "|" data)
                 :resolution/domain domain :resolution/rank rank
                 :resolution/record-type type :resolution/value data
                 :resolution/ttl (:TTL row) :resolution/resolved-at resolved-at
                 :resolution/source "tranco-top-1m" :resolution/source-date tranco-date
                 :resolution/tick-id tick-id})]
    (if (seq rows)
      rows
      [{:resolution/id (str "res/" domain "|NONE")
        :resolution/domain domain :resolution/rank rank
        :resolution/record-type "NONE" :resolution/value ""
        :resolution/ttl nil :resolution/resolved-at resolved-at
        :resolution/source "tranco-top-1m" :resolution/source-date tranco-date
        :resolution/tick-id tick-id}])))
