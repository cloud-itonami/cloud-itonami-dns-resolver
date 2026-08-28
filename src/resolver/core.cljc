(ns resolver.core
  "Pure functions: domain-list parsing (Tranco CSV, Common Crawl's hyperlink
  web-graph domain-vertices format) and DNS-answer -> ledger-row shaping. No
  network, no filesystem, no clock read except where the caller passes one
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

(defn cc-reversed->domain
  "Common Crawl's hyperlink web-graph writes domains TLD-first, dot-reversed
  (`\"org.wikipedia.www\"` for `www.wikipedia.org`) so the file sorts by TLD.
  This undoes that back to normal notation."
  [reversed]
  (str/join "." (reverse (str/split reversed #"\."))))

(defn parse-cc-vertices-line
  "One line of a CC hyperlink-graph `*-domain-vertices.txt` file:
  \"<id>\\t<tld.reversed.labels>\\t<n>\" -> {:domain str} or nil for a
  malformed line — never throws. A streaming parse over ~10^8 lines must
  not die on one bad row; the caller counts skipped lines itself if it
  wants that measured (CLAUDE.md 検査を書く前の 6 問, #4)."
  [line]
  (let [parts (str/split (str/trim line) #"\t")]
    (when (= 3 (count parts))
      (let [[_id reversed _n] parts]
        (when-not (str/blank? reversed)
          {:domain (cc-reversed->domain reversed)})))))

(defn resolution-rows
  "One {:domain :rank :results [{:type :answer [{:data :TTL} ...]} ...]} ->
  ledger rows. A domain with zero answers across all 4 record types still
  gets ONE row (`:resolution/record-type \"NONE\"`) — evidence floor:
  'checked, nothing there' must not look the same as 'never checked'
  (CLAUDE.md 検査を書く前の 6 問, #1). `source`/`source-date` are carried
  by the caller (Tranco vs. Common Crawl vs. whatever gets added next) —
  this function does not know or care which list a domain came from."
  [{:keys [source source-date tick-id resolved-at]} {:keys [domain rank results]}]
  (let [rows (for [{:keys [type answer]} results
                    row answer
                    :let [data (some-> (:data row) str (str/replace #"\.$" ""))]
                    :when (not (str/blank? data))]
                {:resolution/id (str "res/" domain "|" type "|" data)
                 :resolution/domain domain :resolution/rank rank
                 :resolution/record-type type :resolution/value data
                 :resolution/ttl (:TTL row) :resolution/resolved-at resolved-at
                 :resolution/source source :resolution/source-date source-date
                 :resolution/tick-id tick-id})]
    (if (seq rows)
      rows
      [{:resolution/id (str "res/" domain "|NONE")
        :resolution/domain domain :resolution/rank rank
        :resolution/record-type "NONE" :resolution/value ""
        :resolution/ttl nil :resolution/resolved-at resolved-at
        :resolution/source source :resolution/source-date source-date
        :resolution/tick-id tick-id}])))
