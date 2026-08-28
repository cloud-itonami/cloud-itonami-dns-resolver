#!/usr/bin/env nbb
(ns resolve-tick
  "One bounded, G7-gated tick of active DNS resolution against a slice of
  Tranco's daily top-1M domain list.

  GATE-DNS_RESOLVER: offline-default. A live network pull (--live) requires
  the DNS_RESOLVER_OPERATOR_GATE env var to be set, matching the
  yabai-actor / ipaddress-actor G7 pattern this repo follows (see README).

  Pipeline (mirrors app-hyakka's resident_ingest.cljs — bounded, deterministic,
  no LLM, evidence-is-the-fact-itself):

    Tranco top-1M zip (daily, free, no key)
      -> unzip (shells out to `unzip`, no JS zip lib needed)
      -> cursor N domains (state.edn tracks rank offset, wraps at EOF)
      -> DNS-over-HTTPS A/AAAA/MX/NS lookups (dns.google/resolve, same
         transport app-hyakka's collect-dns! already uses)
      -> one dated EDN ledger file (git, source of truth — CLAUDE.md
         「消して再構築できるか」: this ledger is the premise, R2/Iceberg
         is a rebuildable projection of it, never the other way round)

  Scope and policy: 90-docs/adr/2608280900-world-scale-dns-domain-collection.edn
  (superproject com-junkawasaki/root). No registrant/WHOIS data is touched —
  DNS resolution results only."
  (:require [cljs.reader :as edn]
            [clojure.string :as str]
            [resolver.core :as core]
            ["node:child_process" :as cp]
            ["node:crypto" :as crypto]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]))

(defn die! [code msg]
  (binding [*print-fn* *print-err-fn*] (println (str "REFUSED: " msg)))
  (.exit js/process code))

(defn env [k] (aget (.-env js/process) k))
(defn now [] (.toISOString (js/Date.)))
(defn sha256 [s] (-> (crypto/createHash "sha256") (.update s "utf8") (.digest "hex")))

(defn parse-args [argv]
  (loop [xs argv out {}]
    (if-let [x (first xs)]
      (if (str/starts-with? x "--")
        (let [k (keyword (subs x 2))]
          (if (= k :live)
            (recur (rest xs) (assoc out :live true))
            (let [v (second xs)]
              (recur (nnext xs) (assoc out k v)))))
        (recur (rest xs) out))
      out)))

(defn read-edn [p fallback]
  (if (fs/existsSync p) (edn/read-string (fs/readFileSync p "utf8")) fallback))

(defn write-edn! [p x]
  (fs/mkdirSync (path/dirname p) #js {:recursive true})
  (fs/writeFileSync p (str (pr-str x) "\n")))

;; ── Tranco fetch + unzip (shell out to `unzip`; no JS zip dependency) ───────

(def tranco-url "https://tranco-list.eu/top-1m.csv.zip")

(defn fetch-with-timeout! [url opts timeout-ms]
  (let [controller (js/AbortController.)
        timer (js/setTimeout #(.abort controller) timeout-ms)
        request-opts (js/Object.assign #js {} opts #js {:signal (.-signal controller)})]
    (-> (js/fetch url request-opts)
        (.finally #(js/clearTimeout timer)))))

(defn fetch-tranco-csv!
  "-> {:csv-text :sha256 :bytes}. Downloads the zip to a tmp file (fetch's
  streaming body -> Buffer), then shells out to `unzip -p` for the CSV text —
  Node has no zip-central-directory reader in core, and vendoring one for a
  single call is a worse trade than one `unzip` invocation."
  []
  (-> (fetch-with-timeout! tranco-url #js {} 120000)
      (.then (fn [r]
               (when-not (.-ok r) (throw (js/Error. (str "HTTP " (.-status r) " " tranco-url))))
               (.arrayBuffer r)))
      (.then (fn [ab]
               (let [buf (js/Buffer.from ab)
                     tmp (path/join (.tmpdir os) (str "tranco-" (.now js/Date) ".zip"))]
                 (fs/writeFileSync tmp buf)
                 (let [out (.execFileSync cp "unzip" #js ["-p" tmp]
                                          #js {:encoding "utf8" :maxBuffer (* 64 1024 1024)})]
                   (fs/unlinkSync tmp)
                   {:csv-text out :sha256 (sha256 out) :bytes (.-length buf)}))))))

;; ── DNS-over-HTTPS resolution (same transport as app-hyakka's collect-dns!) ──

(defn fetch-text! [url]
  (-> (fetch-with-timeout! url #js {:headers #js {"accept" "application/dns-json"}} 15000)
      (.then (fn [r]
               (-> (.text r)
                   (.then (fn [body]
                            (when-not (.-ok r) (throw (js/Error. (str "HTTP " (.-status r) " " url))))
                            body)))))))

(defn doh-url [name type]
  (str "https://dns.google/resolve?name=" (js/encodeURIComponent name) "&type=" type))

(defn resolve-domain!
  "One domain, all 4 record types, in parallel. Never rejects — a failed type
  lookup (timeout, NXDOMAIN, network error) becomes an empty answer for that
  type rather than failing the whole domain; one dead lookup must not cost
  the rest of the tick."
  [domain]
  (-> (js/Promise.all
       (clj->js
        (map (fn [type]
               (-> (fetch-text! (doh-url domain type))
                   (.then (fn [body] {:type type :answer (:Answer (js->clj (js/JSON.parse body) :keywordize-keys true))}))
                   (.catch (fn [_] {:type type :answer []}))))
             ["A" "AAAA" "MX" "NS"])))
      (.then (fn [raw] {:domain domain :results (js->clj raw :keywordize-keys true)}))))

;; ── main ─────────────────────────────────────────────────────────────────
;; Split into small helpers (rather than one deeply-nested promise chain) —
;; a >10-level-deep paren nest is exactly where a single extra/missing `)`
;; hides; shallow functions make that class of bug visible on sight.

(defn write-tick!
  "Already-resolved rows (js->clj'd) + tick context -> ledger file + state
  file + a one-line summary. The single write side-effect point for a tick."
  [{:keys [tranco-date tick-id resolved-at slice sha256 total cursor out-root state-path]}
   resolved]
  (let [ctx {:tranco-date tranco-date :tick-id tick-id :resolved-at resolved-at :sha256 sha256}
        by-domain (into {} (map (juxt :domain identity)) resolved)
        rows (mapcat (fn [{:keys [domain rank]}]
                       (core/resolution-rows ctx (assoc (get by-domain domain) :rank rank)))
                     slice)
        day (subs resolved-at 0 10)
        ledger-path (path/join out-root "ledger" day (str tick-id ".edn"))
        next-cursor (mod (+ cursor (count slice)) total)]
    (write-edn! ledger-path (vec rows))
    (write-edn! state-path {:version 1 :cursor next-cursor
                            :last-tick-id tick-id :last-tick-at resolved-at
                            :tranco-sha256 sha256 :tranco-total total})
    (println (str "ledger " ledger-path " rows=" (count rows)
                 " domains=" (count slice) " next-cursor=" next-cursor "/" total))))

(defn process-tranco!
  "{:csv-text :sha256 :bytes} + run opts -> resolves the cursor slice and
  writes the tick. Returns the .then/.catch-able promise."
  [{:keys [csv-text sha256 bytes]} {:keys [n out-root state-path state]}]
  (let [domains (core/parse-tranco-csv csv-text)
        total (count domains)
        _ (when (zero? total)
            (die! 2 "Tranco CSV parsed to 0 rows — refusing to report a tick against nothing"))
        cursor (mod (:cursor state 0) total)
        slice (->> (cycle domains) (drop cursor) (take (min n total)) vec)
        tick-id (str (str/replace (now) #"[:.]" "-") "-" (subs sha256 0 8))
        tranco-date (subs (now) 0 10)]
    (println (str "tranco: " total " domains, sha256=" (subs sha256 0 12)
                 ", bytes=" bytes ", cursor=" cursor ", slice=" (count slice)))
    (-> (js/Promise.all (clj->js (map resolve-domain! (map :domain slice))))
        (.then (fn [resolved]
                 (write-tick! {:tranco-date tranco-date :tick-id tick-id
                               :resolved-at (now) :slice slice :sha256 sha256
                               :total total :cursor cursor :out-root out-root
                               :state-path state-path}
                              (js->clj resolved :keywordize-keys true)))))))

(defn run! []
  (let [args (parse-args *command-line-args*)
        n (js/parseInt (or (:n args) "200"))
        out-root (or (:out args) "data")
        state-path (or (:state args) (path/join out-root "state.edn"))
        state (read-edn state-path {:version 1 :cursor 0})
        gate (some-> (env "DNS_RESOLVER_OPERATOR_GATE") str/lower-case)]
    (when-not (:live args)
      (die! 0 "--live not set (GATE-DNS_RESOLVER, offline-default). No network call made."))
    (when-not (= gate "open")
      (die! 2 "DNS_RESOLVER_OPERATOR_GATE is not \"open\" — --live alone does not authorize a live pull."))
    (-> (fetch-tranco-csv!)
        (.then (fn [tranco]
                 (process-tranco! tranco {:n n :out-root out-root
                                          :state-path state-path :state state})))
        (.catch (fn [e] (die! 1 (or (.-stack e) (.-message e) (str e))))))))

(run!)
