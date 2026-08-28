#!/usr/bin/env nbb
(ns resolve-tick
  "One bounded, G7-gated tick of active DNS resolution against a slice of
  one of two domain lists, chosen with --source:

    tranco (default)  Tranco's daily top-1M list — fetched fresh every tick
                       (9.7MB, cheap). Wraps at EOF (cycles).
    commoncrawl        Common Crawl's hyperlink web-graph domain list
                       (~120M domains) — cursored from the LOCAL cache file
                       scripts/refresh_cc_domains.cljs writes (too large to
                       fetch fresh every tick; run that script first). Does
                       NOT wrap — at this scale reaching EOF is not a
                       realistic near-term event, and pretending it wraps
                       the same way Tranco does would hide that a stalled
                       refresh_cc_domains run silently truncates coverage.

  GATE-DNS_RESOLVER: offline-default. A live network pull (--live) requires
  the DNS_RESOLVER_OPERATOR_GATE env var to be set, matching the
  yabai-actor / ipaddress-actor G7 pattern this repo follows (see README).

  Pipeline (mirrors app-hyakka's resident_ingest.cljs — bounded, deterministic,
  no LLM, evidence-is-the-fact-itself):

    domain list (source-specific acquisition, see above)
      -> cursor N domains (--state tracks the offset per source, wraps at EOF
         for tranco only)
      -> DNS-over-HTTPS A/AAAA/MX/NS lookups, bounded-concurrency worker
         pool (--concurrency) split round-robin across two resolvers
         (Google + Cloudflare — see doh-resolvers below), same JSON API
         shape app-hyakka's collect-dns! already uses
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
            ["node:path" :as path]
            ["node:readline" :as readline]))

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

(defn fetch-with-timeout! [url opts timeout-ms]
  (let [controller (js/AbortController.)
        timer (js/setTimeout #(.abort controller) timeout-ms)
        request-opts (js/Object.assign #js {} opts #js {:signal (.-signal controller)})]
    (-> (js/fetch url request-opts)
        (.finally #(js/clearTimeout timer)))))

;; ── source: tranco (fetch + unzip; shell out to `unzip`, no JS zip dep) ──────

(def tranco-url "https://tranco-list.eu/top-1m.csv.zip")

(defn tranco-window!
  "-> {:slice [{:domain :rank}...] :total :source :source-date :list-id}.
  Fetches the whole list fresh (cheap, 9.7MB) and cursors+cycles in memory —
  fine at Tranco's 10^6 scale."
  [cursor n]
  (-> (fetch-with-timeout! tranco-url #js {} 120000)
      (.then (fn [r]
               (when-not (.-ok r) (throw (js/Error. (str "HTTP " (.-status r) " " tranco-url))))
               (.arrayBuffer r)))
      (.then (fn [ab]
               (let [buf (js/Buffer.from ab)
                     tmp (path/join (.tmpdir os) (str "tranco-" (.now js/Date) ".zip"))]
                 (fs/writeFileSync tmp buf)
                 (let [csv-text (.execFileSync cp "unzip" #js ["-p" tmp]
                                               #js {:encoding "utf8" :maxBuffer (* 64 1024 1024)})]
                   (fs/unlinkSync tmp)
                   (let [list-id (sha256 csv-text)
                         domains (core/parse-tranco-csv csv-text)
                         total (count domains)]
                     (when (zero? total)
                       (die! 2 "Tranco CSV parsed to 0 rows — refusing to report a tick against nothing"))
                     {:slice (->> (cycle domains) (drop (mod cursor total)) (take (min n total)) vec)
                      :total total :source "tranco-top-1m"
                      :source-date (subs (now) 0 10) :list-id (subs list-id 0 12)})))))))

;; ── source: commoncrawl (local cache; see refresh_cc_domains.cljs) ──────────

(defn cc-cache-paths [out-root]
  {:cache (path/join out-root "cache" "cc-domains.txt")
   :meta (path/join out-root "cache" "cc-domains.meta.edn")})

(defn cc-window!
  "-> {:slice [{:domain}...] :total :source :source-date :list-id}. Streams
  the local cache file, skipping to `cursor` and collecting up to `n` lines,
  then STOPS READING (does not reach EOF on a 10^8-line file just to confirm
  there was nothing left). Does not wrap: a slice shorter than `n` at the
  tail means 'caught up to the cache's current end', not an error."
  [out-root cursor n]
  (let [{:keys [cache meta]} (cc-cache-paths out-root)]
    (when-not (fs/existsSync cache)
      (die! 2 (str "REFUSED: no " cache " — run refresh_cc_domains.cljs first")))
    (let [m (read-edn meta {})]
      (js/Promise.
       (fn [resolve reject]
         (let [rs (fs/createReadStream cache #js {:encoding "utf8"})
               rl (readline/createInterface #js {:input rs :crlfDelay js/Infinity})
               idx (atom 0) out (atom [])]
           (.on rl "line"
                (fn [line]
                  (when (and (>= @idx cursor) (< (count @out) n) (not (str/blank? line)))
                    (swap! out conj {:domain line}))
                  (swap! idx inc)
                  (when (>= (count @out) n)
                    (.close rl) (.destroy rs))))
           (.on rl "close"
                (fn []
                  (resolve {:slice @out
                            :total (or (:domains-written m) @idx)
                            :source "commoncrawl-hyperlinkgraph"
                            :source-date (some-> (:fetched-at m) (subs 0 10))
                            :list-id (:graph-id m)})))
           (.on rs "error" reject)
           (.on rl "error" reject)))))))

;; ── DNS-over-HTTPS resolution (same JSON API app-hyakka's collect-dns! uses,
;;    split across two independent public resolvers) ─────────────────────────

(defn fetch-text! [url]
  (-> (fetch-with-timeout! url #js {:headers #js {"accept" "application/dns-json"}} 15000)
      (.then (fn [r]
               (-> (.text r)
                   (.then (fn [body]
                            (when-not (.-ok r) (throw (js/Error. (str "HTTP " (.-status r) " " url))))
                            body)))))))

(def doh-resolvers
  "Two independent public DoH resolvers, both confirmed live to speak the
  same `application/dns-json` GET shape (Google's own, and Cloudflare's —
  verified 2026-08-28: identical Answer/Question structure). Splitting
  domains between them roughly halves the sustained rate either one sees
  for a given total throughput — the polite way to raise throughput
  without doubling the load on any single free service."
  ["https://dns.google/resolve" "https://cloudflare-dns.com/dns-query"])

(defn doh-url [resolver name type]
  (str resolver "?name=" (js/encodeURIComponent name) "&type=" type))

(defn resolve-domain!
  "One domain, all 4 record types, in parallel, against ONE resolver picked
  by `domain-index` (round-robin — every domain's 4 queries go to the same
  resolver, rather than splitting one domain's queries across resolvers for
  no benefit). Never rejects — a failed type lookup (timeout, NXDOMAIN,
  network error) becomes an empty answer for that type rather than failing
  the whole domain; one dead lookup must not cost the rest of the tick."
  [domain domain-index]
  (let [resolver (nth doh-resolvers (mod domain-index (count doh-resolvers)))]
    (-> (js/Promise.all
         (clj->js
          (map (fn [type]
                 (-> (fetch-text! (doh-url resolver domain type))
                     (.then (fn [body] {:type type :answer (:Answer (js->clj (js/JSON.parse body) :keywordize-keys true))}))
                     (.catch (fn [_] {:type type :answer []}))))
               ["A" "AAAA" "MX" "NS"])))
        (.then (fn [raw] {:domain domain :results (js->clj raw :keywordize-keys true)})))))

(defn resolve-all!
  "Resolves `domains` with at most `concurrency` domains in flight at once
  (each domain itself fires 4 parallel queries, so peak sockets are roughly
  4x this). An unbounded js/Promise.all over a 40,000-domain tick would
  open ~160,000 simultaneous connections — a burst, not a rate, and the
  kind of pattern that gets a client rate-limited or blocked. This paces
  requests to a bounded worker-pool instead, so a large --n still finishes
  as a sustained trickle rather than a spike."
  [domains concurrency]
  (js/Promise.
   (fn [resolve _reject]
     (let [queue (atom (vec (map-indexed vector domains)))
           results (atom [])
           in-flight (atom 0)]
       (letfn [(pump! []
                 (if (and (empty? @queue) (zero? @in-flight))
                   (resolve @results)
                   (loop []
                     (when (and (seq @queue) (< @in-flight concurrency))
                       (let [[i d] (first @queue)]
                         (swap! queue rest)
                         (swap! in-flight inc)
                         (-> (resolve-domain! d i)
                             (.then (fn [r]
                                      (swap! results conj r)
                                      (swap! in-flight dec)
                                      (pump!)))))
                       (recur)))))]
         (pump!))))))

;; ── main ─────────────────────────────────────────────────────────────────
;; Split into small helpers (rather than one deeply-nested promise chain) —
;; a >10-level-deep paren nest is exactly where a single extra/missing `)`
;; hides; shallow functions make that class of bug visible on sight.

(defn write-tick!
  "Already-resolved rows (js->clj'd) + tick context -> ledger file + state
  file + a one-line summary. The single write side-effect point for a tick."
  [{:keys [source source-date tick-id resolved-at slice total cursor wrap? n out-root state-path]}
   resolved]
  (let [ctx {:source source :source-date source-date :tick-id tick-id :resolved-at resolved-at}
        by-domain (into {} (map (juxt :domain identity)) resolved)
        rows (mapcat (fn [{:keys [domain rank]}]
                       (core/resolution-rows ctx (assoc (get by-domain domain) :rank rank)))
                     slice)
        day (subs resolved-at 0 10)
        ledger-path (path/join out-root "ledger" day (str tick-id ".edn"))
        next-cursor (if wrap? (mod (+ cursor (count slice)) total) (+ cursor (count slice)))]
    (write-edn! ledger-path (vec rows))
    (write-edn! state-path {:version 1 :cursor next-cursor :source source
                            :last-tick-id tick-id :last-tick-at resolved-at :list-total total})
    ;; "caught up" means we asked for `n` and the window gave us fewer — the
    ;; only honest signal of that, since `total` alone (esp. commoncrawl's
    ;; ~1.2x10^8) says nothing about how close THIS tick's cursor is to EOF.
    (println (str "ledger " ledger-path " rows=" (count rows)
                 " domains=" (count slice) " next-cursor=" next-cursor "/" total
                 (when (and (not wrap?) n (< (count slice) n))
                   "  (caught up to cache end this tick — refresh_cc_domains.cljs for more)")))))

(defn run! []
  (let [args (parse-args *command-line-args*)
        source (or (:source args) "tranco")
        n (js/parseInt (or (:n args) (if (= source "commoncrawl") "40000" "200")))
        concurrency (js/parseInt (or (:concurrency args) "80"))
        out-root (or (:out args) "data")
        state-path (or (:state args) (path/join out-root (str "state-" source ".edn")))
        state (read-edn state-path {:version 1 :cursor 0})
        cursor (:cursor state 0)
        gate (some-> (env "DNS_RESOLVER_OPERATOR_GATE") str/lower-case)]
    (when-not (:live args)
      (die! 0 "--live not set (GATE-DNS_RESOLVER, offline-default). No network call made."))
    (when-not (= gate "open")
      (die! 2 "DNS_RESOLVER_OPERATOR_GATE is not \"open\" — --live alone does not authorize a live pull."))
    (when-not (#{"tranco" "commoncrawl"} source)
      (die! 2 (str "unknown --source " source " (want tranco or commoncrawl)")))
    (-> (if (= source "commoncrawl") (cc-window! out-root cursor n) (tranco-window! cursor n))
        (.then (fn [{:keys [slice total source source-date list-id]}]
                 (println (str source " list-id=" list-id " total=" total
                              " cursor=" cursor " slice=" (count slice)))
                 (when (empty? slice)
                   (die! 0 "0 domains in this window — nothing to resolve this tick."))
                 (-> (resolve-all! (map :domain slice) concurrency)
                     (.then (fn [resolved]
                              (write-tick!
                               {:source source :source-date source-date
                                :tick-id (str (str/replace (now) #"[:.]" "-") "-"
                                              (subs (or list-id (sha256 source)) 0 8))
                                :resolved-at (now) :slice slice :total total :cursor cursor
                                :wrap? (= source "tranco-top-1m") :n n
                                :out-root out-root :state-path state-path}
                               resolved))))))
        (.catch (fn [e] (die! 1 (or (.-stack e) (.-message e) (str e))))))))

(run!)
