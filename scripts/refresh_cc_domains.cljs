#!/usr/bin/env nbb
(ns refresh-cc-domains
  "Downloads Common Crawl's latest hyperlink web-graph domain-vertices file
  and writes a flat plain-text domain list — one domain per line — that
  resolve_tick.cljs --source commoncrawl cursors through.

  This is a plain public HTTPS S3 object, NO AWS ACCOUNT NEEDED (confirmed
  live 2026-08-28: cc-main-2026-jun-jul-aug carries 119,722,885 distinct
  registered domains, ~893MB compressed). Streams the whole way — fetch ->
  gunzip -> readline -> buffered write — so the multi-GB uncompressed file
  is never held in memory at once.

  GATE-DNS_RESOLVER (same gate as resolve_tick.cljs — this is also a live
  network pull, just not a DNS resolution one).

  Run:
    # refuses, no network call:
    nbb --classpath src scripts/refresh_cc_domains.cljs

    # small validation run (first 10k parsed lines only):
    DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/refresh_cc_domains.cljs --live --limit 10000

    # the real thing (893MB download, several minutes, ~2-3GB written):
    DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/refresh_cc_domains.cljs --live"
  (:require [clojure.string :as str]
            [resolver.core :as core]
            ["node:fs" :as fs]
            ["node:path" :as path]
            ["node:readline" :as readline]
            ["node:stream" :as stream]
            ["node:zlib" :as zlib]))

(defn die! [code msg]
  (binding [*print-fn* *print-err-fn*] (println (str "REFUSED: " msg)))
  (.exit js/process code))

(defn env [k] (aget (.-env js/process) k))
(defn now [] (.toISOString (js/Date.)))

(defn parse-args [argv]
  (loop [xs argv out {}]
    (if-let [x (first xs)]
      (if (str/starts-with? x "--")
        (let [k (keyword (subs x 2))]
          (if (= k :live)
            (recur (rest xs) (assoc out :live true))
            (let [v (second xs)] (recur (nnext xs) (assoc out k v)))))
        (recur (rest xs) out))
      out)))

;; The most recent hyperlink web-graph release as of 2026-08-28. CC publishes
;; a new one every few months (see https://commoncrawl.org/web-graphs);
;; bumping this id is the whole upgrade — nothing else in this file changes.
(def graph-id "cc-main-2026-jun-jul-aug")
(def source-url
  (str "https://data.commoncrawl.org/projects/hyperlinkgraph/" graph-id
       "/domain/" graph-id "-domain-vertices.txt.gz"))

(def flush-every 20000)

(defn write-buffered!
  "Appends `line` (already newline-free) to `state`'s buffer, flushing to
  `fd` every `flush-every` lines so we pay one syscall per batch, not one
  per domain, across a file that can be 10^8 lines long."
  [fd state line]
  (swap! state update :buf conj line)
  (when (>= (count (:buf @state)) flush-every)
    (fs/writeSync fd (str (str/join "\n" (:buf @state)) "\n"))
    (swap! state assoc :buf [])))

(defn flush-remaining! [fd state]
  (when (seq (:buf @state))
    (fs/writeSync fd (str (str/join "\n" (:buf @state)) "\n"))
    (swap! state assoc :buf [])))

(defn -main []
  (let [args (parse-args *command-line-args*)
        out-root (or (:out args) "data")
        cache-path (path/join out-root "cache" "cc-domains.txt")
        meta-path (path/join out-root "cache" "cc-domains.meta.edn")
        limit (some-> (:limit args) js/parseInt)
        gate (some-> (env "DNS_RESOLVER_OPERATOR_GATE") str/lower-case)]
    (when-not (:live args)
      (die! 0 "--live not set (GATE-DNS_RESOLVER, offline-default). No network call made."))
    (when-not (= gate "open")
      (die! 2 "DNS_RESOLVER_OPERATOR_GATE is not \"open\" — --live alone does not authorize a live pull."))
    (fs/mkdirSync (path/dirname cache-path) #js {:recursive true})
    (let [controller (js/AbortController.)]
      (-> (js/fetch source-url #js {:signal (.-signal controller)})
          (.then
           (fn [r]
             (when-not (.-ok r) (throw (js/Error. (str "HTTP " (.-status r) " " source-url))))
             (js/Promise.
              (fn [resolve reject]
                (let [node-stream (.fromWeb stream/Readable (.-body r))
                      gunzip (zlib/createGunzip)
                      piped (.pipe node-stream gunzip)
                      rl (readline/createInterface #js {:input piped :crlfDelay js/Infinity})
                      out-fd (fs/openSync cache-path "w")
                      state (atom {:buf [] :seen 0 :written 0 :skipped 0 :stopped? false})
                      finish! (fn []
                                (when-not (:stopped? @state)
                                  (swap! state assoc :stopped? true)
                                  (flush-remaining! out-fd state)
                                  (fs/closeSync out-fd)
                                  (fs/writeFileSync meta-path
                                                    (str (pr-str {:version 1 :source "commoncrawl-hyperlinkgraph"
                                                                  :graph-id graph-id :source-url source-url
                                                                  :fetched-at (now) :partial? (boolean limit)
                                                                  :lines-seen (:seen @state)
                                                                  :domains-written (:written @state)
                                                                  :lines-skipped (:skipped @state)})
                                                         "\n"))
                                  (println (str "cc-domains: seen=" (:seen @state) " written=" (:written @state)
                                               " skipped=" (:skipped @state) " -> " cache-path))
                                  (resolve nil)))]
                  ;; Stopping early means aborting the HTTP request too, not
                  ;; just the line parser — without this the fetch keeps
                  ;; streaming the remaining ~890MB into a gunzip nobody
                  ;; reads from, and the process never exits (found live:
                  ;; a --limit 200 run printed its result line, then hung
                  ;; until an external `timeout` killed it).
                  (.on rl "line"
                       (fn [line]
                         (swap! state update :seen inc)
                         (if-let [{:keys [domain]} (core/parse-cc-vertices-line line)]
                           (do (write-buffered! out-fd state domain)
                               (swap! state update :written inc))
                           (swap! state update :skipped inc))
                         (when (and limit (>= (:written @state) limit) (not (:stopped? @state)))
                           (.abort controller)
                           (.destroy piped) (.destroy node-stream)
                           (.close rl)
                           (finish!))))
                  (.on rl "close" finish!)
                  (.on piped "error" (fn [e] (when-not (:stopped? @state) (reject e))))
                  (.on node-stream "error" (fn [e] (when-not (:stopped? @state) (reject e))))
                  (.on rl "error" reject))))))
          (.catch (fn [e]
                    (if (= "AbortError" (.-name e))
                      nil
                      (die! 1 (or (.-stack e) (.-message e) (str e))))))))))

(-main)
