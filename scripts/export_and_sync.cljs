#!/usr/bin/env nbb
(ns export-and-sync
  "data/ledger/**/*.edn -> NDJSON -> Cloudflare R2 Data Catalog (Apache
  Iceberg), reusing the ALREADY-GENERAL superproject loader
  (com-junkawasaki/root's scripts/datalake-sync.py — the same one
  com-junkawasaki/org-gleif-projections and app-hyakka's ASN/prefix data
  already land through, per 90-docs/adr/2608280100 and 2608280900).

  THIS IS A PROJECTION, NOT A PREMISE (CLAUDE.md 「消して再構築できるか」):
  data/ledger/**/*.edn is the source; this script dedups by :resolution/id
  (last file wins) and rebuilds `cloud_itonami.dns_resolution`.
  `datalake-sync.py` always does a full `t.overwrite(tbl)` — there is no
  separate `--replace`-flagged writer in this workspace (a prior version
  of this docstring claimed one existed at
  `scripts/datalake/iceberg_writer.py`; that path does not exist, checked
  2026-08-28 — corrected rather than left to mislead the next reader).

  INCREMENTAL EXPORT, NOT re-derived from scratch every run: found live
  (2026-08-28) that re-reading and re-parsing the WHOLE historical ledger
  every tick (cljs.reader on ~1.6GB of accumulated EDN, ~7.5s per 10MB
  file) OOM'd the export step outright once the ledger had been running
  long enough, and even short of OOM was headed toward minutes of wasted
  CPU per sync that only gets worse as the ledger grows. `--out-dir`
  persists an `accumulator.json` (id -> row, i.e. the CURRENT deduped
  table state) and a `folded-files.json` (which ledger files are already
  folded in) between runs — each run parses only the ledger files not yet
  in that set, merges them into the accumulator, and syncs the full
  accumulator (unchanged Iceberg overwrite semantics; the incrementality
  is purely in how the payload gets BUILT, not in what gets committed).
  First run with an empty/missing `--out-dir` bootstraps by treating
  every ledger file as new — same cost as the old always-full-reparse
  behavior, but paid once instead of every tick.

  Requires the superproject checkout for the loader — pass its root with
  --root (default: two levels up from this repo, i.e. this repo living at
  <superproject>/orgs/cloud-itonami/dns-resolver-actor).

  Run:
    nbb scripts/export_and_sync.cljs [--root <superproject>] [--out-dir <dir>]"
  (:require [cljs.reader :as edn]
            [clojure.string :as str]
            ["node:child_process" :as cp]
            ["node:fs" :as fs]
            ["node:os" :as os]
            ["node:path" :as path]))

(def argv (vec (drop 3 (js->clj (.-argv js/process)))))
(defn- flag [n] (let [i (.indexOf (clj->js argv) n)]
                  (when (and (>= i 0) (< (inc i) (count argv))) (nth argv (inc i)))))

(def superproject-root
  (or (flag "--root") (path/resolve (str (.cwd js/process)) ".." ".." "..")))
(def out-dir (or (flag "--out-dir") (path/join (.tmpdir os) "dns-resolver-lake")))
(def ledger-dir "data/ledger")

(defn die! [code msg] (js/console.error msg) (.exit js/process code))

(defn ledger-files [dir]
  (if-not (fs/existsSync dir)
    (die! 2 (str "REFUSED: no ledger at " dir))
    (sort (mapcat (fn walk [p]
                    (let [st (fs/statSync p)]
                      (if (.isDirectory st)
                        (mapcat walk (sort (map #(path/join p %) (fs/readdirSync p))))
                        (when (str/ends-with? p ".edn") [p]))))
                  [dir]))))

(defn row->json [r]
  {"id" (:resolution/id r) "domain" (:resolution/domain r)
   "rank" (str (:resolution/rank r)) "record_type" (:resolution/record-type r)
   "value" (:resolution/value r) "ttl" (str (or (:resolution/ttl r) ""))
   "resolved_at" (:resolution/resolved-at r) "source" (:resolution/source r)
   "source_date" (:resolution/source-date r) "tick_id" (:resolution/tick-id r)})

(defn read-json [path default]
  (if (fs/existsSync path) (js/JSON.parse (fs/readFileSync path "utf8")) default))

(defn -main []
  (let [files (ledger-files ledger-dir)
        _ (when (empty? files) (die! 2 "REFUSED: 0 ledger files — an unread tree is not an empty dataset."))
        _ (fs/mkdirSync out-dir #js {:recursive true})
        acc-path (path/join out-dir "accumulator.json")
        folded-path (path/join out-dir "folded-files.json")
        acc (read-json acc-path #js {})
        folded (js/Set. (read-json folded-path #js []))
        new-files (remove #(.has folded %) files)]
    (println (str "LEDGER\t" (count files) " files\tfolded=" (- (count files) (count new-files))
                 "\tnew=" (count new-files)))
    (doseq [f new-files]
      (doseq [r (edn/read-string (fs/readFileSync f "utf8"))]
        (aset acc (:resolution/id r) (clj->js (row->json r))))
      (.add folded f))
    (when (seq new-files)
      (fs/writeFileSync acc-path (js/JSON.stringify acc))
      (fs/writeFileSync folded-path (js/JSON.stringify (js/Array.from folded))))
    (let [row-count (count (js/Object.keys acc))]
      (when (zero? row-count) (die! 1 "REFUSED: ledger files parsed to 0 rows."))
      ;; datalake-sync.py does `json.loads(p.read_text())` — ONE JSON array
      ;; per file, not NDJSON. The payload is the FULL current accumulator
      ;; every run (unchanged overwrite semantics) — only the BUILDING of
      ;; it is incremental (see docstring).
      (let [table-path (path/join out-dir "dns_resolution.json")
            spec-path (path/join out-dir "dns-resolver.spec.json")]
        (fs/writeFileSync table-path (js/JSON.stringify (js/Object.values acc)))
        (fs/writeFileSync spec-path
                          (js/JSON.stringify
                           (clj->js {:namespace "cloud_itonami"
                                     :tables [{:file "dns_resolution.json"
                                              :table "dns_resolution" :int_columns []}]})
                           nil 2))
        (println (str "export " (count files) " ledger files (" (count new-files) " newly folded) -> "
                     row-count " distinct rows -> " table-path))
        (let [loader (path/join superproject-root "scripts" "datalake-sync.py")]
          (when-not (fs/existsSync loader)
            (die! 2 (str "REFUSED: loader not found at " loader
                         " — pass --root <superproject checkout>")))
          (let [r (cp/spawnSync "python3" (clj->js [loader "--spec" spec-path "--in-dir" out-dir])
                                #js {:encoding "utf8" :stdio "inherit"})]
            (.exit js/process (or (.-status r) 1))))))))

(-main)
