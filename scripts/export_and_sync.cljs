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
  `datalake-sync.py` always does a full `t.overwrite(tbl)` (no --replace
  flag exists on it — that flag belongs to the OTHER writer,
  `scripts/datalake/iceberg_writer.py`, which this script does not use),
  so every run here is already a full rebuild from the ledger, exactly
  like hyakka-datalake-tick.cljs's use of the same loader for the same
  reason (a growing ledger, one droppable/rebuildable table).

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

(defn -main []
  (let [files (ledger-files ledger-dir)
        _ (when (empty? files) (die! 2 "REFUSED: 0 ledger files — an unread tree is not an empty dataset."))
        rows (reduce (fn [acc f]
                       (reduce (fn [acc2 r] (assoc acc2 (:resolution/id r) r))
                              acc (edn/read-string (fs/readFileSync f "utf8"))))
                     {} files)
        rows (vec (vals rows))]
    (when (empty? rows) (die! 1 "REFUSED: ledger files parsed to 0 rows."))
    (fs/mkdirSync out-dir #js {:recursive true})
    ;; datalake-sync.py does `json.loads(p.read_text())` — ONE JSON array per
    ;; file, not NDJSON (that line-delimited shape belongs to the OTHER
    ;; loader, scripts/datalake/iceberg_writer.py, which this script does
    ;; not use).
    (let [table-path (path/join out-dir "dns_resolution.json")
          spec-path (path/join out-dir "dns-resolver.spec.json")]
      (fs/writeFileSync table-path (js/JSON.stringify (clj->js (mapv row->json rows))))
      (fs/writeFileSync spec-path
                        (js/JSON.stringify
                         (clj->js {:namespace "cloud_itonami"
                                   :tables [{:file "dns_resolution.json"
                                            :table "dns_resolution" :int_columns []}]})
                         nil 2))
      (println (str "export " (count files) " ledger files -> " (count rows) " rows -> " table-path))
      (let [loader (path/join superproject-root "scripts" "datalake-sync.py")]
        (when-not (fs/existsSync loader)
          (die! 2 (str "REFUSED: loader not found at " loader
                       " — pass --root <superproject checkout>")))
        (let [r (cp/spawnSync "python3" (clj->js [loader "--spec" spec-path "--in-dir" out-dir])
                              #js {:encoding "utf8" :stdio "inherit"})]
          (.exit js/process (or (.-status r) 1)))))))

(-main)
