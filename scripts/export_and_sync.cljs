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

  INCREMENTAL EXPORT, NOT re-derived from scratch every run — found live
  (2026-08-28) that re-reading and re-parsing the WHOLE historical ledger
  every tick (cljs.reader on ~1.6GB of accumulated EDN, ~7.5s per 10MB
  file) OOM'd the export step outright once the ledger had been running
  long enough. `--out-dir` persists `accumulator.ndjson` (one JSON row per
  line — the CURRENT deduped table state) and `folded-files.json` (which
  ledger files are already folded in) between runs — each run parses only
  the ledger files not yet in that set. First run against an empty/
  missing `--out-dir` bootstraps by treating every ledger file as new
  (same one-time cost as before, paid once instead of every ~13min tick).

  FOUR THINGS THAT LOOKED LIKE FIXES AND WEREN'T, all found live on the
  same 2026-08-28 incident, in order:

  1. A first attempt kept the accumulator as a plain #js{} keyed by
     :resolution/id. Once that carries millions of dynamic string keys,
     V8 drives it into slow dictionary-mode property storage — the run
     went 1.5+ hours with RSS oscillating under heavy GC and never made
     forward progress. No error, no crash — just never finishing. A
     bigger heap ceiling would not have helped; the structure was wrong,
     not its size limit. Fixed by making the in-memory accumulator a real
     JS Map (a genuine hash table at any size) instead.

  2. With the Map fix, the SAME run then hit V8's max string length
     inside `JSON.stringify(Object.fromEntries(acc))` at ~1.4M
     accumulated rows during a mid-bootstrap checkpoint save — nowhere
     near the ledger's eventual target size. Building ONE JS string for
     the whole table (whether via a plain-object round trip or an array)
     cannot scale regardless of how the in-memory structure is shaped.
     Fixed by never materializing the whole table as a single string:
     `write-ndjson!`/`write-json-array!` below write in bounded batches
     via sequential `appendFileSync` calls — total file size is
     unbounded, no individual write is.

  3. The write-side fix alone would still break on the NEXT run's
     *read* of `accumulator.ndjson`, once that file itself grows past
     the same string-length ceiling — `fs/readFileSync` decodes an
     entire file into one JS string exactly like `JSON.stringify` builds
     one. `load-accumulator` below streams the file line-by-line via
     `node:readline` instead, so loading is unbounded in the same way
     writing is. This is the one piece of this script that is
     inherently async (Node's streaming APIs are), so `-main` runs as a
     promise chain from the load onward — everything after the load
     stays synchronous, matching the rest of this script's style.

  4. With 1-3 fixed, a bootstrap run got to 110/170 files (4.08M rows)
     before a genuine heap OOM (this one WAS a size-limit problem, not a
     structural one — the accumulator itself, plus per-checkpoint batch
     buffers, outgrew the default ~4.2GB heap; the wrapper
     dns-resolver-datalake-tick.cljs now runs this with
     NODE_OPTIONS=--max-old-space-size=8192). That crash exposed a
     SEPARATE bug: `write-ndjson!`/`write-json-array!` truncated `path`
     in place (`writeFileSync path \"\"`) before re-appending it — a
     crash mid-write didn't just lose the checkpoint in progress, it
     destroyed the PREVIOUS good checkpoint too (found live: the file 110
     checkpoint had logged 4,082,939 rows, but the file on disk after the
     OOM held only 2,315,000 lines — the NEXT checkpoint attempt had
     already truncated it before dying mid-rewrite). Fixed by writing to
     `<path>.tmp` and `fs/renameSync`-ing into place only once the write
     fully succeeds; rename on the same filesystem is atomic, so a crash
     mid-write now leaves the last successful checkpoint untouched.

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
            ["node:path" :as path]
            ["node:readline" :as readline]))

(def argv (vec (drop 3 (js->clj (.-argv js/process)))))
(defn- flag [n] (let [i (.indexOf (clj->js argv) n)]
                  (when (and (>= i 0) (< (inc i) (count argv))) (nth argv (inc i)))))

(def superproject-root
  (or (flag "--root") (path/resolve (str (.cwd js/process)) ".." ".." "..")))
(def out-dir (or (flag "--out-dir") (path/join (.tmpdir os) "dns-resolver-lake")))
(def ledger-dir "data/ledger")
(def batch-size 5000)

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

;; Never build a single JS string for a whole table — see docstring
;; point 2. `rows` may be any seqable collection of JS-encodable values.
;;
;; Write to `<path>.tmp` and rename into place at the end, not truncate-
;; and-append `path` directly — found live (2026-08-28, same incident):
;; an in-place truncate-then-append checkpoint write that gets killed
;; (OOM, kill -9, crash) mid-write doesn't just lose the NEW checkpoint,
;; it destroys the PREVIOUS good one (the file was already truncated to
;; empty before the crash). `fs/renameSync` on the same filesystem is
;; atomic, so a crash mid-write leaves the last successful checkpoint
;; completely untouched and only a stray `.tmp` file to clean up.
(defn write-ndjson! [path rows]
  (let [tmp (str path ".tmp")]
    (fs/writeFileSync tmp "")
    (doseq [batch (partition-all batch-size rows)]
      (fs/appendFileSync tmp (str (str/join "\n" (map js/JSON.stringify batch)) "\n")))
    (fs/renameSync tmp path)))

(defn write-json-array! [path rows]
  (let [tmp (str path ".tmp")]
    (fs/writeFileSync tmp "[")
    (let [first-batch? (atom true)]
      (doseq [batch (partition-all batch-size rows)]
        (when-not @first-batch? (fs/appendFileSync tmp ","))
        (reset! first-batch? false)
        (fs/appendFileSync tmp (str/join "," (map js/JSON.stringify batch)))))
    (fs/appendFileSync tmp "]")
    (fs/renameSync tmp path)))

;; Streams accumulator.ndjson line-by-line instead of fs/readFileSync-ing
;; it whole — see docstring point 3. Returns a Promise<Map>.
(defn load-accumulator [path]
  (js/Promise.
   (fn [resolve reject]
     (if-not (fs/existsSync path)
       (resolve (js/Map.))
       (let [m (js/Map.)
             rl (readline/createInterface
                 #js {:input (fs/createReadStream path) :crlfDelay js/Infinity})]
         (.on rl "line" (fn [line]
                          (when (pos? (count (str/trim line)))
                            (let [row (js/JSON.parse line)]
                              (.set m (aget row "id") row)))))
         (.on rl "close" (fn [] (resolve m)))
         (.on rl "error" reject))))))

(defn save-checkpoint! [acc-path folded-path acc folded]
  (write-ndjson! acc-path (js/Array.from (.values acc)))
  (fs/writeFileSync folded-path (js/JSON.stringify (js/Array.from folded))))

(defn fold-and-sync! [acc folded acc-path folded-path files new-files]
  (let [started (.now js/Date)]
    (doseq [[i f] (map-indexed vector new-files)]
      (doseq [r (edn/read-string (fs/readFileSync f "utf8"))]
        (.set acc (:resolution/id r) (clj->js (row->json r))))
      (.add folded f)
      ;; Checkpoint every 10 files, not only at the end — a bootstrap over
      ;; the whole ledger can run long; a kill/crash mid-run should lose
      ;; at most 10 files of already-done work, not all of it.
      (when (zero? (mod (inc i) 10))
        (save-checkpoint! acc-path folded-path acc folded)
        (println (str "  folded " (inc i) "/" (count new-files)
                     " (" (int (/ (- (.now js/Date) started) 1000)) "s elapsed, "
                     (.-size acc) " distinct rows so far)"))))
    (when (seq new-files) (save-checkpoint! acc-path folded-path acc folded))
    (let [row-count (.-size acc)]
      (when (zero? row-count) (die! 1 "REFUSED: ledger files parsed to 0 rows."))
      ;; datalake-sync.py does `json.loads(p.read_text())` — ONE JSON
      ;; array per file (not NDJSON), written in bounded batches (see
      ;; write-json-array!) so this step doesn't reintroduce the same
      ;; single-string ceiling on the way out. Payload is the FULL current
      ;; accumulator every run (unchanged overwrite semantics); only how
      ;; it gets BUILT is incremental.
      (let [table-path (path/join out-dir "dns_resolution.json")
            spec-path (path/join out-dir "dns-resolver.spec.json")]
        (write-json-array! table-path (js/Array.from (.values acc)))
        (fs/writeFileSync spec-path
                          (js/JSON.stringify
                           (clj->js {:namespace "cloud_itonami"
                                     :tables [{:file "dns_resolution.json"
                                              :table "dns_resolution" :int_columns []}]})
                           nil 2))
        (println (str "export " (count files) " ledger files (" (count new-files)
                     " newly folded) -> " row-count " distinct rows -> " table-path))
        (let [loader (path/join superproject-root "scripts" "datalake-sync.py")]
          (when-not (fs/existsSync loader)
            (die! 2 (str "REFUSED: loader not found at " loader
                         " — pass --root <superproject checkout>")))
          (let [r (cp/spawnSync "python3" (clj->js [loader "--spec" spec-path "--in-dir" out-dir])
                                #js {:encoding "utf8" :stdio "inherit"})]
            (.exit js/process (or (.-status r) 1))))))))

(defn -main []
  (let [files (ledger-files ledger-dir)
        _ (when (empty? files) (die! 2 "REFUSED: 0 ledger files — an unread tree is not an empty dataset."))
        _ (fs/mkdirSync out-dir #js {:recursive true})
        acc-path (path/join out-dir "accumulator.ndjson")
        folded-path (path/join out-dir "folded-files.json")]
    (-> (load-accumulator acc-path)
        (.then (fn [acc]
                 (let [folded (js/Set. (read-json folded-path #js []))
                       new-files (remove #(.has folded %) files)]
                   (println (str "LEDGER\t" (count files) " files\tfolded="
                                (- (count files) (count new-files))
                                "\tnew=" (count new-files)))
                   (fold-and-sync! acc folded acc-path folded-path files new-files))))
        (.catch (fn [e] (die! 1 (or (.-stack e) (.-message e) (str e))))))))

(-main)
