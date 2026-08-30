#!/usr/bin/env nbb
(ns export-and-sync
  "data/ledger/**/*.edn -> NDJSON -> Cloudflare R2 Data Catalog (Apache
  Iceberg), reusing the ALREADY-GENERAL superproject loader
  (com-junkawasaki/root's scripts/datalake-sync.py — the same one
  com-junkawasaki/org-gleif-projections and app-hyakka's ASN/prefix data
  already land through, per 90-docs/adr/2608280100 and 2608280900).

  THIS IS A PROJECTION, NOT A PREMISE (CLAUDE.md 「消して再構築できるか」):
  data/ledger/**/*.edn is the source; this script dedups by :resolution/id
  (last file wins) and rebuilds `cloud_itonami.dns_resolution`. Since
  2026-08-29 the Iceberg side is kept current via `mode: upsert` (see
  point 6) rather than a full `t.overwrite(tbl)` every run.

  INCREMENTAL EXPORT, NOT re-derived from scratch every run — `--out-dir`
  persists checkpoint state between runs; each run parses only the ledger
  files not yet folded in, and (since point 6) sends only the rows Iceberg
  doesn't have yet, not the whole accumulator.

  TEN THINGS THAT LOOKED LIKE FIXES AND WEREN'T, all found live, in order
  (2026-08-28 unless noted):

  1. A first attempt kept the accumulator as a plain #js{} keyed by
     :resolution/id. Once that carries millions of dynamic string keys, V8
     drives it into slow dictionary-mode property storage — the run went
     1.5+ hours with RSS oscillating under heavy GC and never made forward
     progress. No error, no crash — just never finishing. A bigger heap
     ceiling would not have helped; the structure was wrong, not its size
     limit. Fixed by making the in-memory accumulator a real JS Map (a
     genuine hash table at any size) instead.

  2. With the Map fix, the SAME run then hit V8's max string length inside
     `JSON.stringify(Object.fromEntries(acc))` at ~1.4M accumulated rows
     during a mid-bootstrap checkpoint save. Building ONE JS string for
     the whole table cannot scale regardless of how the in-memory
     structure is shaped. Fixed by never materializing the whole table as
     a single string: `write-ndjson!`/`write-json-array!` below write in
     bounded batches via sequential `appendFileSync` calls.

  3. The write-side fix alone would still break on the NEXT run's *read*
     of the checkpoint file, once that file itself grows past the same
     string-length ceiling — `fs/readFileSync` decodes an entire file
     into one JS string exactly like `JSON.stringify` builds one. Loading
     streams the file line-by-line via `node:readline` instead.

  4. A bootstrap run got to 110/170 files (4.08M rows) before a genuine
     heap OOM (this one WAS a size-limit problem: the accumulator plus
     per-checkpoint batch buffers outgrew the default ~4.2GB heap — the
     wrapper dns-resolver-datalake-tick.cljs raised NODE_OPTIONS to
     --max-old-space-size). That crash exposed a SEPARATE bug:
     `write-ndjson!`/`write-json-array!` truncated the checkpoint path in
     place before re-appending it — a crash mid-write didn't just lose
     the checkpoint in progress, it destroyed the PREVIOUS good
     checkpoint too. Fixed by writing to `<path>.tmp` and
     `fs/renameSync`-ing into place only once the write fully succeeds.

  5. (2026-08-29) With 1-4 fixed and the heap raised to 16GB, the run
     crashed again with `Error: Map maximum size exceeded` — NOT a heap
     problem this time. Measured live: the crash landed at EXACTLY
     16,604,474 accumulated rows, i.e. right at V8's hard, undocumented-
     in-spec ceiling on the number of entries a single Map/Set can hold
     (2^24 = 16,777,216). No heap size fixes this — it is a structural
     cap on the data structure itself, independent of available memory.
     `NODE_OPTIONS=--max-old-space-size` bumps in dns-resolver-datalake-
     tick.cljs treated this as if it were case 4 again; it was not, and
     raising the heap further would never have gotten past it. Fixed by
     sharding the accumulator into `n-shards` (256) separate Maps, keyed
     by a hash of :resolution/id, each persisted to its own checkpoint
     file under `<out-dir>/accumulator-shards/`. At 256 shards, a single
     shard only approaches the 16.7M cap once the WHOLE table passes
     roughly 4 billion rows (assuming reasonably uniform hashing) — well
     past any realistic near-term target for this ledger. A one-time
     migration folds the pre-existing single `accumulator.ndjson`
     checkpoint (the 16.6M-row one that hit the cap) into the sharded
     layout on first run, so the ~384 already-folded ledger files do not
     need to be re-parsed from EDN.

  6. (2026-08-29) Even after the sharding fix, every run still sent the
     ENTIRE accumulator to `datalake-sync.py`, which did a full
     `t.overwrite(tbl)` of the whole Iceberg table every sync — measured
     live: 24 snapshots already accumulated on `cloud_itonami.
     dns_resolution` in ~24h of hourly syncs, no partition spec, all
     columns (including timestamps) typed as plain strings. None of that
     scales: I/O and compute per sync grow with the WHOLE table size, not
     with how much actually changed, and old snapshots pile up in R2
     forever since nothing ever expired them. Fixed on both sides of the
     JSON boundary:
       - `datalake-sync.py` gained `mode: upsert` (idempotent — a resent
         delta updates in place instead of duplicating; measured: a
         retried upsert of the same rows yields rows_updated=0
         rows_inserted=0), `partition_by` (Iceberg partition evolution,
         non-destructive — old files stay under the old spec), a
         `timestamp_columns` option (native `timestamp(us, UTC)` instead
         of string, so date-range queries can actually prune), and
         automatic snapshot expiration after every commit
         (`--snapshot-retention-days`, default 7, applied to every
         caller so LEI/watchlist/hyakka get the same cleanup without
         opting in).
       - This script now tracks a SECOND set of sharded checkpoint files
         under `<out-dir>/pending-delta-shards/` — rows `.set` into a
         shard for the FIRST time (`.has` was false) also go into the
         matching pending-delta shard. The Iceberg payload each run is
         ONLY that delta, sent with `mode: upsert, join_columns: [\"id\"]`.
         `datalake-sync.py` itself verifies the read-back row count
         before exiting 0; only on that confirmed success does this
         script clear the pending-delta shards. If the process dies
         between the commit succeeding and that clear landing, the next
         run resends the same delta — safe under upsert, NOT safe if this
         ever switched back to plain append (see the `mode` doc in
         datalake-sync.py). First run under this code bootstraps
         pending-delta as empty rather than replaying everything already
         in the accumulator: that is correct ONLY if the LAST pre-upsert
         run already completed a full overwrite, so the Iceberg table and
         the local accumulator agree as of that point. ⚠ Found live
         (2026-08-29): that pre-upsert overwrite run did NOT finish —
         it was mid-write on a ~23M-row `dns_resolution.json` when the
         whole machine's load average passed 220 (heavy swap thrashing
         from unrelated concurrent load, confirmed via `vm_stat`'s
         Swapins/Swapouts counters, not a bug in this script — the same
         write-in-bounded-batches code had synced 6.48M and 7.65M rows
         fine earlier under normal load) and had to be killed. Its
         accumulator-shards checkpoint was fully durable (safe to kill at
         that point — see points 4/5), but the empty-pending-delta
         bootstrap assumption was FALSE for this specific transition: the
         table was still at its old ~10.1M-row baseline, not the ~23M in
         the local accumulator. Recovered by manually seeding
         pending-delta-shards as a full copy of accumulator-shards before
         the first run of this point's code — i.e. \"treat everything as
         unsynced\" rather than \"nothing is unsynced\", which is always
         the safe direction to be wrong in (a spurious re-upsert of an
         already-current row is a harmless no-op; skipping a row that was
         never actually synced is silent data loss). This manual seed is
         a one-time recovery step, not part of normal operation — a
         clean transition (last overwrite completes normally) still
         bootstraps empty, correctly.

  7. (2026-08-29) Even chunked into a delta-only upsert payload, the sync
     step still sent the WHOLE delta as a single Iceberg commit — fine
     for a normal hourly delta (thousands of rows) but not for a one-time
     catch-up delta in the millions, which is exactly what the recovery
     in point 6 produced. Same root cause as point 6, one level up: an
     unbounded per-run payload was always going to hit a wall as either
     the ledger or the catch-up gap grew, this just made it concrete.
     Fixed by capping each Iceberg sync call at `chunk-row-budget`
     (1,000,000 rows) and looping (`sync-pending-in-chunks!`) — pending
     is walked shard-by-shard, taking entries until the budget is spent
     (`take-chunk`), and on a successful chunk commit only THOSE rows are
     removed from pending (`remove-taken!`) and the checkpoint is
     persisted immediately, before the next chunk starts. A crash between
     chunks loses at most the in-flight chunk's progress, not the whole
     catch-up — the same incremental-checkpoint discipline as the fold
     loop itself, just applied one layer up at the sync step.

  8. (2026-08-29) The point 6 recovery — seeding pending-delta-shards as a
     full copy of accumulator-shards so the interrupted overwrite's
     ~23M-row gap would still get synced — OOM'd on the very next run.
     Cause: `-main` unconditionally loaded BOTH `shards` (the accumulator)
     AND `pending` before doing anything else, and this time pending was
     (by the recovery seed) the same size as shards — nearly DOUBLING
     memory for a run whose new-files was empty and so never touched
     `shards` at all. A pure catch-up sync of an existing pending backlog
     has no reason to hold the accumulator in memory — it was only ever
     needed by the fold loop's `.has` dedup check. Fixed by computing
     `new-files` first (cheap: just `folded-files.json` vs the ledger
     directory listing, no Map loading) and branching in `-main`: load
     `shards` only if there is something to fold (`fold-then-sync!`);
     otherwise load only `pending` and go straight to `sync-only!`. Under
     normal operation this changes nothing (there is usually SOME new
     ledger file, so shards loads as before); it only matters for a large
     pending backlog with nothing new to fold — exactly the point 6
     recovery scenario, and exactly the case that OOM'd.

  9. (2026-08-29) Point 8's fix was necessary but not sufficient: on a
     live resident ledger, `new-files` is almost never actually empty (a
     new ledger file lands roughly every ~13 minutes) — so `fold-then-
     sync!`, not `sync-only!`, kept being the path taken, and it still
     held both `shards` and `pending` in scope through the ENTIRE
     chunked-sync loop even after the (small) fold step was done with
     `shards`. Whether V8 would actually keep 256 Maps' worth of memory
     reachable for the rest of that function's execution is an engine
     implementation detail (liveness analysis on stack-resident locals)
     not worth betting an OOM on. Fixed by explicitly nulling out every
     element of `shards` right after the fold step's last use of it,
     before calling into the sync step — this makes the row-Maps
     collectible on the next GC pass regardless of what the JIT would
     have inferred on its own.

  10. (2026-08-29) The loader is read from the SUPERPROJECT CHECKOUT at
     run time (`<--root>/scripts/datalake-sync.py`), so it can change
     underneath a long-running sync. It did: while a catch-up was
     mid-flight, that checkout was reverted to its pristine (pre-upsert)
     version for a moment during an unrelated landing step. The next
     chunk was written with `mode: upsert` in its spec and handed to a
     loader that had never heard of `mode` — which silently ignored it
     and ran its only code path, `t.overwrite(tbl)`. One 361,588-row
     chunk replaced the whole table: 13,778,525 rows deleted in a single
     commit. Recovered by rolling back to the pre-damage Iceberg snapshot
     (`manage_snapshots().rollback_to_snapshot(...)`, verified with a
     real full scan, not just metadata) — the projection was never the
     source of truth, but the loss would still have cost hours of resync.
     The signal was in the log the whole time and easy to miss: successful
     upserts print `committed (upsert) ... updated=N inserted=M`, that one
     printed bare `committed`.
     Fixed by making a caller/loader version mismatch fail CLOSED instead
     of degrading to overwrite: this script passes
     `--require-incremental-support`, which an old loader rejects with
     argparse's `unrecognized arguments` (exit 2), and the current loader
     independently REFUSES any spec asking for a non-overwrite `mode`
     when that flag is absent. Both directions were tested against an
     actual pre-change loader pulled from git.
     The general shape is one CLAUDE.md already names: an operation that
     could not do what was asked returned the same success-looking result
     as one that did. Silently falling back to the destructive default is
     the worst possible reading of an unknown option.

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

;; See docstring point 5. 256 keeps a single shard well clear of V8's
;; Map/Set entry cap (2^24) even at billions of total rows.
(def n-shards 256)

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

;; djb2-xor-variant string hash, masked to a byte at the end (n-shards is
;; exactly 256) so the shard index is always 0-255 regardless of the
;; sign JS's 32-bit bitwise ops leave `h` in — matches CLAUDE.md's own
;; discipline of not fabricating "close enough" numbers: this hash is
;; deterministic per id, which is the only property dedup needs.
(defn shard-of [id]
  (let [s (str id) len (count s)]
    (loop [i 0 h 5381]
      (if (< i len)
        (recur (inc i) (bit-xor (* h 33) (.charCodeAt s i)))
        (bit-and h 0xff)))))

(defn- shard-dir [out-dir] (path/join out-dir "accumulator-shards"))
;; Rows already `.set` into a shard but not yet confirmed committed to
;; Iceberg — see docstring point 6. A row lives here from the moment it's
;; first seen until a successful (read-back-verified) sync clears it.
(defn- pending-dir [out-dir] (path/join out-dir "pending-delta-shards"))
(defn- shard-path [base-dir i]
  (path/join base-dir (str "shard-" (.padStart (str i) 3 "0") ".ndjson")))

;; Never build a single JS string for a whole table — see docstring
;; point 2. `rows` may be any seqable collection of JS-encodable values.
;;
;; Write to `<path>.tmp` and rename into place at the end, not truncate-
;; and-append `path` directly — see docstring point 4. `fs/renameSync` on
;; the same filesystem is atomic, so a crash mid-write leaves the last
;; successful checkpoint completely untouched.
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

;; Streams a shard file line-by-line instead of fs/readFileSync-ing it
;; whole — see docstring point 3. `base-dir` is either (shard-dir out-dir)
;; or (pending-dir out-dir) — same file shape, two different directories.
(defn- load-one-shard [base-dir i]
  (js/Promise.
   (fn [resolve reject]
     (let [p (shard-path base-dir i)]
       (if-not (fs/existsSync p)
         (resolve (js/Map.))
         (let [m (js/Map.)
               rl (readline/createInterface
                   #js {:input (fs/createReadStream p) :crlfDelay js/Infinity})]
           (.on rl "line" (fn [line]
                            (when (pos? (count (str/trim line)))
                              (let [row (js/JSON.parse line)]
                                (.set m (aget row "id") row)))))
           (.on rl "close" (fn [] (resolve m)))
           (.on rl "error" reject)))))))

;; Sequential, not Promise.all over all 256 — one shard read at a time
;; keeps peak fd/memory usage flat regardless of n-shards. Each .then
;; runs on the microtask queue, so this does not grow the JS call stack
;; the way a synchronous recursive loop would.
(defn- load-shards-from [base-dir i acc]
  (if (>= i n-shards)
    (js/Promise.resolve acc)
    (-> (load-one-shard base-dir i)
        (.then (fn [m] (aset acc i m) (load-shards-from base-dir (inc i) acc))))))

(defn load-shards [base-dir]
  (fs/mkdirSync base-dir #js {:recursive true})
  (load-shards-from base-dir 0 (js/Array. n-shards)))

;; One-time repair for checkpoints written by the pre-sharding version of
;; this script (a single accumulator.ndjson — see docstring point 5,
;; that file is the one that hit the Map cap at 16,604,474 rows). Routes
;; every already-deduped row into its shard so the ledger files already
;; folded into it do not need to be re-parsed from EDN. No-op once the
;; sharded layout exists.
(defn migrate-legacy-accumulator! [out-dir]
  (let [legacy-path (path/join out-dir "accumulator.ndjson")]
    (if-not (and (fs/existsSync legacy-path) (not (fs/existsSync (shard-dir out-dir))))
      (js/Promise.resolve nil)
      (js/Promise.
       (fn [resolve reject]
         (println "MIGRATE\tsharding legacy accumulator.ndjson into" n-shards "shards")
         (fs/mkdirSync (shard-dir out-dir) #js {:recursive true})
         (let [shards (js/Array. n-shards)
               _ (dotimes [i n-shards] (aset shards i (js/Map.)))
               n (atom 0)
               rl (readline/createInterface
                   #js {:input (fs/createReadStream legacy-path) :crlfDelay js/Infinity})]
           (.on rl "line"
                (fn [line]
                  (when (pos? (count (str/trim line)))
                    (let [row (js/JSON.parse line)
                          id (aget row "id")]
                      (.set (aget shards (shard-of id)) id row)
                      (swap! n inc)))))
           (.on rl "close"
                (fn []
                  (dotimes [i n-shards]
                    (write-ndjson! (shard-path (shard-dir out-dir) i)
                                   (js/Array.from (.values (aget shards i)))))
                  (fs/renameSync legacy-path (str legacy-path ".migrated"))
                  (println "MIGRATE\tdone," @n "rows ->" (shard-dir out-dir)
                           "(old file kept as accumulator.ndjson.migrated)")
                  (resolve nil)))
           (.on rl "error" reject)))))))

(defn- write-shards! [base-dir shards]
  (dotimes [i n-shards]
    (write-ndjson! (shard-path base-dir i) (js/Array.from (.values (aget shards i))))))

(defn save-checkpoint! [out-dir folded-path shards pending folded]
  (write-shards! (shard-dir out-dir) shards)
  (write-shards! (pending-dir out-dir) pending)
  (fs/writeFileSync folded-path (js/JSON.stringify (js/Array.from folded))))

(defn- total-rows [shards] (reduce + (map #(.-size %) shards)))

;; Bounds the size of a SINGLE Iceberg sync payload — see docstring
;; point 7. Chosen well below the sizes that were fine under normal
;; system load (earlier successful runs synced 6.48M/7.65M rows in one
;; shot) so a chunk stays small even when the machine is under memory
;; pressure from unrelated concurrent work.
(def chunk-row-budget 1000000)

(defn- take-from-shard [entries n]
  "entries: JS array of [id row] pairs (from a Map's .entries()). Returns
  [rows ids] for the first n of them."
  (loop [j 0 rows (transient []) ids (transient [])]
    (if (>= j n)
      [(persistent! rows) (persistent! ids)]
      (let [pair (aget entries j)]
        (recur (inc j) (conj! rows (aget pair 1)) (conj! ids (aget pair 0)))))))

;; Walks shards in order, taking up to `budget` rows total. Returns
;; {:rows [...] :taken [[shard-idx id] ...]} — `taken` records exactly
;; which entries this chunk consumed so a successful sync can delete
;; precisely those, leaving the rest of each shard's pending rows for
;; the next chunk (a shard can be bigger than the whole chunk budget).
(defn- take-chunk [pending budget]
  (loop [idx 0 remaining budget all-rows [] all-taken []]
    (if (or (>= idx n-shards) (zero? remaining))
      {:rows all-rows :taken all-taken}
      (let [shard (aget pending idx)
            entries (js/Array.from (.entries shard))
            n (min remaining (.-length entries))]
        (if (zero? n)
          (recur (inc idx) remaining all-rows all-taken)
          (let [[shard-rows shard-ids] (take-from-shard entries n)
                shard-taken (mapv (fn [id] [idx id]) shard-ids)]
            (recur (inc idx)
                   (- remaining n)
                   (into all-rows shard-rows)
                   (into all-taken shard-taken))))))))

(defn- remove-taken! [pending taken]
  (doseq [[idx id] taken] (.delete (aget pending idx) id)))

;; One Iceberg sync call, capped at chunk-row-budget rows. On success,
;; removes exactly the rows it sent from `pending` (the caller persists
;; the checkpoint) — a crash between chunks loses at most the IN-FLIGHT
;; chunk's progress, not the whole catch-up.
(defn- sync-chunk! [pending out-dir loader]
  (let [{:keys [rows taken]} (take-chunk pending chunk-row-budget)
        table-path (path/join out-dir "dns_resolution.json")
        spec-path (path/join out-dir "dns-resolver.spec.json")]
    (write-json-array! table-path rows)
    (fs/writeFileSync spec-path
                      (js/JSON.stringify
                       (clj->js {:namespace "cloud_itonami"
                                 ;; NOT :timestamp_columns ["resolved_at"] — the table
                                 ;; already exists with resolved_at as string (created
                                 ;; before that spec option existed). pyiceberg's upsert
                                 ;; correctly REFUSES a string->timestamptz column type
                                 ;; mismatch rather than silently coercing (measured live
                                 ;; 2026-08-29 via catchup_sync.cljs: exit 1, "Mismatch in
                                 ;; fields"). Retrofitting would need a table recreation
                                 ;; (loses time-travel history) — deferred, not done here.
                                 :tables [{:file "dns_resolution.json"
                                          :table "dns_resolution"
                                          :int_columns []
                                          :partition_by ["source_date"]
                                          :mode "upsert"
                                          :join_columns ["id"]}]})
                       nil 2))
    (let [r (cp/spawnSync "python3" (clj->js [loader "--spec" spec-path "--in-dir" out-dir
                                                    "--require-incremental-support"])
                          #js {:encoding "utf8" :stdio "inherit"})
          ;; (.-status r) is nil when spawnSync couldn't even launch the
          ;; process (e.g. python3 missing) -- treat that as failure, not
          ;; as process.exit(nil)'s silent 0.
          status (or (.-status r) 1)]
      (when (zero? status) (remove-taken! pending taken))
      {:status status :n (count rows)})))

;; Loops sync-chunk! until `pending` is fully drained or a chunk fails.
;; Persists the pending checkpoint after EVERY chunk (not just at the
;; end) so forward progress survives a kill/crash mid-catch-up.
(defn- sync-pending-in-chunks! [pending out-dir loader]
  (loop [chunk-n 1]
    (if (zero? (total-rows pending))
      0
      (let [{:keys [status n]} (sync-chunk! pending out-dir loader)]
        (write-shards! (pending-dir out-dir) pending)
        (if (zero? status)
          (do (println (str "  chunk " chunk-n " synced " n " rows, "
                           (total-rows pending) " pending remain"))
              (recur (inc chunk-n)))
          (do (println (str "  chunk " chunk-n " FAILED (exit " status "), "
                           (total-rows pending) " rows still pending"))
              status))))))

;; Only the sync half — used when there is nothing new to fold (see
;; docstring point 8). Does NOT take `shards`: a pure catch-up sync of an
;; existing pending backlog has no reason to hold the accumulator in
;; memory at all, and holding both was exactly what caused the OOM this
;; point documents.
(defn sync-only! [pending out-dir loader]
  (let [delta-count (total-rows pending)]
    (if (zero? delta-count)
      (do (println "nothing pending, nothing to sync")
          (.exit js/process 0))
      (do (println (str delta-count " rows pending sync, syncing in chunks of " chunk-row-budget))
          (.exit js/process (sync-pending-in-chunks! pending out-dir loader))))))

(defn fold-then-sync! [shards pending folded out-dir folded-path files new-files loader]
  (let [started (.now js/Date)]
    (doseq [[i f] (map-indexed vector new-files)]
      (doseq [r (edn/read-string (fs/readFileSync f "utf8"))]
        (let [id (:resolution/id r)
              idx (shard-of id)
              shard (aget shards idx)
              js-row (clj->js (row->json r))]
          ;; New-to-the-global-accumulator, ever — not new-to-this-run.
          ;; This is exactly the set of rows Iceberg does not have yet
          ;; (see docstring point 6): anything already in `shard` was
          ;; already part of some earlier successful sync's payload.
          (when-not (.has shard id)
            (.set (aget pending idx) id js-row))
          (.set shard id js-row)))
      (.add folded f)
      ;; Checkpoint every 10 files, not only at the end — a bootstrap over
      ;; the whole ledger can run long; a kill/crash mid-run should lose
      ;; at most 10 files of already-done work, not all of it.
      (when (zero? (mod (inc i) 10))
        (save-checkpoint! out-dir folded-path shards pending folded)
        (println (str "  folded " (inc i) "/" (count new-files)
                     " (" (int (/ (- (.now js/Date) started) 1000)) "s elapsed, "
                     (total-rows shards) " distinct rows so far, "
                     (total-rows pending) " pending sync)"))))
    (when (seq new-files) (save-checkpoint! out-dir folded-path shards pending folded))
    (let [row-count (total-rows shards)]
      (when (zero? row-count) (die! 1 "REFUSED: ledger files parsed to 0 rows."))
      (println (str "export " (count files) " ledger files (" (count new-files)
                   " newly folded) -> " row-count " distinct rows total"))
      ;; Drop every shard's Map before syncing — see docstring point 9.
      ;; The sync step below only needs `pending`; leaving `shards`
      ;; referenced (even unused) for the whole chunked-sync loop would
      ;; keep ~23M row-objects reachable, right where point 8's OOM
      ;; happened. Clearing the array elements (not just letting the
      ;; local binding go out of scope) makes the Maps collectible on
      ;; the very next GC pass regardless of whether V8 does liveness
      ;; analysis on this stack frame.
      (dotimes [i n-shards] (aset shards i nil))
      (sync-only! pending out-dir loader))))

(defn -main []
  (let [files (ledger-files ledger-dir)
        _ (when (empty? files) (die! 2 "REFUSED: 0 ledger files — an unread tree is not an empty dataset."))
        _ (fs/mkdirSync out-dir #js {:recursive true})
        folded-path (path/join out-dir "folded-files.json")
        folded (js/Set. (read-json folded-path #js []))
        new-files (remove #(.has folded %) files)
        loader (path/join superproject-root "scripts" "datalake-sync.py")]
    (when-not (fs/existsSync loader)
      (die! 2 (str "REFUSED: loader not found at " loader " — pass --root <superproject checkout>")))
    (println (str "LEDGER\t" (count files) " files\tfolded=" (- (count files) (count new-files))
                 "\tnew=" (count new-files)))
    (-> (migrate-legacy-accumulator! out-dir)
        (.then (fn [_]
                 (if (seq new-files)
                   ;; Folding needs the accumulator, to dedup against and
                   ;; to detect which rows are genuinely new (see point 6).
                   (-> (load-shards (shard-dir out-dir))
                       (.then (fn [shards]
                                (-> (load-shards (pending-dir out-dir))
                                    (.then (fn [pending]
                                             (fold-then-sync! shards pending folded out-dir
                                                              folded-path files new-files loader)))))))
                   ;; Nothing new to fold — see docstring point 8, do not
                   ;; load the accumulator at all.
                   (-> (load-shards (pending-dir out-dir))
                       (.then (fn [pending] (sync-only! pending out-dir loader)))))))
        (.catch (fn [e] (die! 1 (or (.-stack e) (.-message e) (str e))))))))

(-main)
