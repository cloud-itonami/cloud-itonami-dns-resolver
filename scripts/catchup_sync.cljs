#!/usr/bin/env nbb
(ns catchup-sync
  "ONE-TIME streaming catch-up: syncs accumulator-shards/*.ndjson (the
  full deduped DNS resolution accumulator, written by export_and_sync.cljs)
  to Iceberg in small batches — a handful of shard files at a time —
  without ever loading more than that handful into memory at once.

  WHY THIS EXISTS SEPARATELY FROM export_and_sync.cljs: that script's
  pending-delta machinery is sized for ONGOING small hourly deltas (a
  few thousand newly-resolved rows), not a multi-million-row historical
  backlog. On 2026-08-29, after the accumulator grew to ~23.4M rows
  while an interrupted pre-upsert overwrite left Iceberg stuck at its
  old ~10.1M-row baseline, the recovery attempt seeded pending-delta-
  shards as a full copy of the accumulator so the gap would still sync.
  That seed meant a completely ordinary run (35 new ledger files to
  fold) needed BOTH the ~23M-row accumulator AND a ~23M-row pending
  copy in memory simultaneously — the exact doubling export_and_sync.
  cljs's docstring points 8/9 already fixed for the case of an EMPTY
  fold, but the machine's memory ceiling (16GB heap) was still crossed
  twice more on runs that DID have new files to fold: the OOM happened
  during the initial dual-load, before point 9's post-fold shard-drop
  could even run.

  Reading this as \"the design still isn't lean enough\" rather than
  \"raise the heap again\" (see export_and_sync.cljs's own point 5 about
  that exact mistake): a multi-million-row historical backlog is not the
  same *kind* of problem as an ongoing small delta, and forcing both
  through the same in-memory-Map code path was the actual bug. This
  script handles the ONE-TIME backlog by streaming shard files in and
  out of memory in small batches; export_and_sync.cljs goes back to
  tracking only genuinely-new, naturally-small deltas in pending-delta
  (which starts EMPTY again after this script brings Iceberg current).

  Progress is checkpointed (`catchup-progress.json`: next shard index to
  send) so a kill/crash resumes instead of re-sending already-synced
  shards — though a resend would be SAFE anyway (upsert is idempotent,
  same property export_and_sync.cljs relies on), this just avoids
  wasted work.

  Run once:
    nbb scripts/catchup_sync.cljs [--out-dir <dir>] [--root <superproject>]
                                  [--shards-per-batch N (default 4)]"
  (:require [clojure.string :as str]
            ["node:fs" :as fs]
            ["node:path" :as path]
            ["node:os" :as os]
            ["node:child_process" :as cp]
            ["node:readline" :as readline]))

(def argv (vec (drop 3 (js->clj (.-argv js/process)))))
(defn- flag [n] (let [i (.indexOf (clj->js argv) n)]
                  (when (and (>= i 0) (< (inc i) (count argv))) (nth argv (inc i)))))

(def superproject-root
  (or (flag "--root") (path/resolve (str (.cwd js/process)) ".." ".." "..")))
(def out-dir (or (flag "--out-dir") (str (os/homedir) "/.gftd/dns-resolver-lake")))
(def shard-dir (path/join out-dir "accumulator-shards"))
(def n-shards 256)
(def shards-per-batch (js/parseInt (or (flag "--shards-per-batch") "4")))
(def batch-write-size 5000)
(def progress-path (path/join out-dir "catchup-progress.json"))

;; The SAME lock dns-resolver-datalake-tick.cljs claims. Without it the
;; hourly launchd job runs export_and_sync.cljs against the same Iceberg
;; table while this catch-up is mid-flight, and both writers race on the
;; catalog's `main` branch pointer. Measured live 2026-08-29 at shard
;; 60/256: `CommitFailedException: CatalogCommitConflicts ... Branch or
;; tag `main`'s snapshot has changed`. Holding the lock makes the hourly
;; job refuse (exit 2) for the duration instead — it has nothing urgent
;; to do while a catch-up is running anyway, since the catch-up is
;; syncing a superset of what it would send.
(def lock-path (str (os/homedir) "/.gftd/locks/dns-resolver-datalake.lock"))

;; Even holding the lock, a commit can still lose a race (another
;; machine, a manually-started run, a lock reclaimed from a PID that
;; looked dead). A conflict is transient by nature: the table moved, so
;; re-reading and re-upserting the same rows succeeds. Retries are safe
;; precisely because the mode is upsert (idempotent) -- see
;; datalake-sync.py's `mode` docs.
(def max-commit-attempts 4)

(defn alive? [pid]
  (if-not (and (number? pid) (pos? pid))
    true
    (try (.kill js/process pid 0) true
         (catch :default e (not= "ESRCH" (.-code e))))))

(defn release-lock! []
  ;; Only drop the lock if it is still OURS -- a lock reclaimed by
  ;; another process after we were declared dead must not be deleted by
  ;; our own exit path.
  (try
    (when (= (str (.-pid js/process)) (str/trim (fs/readFileSync lock-path "utf8")))
      (fs/rmSync lock-path #js {:force true}))
    (catch :default _ nil)))

(defn exit! [code] (release-lock!) (.exit js/process code))

(defn die! [code msg] (js/console.error msg) (exit! code))

(defn claim-lock-or-die! []
  (when (fs/existsSync lock-path)
    (let [pid (js/parseInt (str/trim (fs/readFileSync lock-path "utf8")))]
      (if (alive? pid)
        (do (println (str "REFUSED: lock held by live pid " pid " at " lock-path))
            (.exit js/process 2))
        (do (println (str "reclaiming lock from dead pid " pid))
            (fs/rmSync lock-path #js {:force true})))))
  (fs/mkdirSync (path/dirname lock-path) #js {:recursive true})
  (fs/writeFileSync lock-path (str (.-pid js/process))))

(defn- shard-path [i]
  (path/join shard-dir (str "shard-" (.padStart (str i) 3 "0") ".ndjson")))

(defn write-json-array! [path rows]
  (let [tmp (str path ".tmp")]
    (fs/writeFileSync tmp "[")
    (let [first-batch? (atom true)]
      (doseq [batch (partition-all batch-write-size rows)]
        (when-not @first-batch? (fs/appendFileSync tmp ","))
        (reset! first-batch? false)
        (fs/appendFileSync tmp (str/join "," (map js/JSON.stringify batch)))))
    (fs/appendFileSync tmp "]")
    (fs/renameSync tmp path)))

;; Streams one shard file's rows in — small (a shard averages tens of
;; thousands of rows, not millions), never held alongside the other 255.
(defn- read-shard [i]
  (js/Promise.
   (fn [resolve reject]
     (let [p (shard-path i)]
       (if-not (fs/existsSync p)
         (resolve [])
         (let [rows (atom (transient []))
               rl (readline/createInterface
                   #js {:input (fs/createReadStream p) :crlfDelay js/Infinity})]
           (.on rl "line" (fn [line]
                            (when (pos? (count (str/trim line)))
                              (swap! rows conj! (js/JSON.parse line)))))
           (.on rl "close" (fn [] (resolve (persistent! @rows))))
           (.on rl "error" reject)))))))

;; -> Promise<js-array-of-rows> for shard indices [start, end), read one
;; shard at a time and concatenated — bounded by shards-per-batch shards'
;; worth of rows at once, never the whole accumulator.
(defn- read-batch [start end]
  (loop [i start acc (js/Promise.resolve [])]
    (if (>= i end)
      acc
      (recur (inc i)
             (.then acc (fn [rows]
                          (.then (read-shard i) (fn [more] (concat rows more)))))))))

(defn read-state []
  (try (js->clj (js/JSON.parse (fs/readFileSync progress-path "utf8")) :keywordize-keys true)
       (catch :default _ nil)))

;; One `datalake-sync.py` invocation, retried on failure. The failure
;; this exists for is CatalogCommitConflicts: two writers raced on the
;; table's `main` branch pointer and this one lost. That is transient --
;; the retry re-reads the moved table and re-applies the same upsert.
;; Retrying is only safe because the mode is upsert (idempotent): a
;; retry after a commit that actually DID land is a no-op
;; (rows_updated=0 rows_inserted=0), not a duplicate.
;;
;; Sleeps between attempts via `sleep(1)` rather than a JS timer on
;; purpose: everything from here down is deliberately synchronous (only
;; the shard reads are async), and a real subprocess sleep keeps it that
;; way without restructuring the chain.
(defn- sync-with-retry! [loader spec-path]
  (loop [attempt 1]
    (let [r (cp/spawnSync "python3" (clj->js [loader "--spec" spec-path "--in-dir" out-dir
                                                    "--require-incremental-support"])
                          #js {:encoding "utf8" :stdio "inherit"})
          status (or (.-status r) 1)]
      (cond
        (zero? status) 0
        (>= attempt max-commit-attempts)
        (do (println (str "  giving up after " attempt " attempts (exit " status ")"))
            status)
        :else
        (do (println (str "  attempt " attempt " failed (exit " status "), retrying in "
                         (* attempt 5) "s"))
            (cp/spawnSync "sleep" (clj->js [(str (* attempt 5))]))
            (recur (inc attempt)))))))

;; Recursive promise chain (not loop/recur — this crosses async
;; boundaries) over shard-index batches, matching export_and_sync.cljs's
;; own style for the same reason: streaming reads are the only genuinely
;; async part, everything else stays plain synchronous code.
(defn- run-from [i loader]
  (if (>= i n-shards)
    (do (fs/rmSync progress-path #js {:force true})
        (println "CATCHUP\tdone — all shards synced, progress file cleared")
        (exit! 0))
    (let [end (min n-shards (+ i shards-per-batch))]
      (-> (read-batch i end)
          (.then
           (fn [rows]
             (if (empty? rows)
               (do (fs/writeFileSync progress-path (js/JSON.stringify (clj->js {:next-shard end})))
                   (println (str "CATCHUP\tshards " i "-" (dec end) " empty, skipping"))
                   (run-from end loader))
               (let [table-path (path/join out-dir "dns_resolution.json")
                     spec-path (path/join out-dir "dns-resolver.spec.json")]
                 (write-json-array! table-path rows)
                 (fs/writeFileSync spec-path
                                   (js/JSON.stringify
                                    (clj->js {:namespace "cloud_itonami"
                                              ;; NOT :timestamp_columns ["resolved_at"] —
                                              ;; the table already exists with resolved_at
                                              ;; as string (created before that spec option
                                              ;; existed). pyiceberg's upsert correctly
                                              ;; REFUSES a string->timestamptz column type
                                              ;; mismatch rather than silently coercing
                                              ;; (measured live 2026-08-29: exit 1,
                                              ;; "Mismatch in fields"). Retrofitting would
                                              ;; need a table recreation (loses time-travel
                                              ;; history) — deferred, not done implicitly.
                                              :tables [{:file "dns_resolution.json"
                                                       :table "dns_resolution"
                                                       :int_columns []
                                                       :partition_by ["source_date"]
                                                       :mode "upsert"
                                                       :join_columns ["id"]}]})
                                    nil 2))
                 (let [status (sync-with-retry! loader spec-path)]
                   (if (zero? status)
                     (do (fs/writeFileSync progress-path (js/JSON.stringify (clj->js {:next-shard end})))
                         (println (str "CATCHUP\tshards " i "-" (dec end) " synced ("
                                      (count rows) " rows), next=" end "/" n-shards))
                         (run-from end loader))
                     (do (println (str "CATCHUP\tFAILED at shards " i "-" (dec end) " (exit " status ")"))
                         (exit! status))))))))
          (.catch (fn [e] (die! 1 (or (.-stack e) (.-message e) (str e))))))) ))

(defn -main []
  (when-not (fs/existsSync shard-dir)
    (js/console.error (str "REFUSED: no accumulator at " shard-dir))
    (.exit js/process 2))
  ;; Claimed BEFORE any work so the hourly launchd job (which claims the
  ;; same lock) refuses for the duration -- see lock-path's comment.
  (claim-lock-or-die!)
  (let [loader (path/join superproject-root "scripts" "datalake-sync.py")]
    (when-not (fs/existsSync loader)
      (die! 2 (str "REFUSED: loader not found at " loader " — pass --root <superproject checkout>")))
    (let [start (or (:next-shard (read-state)) 0)]
      (println (str "CATCHUP\tstarting at shard " start "/" n-shards
                   " (" shards-per-batch " shards/batch)"))
      (run-from start loader))))

(-main)
