# Operator quickstart

Every command below was **actually run** against this repo on
2026-09-01 (UTC), nbb v1.5.212 / node v26.7.0, and the output shown is the
output it produced. Where a step was *not* walked, this document says so in
that step rather than describing it as if it had been. The last section
lists everything left unwalked in one place.

Design rationale lives in the [README](../README.md) and in the
superproject ADR `90-docs/adr/2608280900-world-scale-dns-domain-collection.edn`.
This file is the *operating* half: what to type, what you should see back,
and which failures look like something they are not.

---

## 0. Orientation in 60 seconds

This repo does one thing on a timer: take the next N domains off a list,
ask two public DNS-over-HTTPS resolvers for their A/AAAA/MX/NS records, and
append the answers to a dated EDN ledger under `data/ledger/`. A second,
separate script folds that ledger into a Cloudflare R2 Data Catalog
(Iceberg) table. The ledger is the source of truth; the Iceberg table is a
rebuildable projection.

Three moving parts, and it is worth knowing which one you are touching:

| | what it is | where its state lives |
|---|---|---|
| `scripts/resolve_tick.cljs` | one bounded tick: list slice → DoH → ledger | `data/state-<source>.edn` (gitignored, per-checkout) |
| `scripts/refresh_cc_domains.cljs` | one-time Common Crawl domain-list download | `data/cache/cc-domains.txt` (gitignored) |
| `scripts/export_and_sync.cljs` | ledger → Iceberg | `--out-dir` checkpoint |

There is no LLM and no Governor here. It is a deterministic collector
behind an operator gate.

## 1. Preflight

```sh
git clone git@github.com:cloud-itonami/cloud-itonami-dns-resolver
cd cloud-itonami-dns-resolver
```

`nbb` is the only runtime dependency for ticking (`npx nbb` works; this
repo has no `deps.edn` and needs no JVM). The export step additionally
needs Python and the superproject checkout — see step 7.

**Do not do your work in a `git worktree add` worktree.** The ledger is
git-annex-backed and annexed files are relative symlinks of the form
`../../../.git/annex/objects/...`, which require `.git` to be a real
directory. In a linked worktree `.git` is a *file*, so every annexed path
resolves to `ENOTDIR`. This is the root cause of the confusing error in
step 7. Verified 2026-09-01 in both directions:

```sh
$ ls -ld .git          # in a linked worktree
-rw-r--r--  1 …  133 …  .git          # ← a file. annex symlinks cannot resolve.

$ ls -ld .git          # in a normal clone
drwxr-xr-x 13 …  416 …  .git          # ← a directory. they can.
```

## 2. Prove the gate is closed before you open it

`GATE-DNS_RESOLVER` is offline-by-default and needs **both** `--live` and
`DNS_RESOLVER_OPERATOR_GATE=open`. Confirm both arms refuse before you
authorize anything:

```sh
$ nbb --classpath src scripts/resolve_tick.cljs
REFUSED: --live not set (GATE-DNS_RESOLVER, offline-default). No network call made.

$ nbb --classpath src scripts/resolve_tick.cljs --live
REFUSED: DNS_RESOLVER_OPERATOR_GATE is not "open" — --live alone does not authorize a live pull.
```

`scripts/refresh_cc_domains.cljs` shares this one gate and refuses
identically.

> **⚠ The two refusals do not return the same exit code.** Measured
> 2026-09-01: the missing-`--live` arm exits **0**, the missing-env-gate arm
> exits **2**. So a wrapper that only checks `$?` cannot tell "refused to
> run" from "ran and collected nothing" on the first arm — the failure
> mode CLAUDE.md's 検査を書く前の6問 #2 names, and the same one this
> repo's own README invokes when it explains why a dead domain still gets a
> `record_type = "NONE"` row.
>
> **Check the output line, not the exit code.** A real tick always prints a
> `tick <id> …` summary line; a refusal never does. The resident runner
> happens not to be exposed to this, because it hardcodes `--live` and sets
> the env var itself, and re-checks `git status -- data/ledger` afterwards —
> but that is the runner being belt-and-braces, not the exit code being
> trustworthy.

## 3. Run the tests

Pure functions only — list parsing and row shaping. No network, no gate.

```sh
$ nbb --classpath src test/resolve_tick_test.cljs

Testing resolve-tick-test

Ran 3 tests containing 19 assertions.
0 failures, 0 errors.
```

## 4. One bounded live tick

This makes real requests to `dns.google` and `cloudflare-dns.com`. Start
small: the defaults (`--n 200` for tranco) are sized for the resident
timer, not for a human checking that the thing works.

```sh
$ DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/resolve_tick.cljs \
    --live --n 25 --concurrency 10 --max-duration-sec 60
tranco-top-1m list-id=785430c12d60 total=1000000 cursor=0 slice=25
ledger data/ledger/2026-09-01/2026-09-01T20-07-39-447Z-785430c1.edn rows=237 domains=25
tick 2026-09-01T20-07-39-447Z-785430c1 files=1 resolved=25/25 fetched (n=25) next-cursor=25/1000000
```

How to read that third line:

- `resolved=25/25 fetched` — every domain fetched from the list slice was
  resolved. If the `--max-duration-sec` budget expires first you get e.g.
  `resolved=118/200`, and the cursor advances by **118**, not 200. A
  time-budget stop never skips domains.
- `next-cursor=25/1000000` — where the next tick will start.
- `files=1` — one ledger file. Large ticks are split into
  `…-part1.edn`, `…-part2.edn` … so no single file exceeds GitHub's limit.

The ledger directory is named by **UTC** date. The run above happened at
05:07 JST on 2026-09-02 and landed in `data/ledger/2026-09-01/`. If you go
looking for today's files under today's *local* date you may not find them.

## 5. Verify what the tick actually wrote

Do not trust the summary line alone — read the file back with a reader,
not with `grep`:

```sh
$ nbb --classpath src -e '
(ns v (:require ["fs" :as fs] [clojure.edn :as edn]))
(def rows (edn/read-string (fs/readFileSync "data/ledger/2026-09-01/2026-09-01T20-07-39-447Z-785430c1.edn" "utf8")))
(println "rows=" (count rows))
(println "distinct domains=" (count (distinct (map :resolution/domain rows))))
(println "record types=" (sort (distinct (map :resolution/record-type rows))))'
rows= 237
distinct domains= 25
record types= (A AAAA MX NS)
```

One row per `(domain, record-type, answer)` triple, so 25 domains → 237
rows is normal. A domain that resolves nothing across all four types still
gets exactly one row with `record-type "NONE"` — "checked, found nothing"
and "never checked" must not look alike.

## 6. The cursor

`data/state-<source>.edn` is gitignored, so it is **per checkout** — a
fresh clone starts at cursor 0 and re-resolves from the top of the list.
That is not corruption; it is what "the ledger is the source of truth, the
cursor is just a bookmark" means in practice.

```sh
$ cat data/state-tranco.edn
{:version 1, :cursor 25, :source "tranco-top-1m", :last-tick-id "…", :last-tick-at "…", :list-total 1000000}
```

Running a second tick resumes from there rather than repeating work
(walked, 2026-09-01: `cursor 0 → 25 → 35`):

```sh
$ DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/resolve_tick.cljs \
    --live --n 10 --concurrency 10 --max-duration-sec 45
tranco-top-1m list-id=785430c12d60 total=1000000 cursor=25 slice=10
ledger data/ledger/2026-09-01/2026-09-01T20-08-14-783Z-785430c1.edn rows=89 domains=10
tick 2026-09-01T20-08-14-783Z-785430c1 files=1 resolved=10/10 fetched (n=10) next-cursor=35/1000000
```

To re-resolve from the top, delete the state file. To resume elsewhere,
edit `:cursor`. Nothing else reads it.

## 7. Reading historical ledger files — read this before you `cat` one

`data/ledger/**` is routed to **git-annex** (see `.gitattributes`), because
a single full-size tick measured 158 MB and plain git blobs at that rate
would make `.git` itself the unbounded thing. Only a small pointer lands in
git.

The consequence for an operator: **most ledger files in a fresh clone are
symlinks whose content is not present.** Measured from the git index at
commit `41aa0cb` (2026-09-01):

| ledger files tracked | mode `100644` (plain, readable anywhere) | mode `120000` (annex symlink) |
|---|---|---|
| 2,522 | 21 | 2,501 |

The resident runner appends to this continuously, so the totals move by the
hour — the ratio is the point, not the count. Re-measure at your own HEAD
rather than quoting the number above:

```sh
git ls-files -s data/ledger | awk '{print $1}' | sort | uniq -c
```

The 21 plain ones are the earliest, up to `2026-08-28T09:00:52Z`;
everything from `2026-08-28T09:14:04Z` onward is annexed. So `cat` on a
recent file fails, and the error tells you nothing useful:

```sh
$ head -c 120 data/ledger/2026-09-01/2026-09-01T19-53-25-760Z-cc-main--part9.edn
head: …: Not a directory          # ← in a linked worktree (step 1)
head: …: No such file or directory # ← in a normal clone, content not fetched
```

Two different messages, two different causes. `Not a directory` means you
are in a worktree and no `get` will ever help; `No such file or directory`
means the pointer is fine and the content simply has not been retrieved.

> **⚠ Retrieval is currently not possible from a fresh clone, and this is a
> real gap, not a step you are missing.** Verified 2026-09-01:
> `git ls-remote` on the GitHub remote returns exactly two refs — `HEAD`
> and `refs/heads/main`. There is **no `git-annex` branch published**, and
> the shared checkout has no B2 special remote configured. git-annex keeps
> its "which remote holds this content" metadata on that branch, so without
> it a clone has 2,501 pointers and no way to resolve any of them. The
> content does exist — the resident runner pushes it to B2 with
> `datalad push --to b2` — but only its own worktree knows where.
>
> This is filed as a finding, not fixed here: closing it means publishing
> the `git-annex` branch and documenting B2 credential access, which
> touches the resident actor's push path and is out of scope for a docs
> change. Until it is closed, treat "the ledger is the source of truth" as
> true of the *repository*, not of any clone of it.

If you do have a checkout with the annex wired up (the resident's), the
normal commands are `git annex get <path>` or `datalad get <path>`. **This
document has not walked those** — see step 9.

## 8. Export to Iceberg

```sh
nbb --classpath src scripts/export_and_sync.cljs --root <path to com-junkawasaki/root>
```

Preconditions, all of which must hold — but note they are **not checked in
the order they are listed**, which matters when several are unmet at once:

1. `data/ledger/` is non-empty. Checked *first*, before anything else
   (walked 2026-09-01, exit **2**):

   ```sh
   $ nbb --classpath src scripts/export_and_sync.cljs --root /tmp/x
   REFUSED: 0 ledger files — an unread tree is not an empty dataset.
   ```

2. **The annexed ledger content is actually present locally** — see step 7.
   This is read next, while scanning the ledger, and it is where a scratch
   clone dies.
3. `--root` points at a `com-junkawasaki/root` checkout containing
   `scripts/datalake-sync.py`. Checked only *after* the ledger scan
   succeeds (walked 2026-09-01, exit **2**):

   ```sh
   $ nbb --classpath src scripts/export_and_sync.cljs --root /tmp/definitely-not-a-superproject
   REFUSED: loader not found at /tmp/definitely-not-a-superproject/scripts/datalake-sync.py — pass --root <superproject checkout>
   ```

4. `CF_CATALOG_TOKEN` in the environment (or Keychain `gftd.cf/API_TOKEN`).

Because precondition 2 is read before 3, a clone with unretrieved annex
content **never reaches the `--root` check** — you get a raw `stat` failure
instead of either refusal. Walked 2026-09-01 in this repo's own tree:

```sh
$ nbb --classpath src scripts/export_and_sync.cljs
----- Error --------------------------------------
Message:  ENOTDIR: not a directory, stat 'data/ledger/2026-08-29/…edn'
```

That is not a bug in the export logic and not a corrupt ledger — it is
`stat` on an unresolvable annex symlink, surfacing raw. It exits 1, so it
does not pass silently, but the message does not name its own cause, and it
masks the two well-worded refusals above. **If you see `ENOTDIR` or
`ENOENT` from a path under `data/ledger/`, go back to step 7 before
debugging anything else** — in particular, do not conclude your `--root` is
correct just because you were not told otherwise.

The remainder of this step — the actual upsert into
`cloud_itonami.dns_resolution` — **has not been walked here**: it writes to a
production Iceberg table, and precondition 2 blocks it from a scratch clone
regardless.

## 9. What this document has not walked

Stated explicitly so the unwalked parts are not mistaken for verified ones:

- `git annex get` / `datalad get` retrieval of ledger content — blocked on
  the unpublished `git-annex` branch (step 7). Not walked.
- `scripts/export_and_sync.cljs` past its precondition checks — would upsert
  a production table. Not walked.
- `scripts/refresh_cc_domains.cljs --live` — an ~893 MB download. Only its
  refusal path was walked (step 2).
- `--source commoncrawl` ticks — depend on that cache. Not walked.
- The `datalad push --to b2` transport in the resident runner. Not walked.

## Troubleshooting

| symptom | cause | what to do |
|---|---|---|
| `REFUSED: --live not set`, exit **0** | gate closed, arm 1 | add `--live`; do not read exit 0 as success (step 2) |
| `REFUSED: … GATE is not "open"`, exit 2 | gate closed, arm 2 | export `DNS_RESOLVER_OPERATOR_GATE=open` |
| `ENOTDIR … data/ledger/…` | annex symlink in a linked worktree | work in a normal clone (step 1) |
| `ENOENT … data/ledger/…` | annex content not retrieved | see step 7 — currently blocked |
| `REFUSED: loader not found at …`, exit 2 | `--root` missing or wrong | pass the superproject checkout (step 8) |
| `resolved=` far below `fetched=` | `--max-duration-sec` budget hit | expected; cursor advanced only by what resolved |
| ledger dir for "today" missing | directories are named by **UTC** | look one day back (step 4) |
| fresh clone re-resolves from rank 1 | `data/state-*.edn` is gitignored | expected; the ledger, not the cursor, is the record (step 6) |
