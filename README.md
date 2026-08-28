# cloud-itonami-dns-resolver

Bounded, G7-gated active DNS resolution against a slice of one of two free,
no-account-needed domain lists — [Tranco's daily top-1M](https://tranco-list.eu/)
or [Common Crawl's hyperlink web-graph domain list](https://commoncrawl.org/web-graphs)
(~120M domains, confirmed live 2026-08-28: `cc-main-2026-jun-jul-aug` carries
119,722,885) — landing in Cloudflare R2 Data Catalog (Apache Iceberg,
`cloud_itonami.dns_resolution`), not in any wiki claim graph. This repo does
not do enough on its own to be "SecurityTrails for the whole world"; it is
one honestly-scoped, free, legal piece of that ambition. See below for what
it actually covers and what it does not.

Design: `90-docs/adr/2608280900-world-scale-dns-domain-collection.edn`
(superproject `com-junkawasaki/root`).

## What this collects, and what it does not

| | in scope | out of scope |
|---|---|---|
| Data | A / AAAA / MX / NS records for domains on Tranco's top-1M or Common Crawl's domain-graph list | RDAP/WHOIS registrant data (name, org, address — see 2607309950's refusal of RDAP bulk search, which stands) |
| Coverage | **"the web-visible / linked-to world"** — domains Tranco sees as actively visited, or Common Crawl's crawler found linked from somewhere | Domains with **zero external footprint**: parked, dormant, brand-defensive registrations that nothing links to and no crawler has ever fetched. CT logs (yabai-actor), Tranco, and Common Crawl all share this blind spot — only a registry's own zone file (CZDS) sees these |
| Source | Free, no account: Tranco CSV, Common Crawl's public S3 web-graph object, Google's DNS-over-HTTPS resolver | Commercial passive-DNS feeds (Farsight/DNSDB, SecurityTrails' own API), AWS Athena over Common Crawl's columnar index — both would need a paid account this repo does not have (the plain web-graph download turned out to make Athena unnecessary for bulk domain enumeration specifically) |

**Scale mismatch, stated plainly.** Tranco's 10^6 domains fit in memory and
wrap around (`cycle`) in a realistic timeframe. Common Crawl's ~1.2×10^8
does not — cursoring through it via hourly ticks at a rate that does not
hammer Google's public DoH resolver is a **months-to-years background
process**, not something a bigger `--n` finishes today. See "Two domain
sources" below for the actual numbers.

**Does not claim completeness.** A domain resolving `NONE` across all 4
record types still gets one row (`record_type = "NONE"`) — "checked, found
nothing" and "never checked" must not look the same in the table
(CLAUDE.md's 検査を書く前の6問, #1).

## Why R2 Data Catalog, not a claim-graph wiki

`app-hyakka` (wiki.kotobase.net) is a curated, non-exhaustive sourced claim
graph — the whole thing was ~8,000 claims / ~6MB of served JSON when this
repo was designed. A million-domain resolution dataset is a different shape
of data (bulk rows, not encyclopedia entries) and belongs in a columnar
table a SQL engine can scan, not in a Worker-bundled Datalog snapshot. See
the ADR for the full reasoning.

## Two domain sources

| | `--source tranco` (default) | `--source commoncrawl` |
|---|---|---|
| List size | ~1,000,000 | ~120,000,000 |
| Fetched | fresh every tick (9.7MB zip) | once, into a local cache (`scripts/refresh_cc_domains.cljs`) — re-fetching 893MB every tick is not reasonable |
| Wraps at EOF | yes (`cycle`) — a realistic weekly/monthly event at this size | **no** — reaching EOF here is not realistic on any near-term timescale; a short slice at the tail means "caught up to the cache", not an error |
| State file | `data/state-tranco.edn` | `data/state-commoncrawl.edn` (separate cursor per source, both gitignored) |
| Default `--n` | 200 | 40000 |
| Default `--concurrency` | 80 | 80 |

## Parallelism, request rate, and being a good citizen of a free service

Every domain fires up to 4 DNS-over-HTTPS queries (A/AAAA/MX/NS). Launching
`--n` domains as one unbounded `js/Promise.all` — the original, naive design —
means a 40,000-domain tick opens **~160,000 simultaneous connections**. That
is a burst, not a rate, and is exactly the shape of traffic a free public
resolver's abuse detection looks for. `resolve-all!` in `resolve_tick.cljs`
instead runs a **bounded worker pool** (`--concurrency`, default 80 domains
in flight at once — so ~320 concurrent HTTP requests, not 160,000): as each
domain's 4 queries finish, the next domain starts. A 40,000-domain tick still
finishes as a sustained trickle over its 15-minute window, not a spike at the
start.

**Split across two resolvers.** Queries alternate, one domain at a time,
between Google (`dns.google`) and Cloudflare (`cloudflare-dns.com`) — both
confirmed live (2026-08-28) to speak the identical `application/dns-json` GET
shape. This roughly halves the sustained load either free service sees for a
given total throughput, and adds resilience (one resolver having a bad
minute does not stall the whole tick).

**Measured throughput (with an honest caveat).** Live runs against the real
cache, 2026-08-28:

| `--n` | `--concurrency` | wall time | domains/sec | machine load avg at the time |
|---|---|---|---|---|
| 2000 | 80 | 47.2s | 42.4 | ~150 |
| 3000 | 150 | 88.0s | 34.1 | ~210 rising to ~310 |
| 300 | 80 | 23.7s | 12.7 | ~358 |

**These numbers are a lower bound, not a clean benchmark.** All three runs
happened on a machine shared with many unrelated heavy processes (other
agent sessions, a 45-minute 181%-CPU `ugrep`, a multi-day 78%-CPU renderer);
load average ranged 150–360 during measurement, and threads compete for
CPU just to drive the event loop and fire the next batch of requests — the
DoH resolvers themselves were very likely idle-fast the whole time. Higher
`--concurrency` did NOT measure faster (150 was slower than 80), which is
consistent with local CPU contention dominating the measurement rather than
network or resolver-side limits. **Don't read these as "the resolvers can't
go faster than ~40/sec" — read them as "this machine, under its own
unrelated load, sustained at least ~40/sec without visible resolver-side
throttling or errors."** Re-measure on a quieter machine before tuning
`--concurrency` further; the code is already built to make that a one-flag
change.

**Coverage math at the current defaults** (`--n 40000` every 15 minutes):

```
40,000 domains / tick × 96 ticks/day = 3,840,000 domains/day
119,722,885 / 3,840,000 ≈ 31 days for one full pass
```

roughly the "finish in about a month" target, at a rate (~44 domains/sec ≈
178 queries/sec, split ~89/sec per resolver) this repo's own measurements
above show is comfortably sustainable — with headroom, since the measured
numbers were themselves suppressed by unrelated local load.

## Pipeline

```
domain list acquisition (source-specific — see table above)
  -> cursor N domains (data/state-<source>.edn tracks the offset)
  -> DNS-over-HTTPS A/AAAA/MX/NS lookups (dns.google/resolve — the same
     transport app-hyakka's collect-dns! already uses)
  -> one dated EDN ledger file (data/ledger/<date>/<tick-id>.edn, git —
     THE source of truth; CLAUDE.md 「消して再構築できるか」)
  -> export_and_sync.cljs: ledger -> JSON -> cloud_itonami.dns_resolution
     (Iceberg, rebuildable projection, never the other way round)
```

## GATE-DNS_RESOLVER (offline-default)

No live network call happens without **both**:

1. `--live` on the command line
2. `DNS_RESOLVER_OPERATOR_GATE=open` in the environment

This is the same shape as `cloud-itonami/yabai-actor`'s `GATE-G7`
(`YABAI_OPERATOR_GATE`) and `cloud-itonami/ipaddress-actor`'s. A deterministic
collector, not a `langgraph-clj` StateGraph+Governor actor (that pattern is
for LLM proposal/veto loops; this has no LLM in it at all). Both the
resolution tick and the Common Crawl cache refresh (it is also a live
network pull) share this one gate.

```sh
# refuses, no network call:
nbb --classpath src scripts/resolve_tick.cljs

# one bounded live tick against Tranco (200 domains by default):
DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/resolve_tick.cljs --live

# refresh the Common Crawl cache first (893MB download, several minutes;
# --limit N for a quick smoke test instead of the full ~120M-line file):
DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/refresh_cc_domains.cljs --live [--limit N]

# then tick against it (40000 domains, concurrency 80, split across 2
# resolvers, by default — see "Parallelism, request rate" above):
DNS_RESOLVER_OPERATOR_GATE=open nbb --classpath src scripts/resolve_tick.cljs --live --source commoncrawl

# ledger (all ticks so far, both sources) -> R2 Data Catalog:
nbb --classpath src scripts/export_and_sync.cljs --root <path to com-junkawasaki/root>
```

## Layout

```
scripts/resolve_tick.cljs         one bounded tick: domain-list slice -> DoH -> ledger
scripts/refresh_cc_domains.cljs   Common Crawl web-graph -> data/cache/cc-domains.txt
scripts/export_and_sync.cljs      ledger -> cloud_itonami.dns_resolution (Iceberg)
src/resolver/core.cljc            pure functions (list parsing, row shaping) — tested, no network
data/ledger/<date>/<id>.edn       canonical, reviewable EDN — the source of truth
data/state-<source>.edn           per-source cursor + last-tick metadata (gitignored)
data/cache/cc-domains*            Common Crawl cache + fetch metadata (gitignored — too large for git)
test/                             pure-function tests — no network
```

## R2 cost — snapshot expiration is on, and matters

`export_and_sync.cljs` calls `datalake-sync.py`, which does a full
`t.overwrite(tbl)` on every sync (not an incremental append — see that
script's own docstring for why). Every overwrite is a new Iceberg snapshot,
and **without cleanup, old snapshots' data files stay in R2 storage
forever**, so a growing table synced hourly accumulates roughly the SUM of
every past snapshot's size, not just the current one. Left unmanaged for a
month against a table growing toward ~11GB (estimated: ~4×10^8 rows at the
measured ~23 bytes/row compressed), that projects to several TB stored and
tens of dollars/month, climbing.

Fixed 2026-08-28: **catalog-level snapshot expiration is enabled on the
`cloud-itonami-datalake` bucket** (`older-than-days 2`, `retain-last 3`) —
free, and (as of the April 2026 R2 Data Catalog release) removes the
now-unreferenced data files too, not just Iceberg metadata:

```sh
wrangler r2 bucket catalog snapshot-expiration enable cloud-itonami-datalake \
  --older-than-days 2 --retain-last 3 --token "$CF_CATALOG_TOKEN"
```

With this on, storage stays bounded to roughly (live table size) × (a small
constant for the retained recent snapshots), not an ever-growing sum —
projected well under $1/month for this table even at full ~1.2×10^8-domain
scale. This setting is bucket-wide, so it also benefits the other Iceberg
tables sharing this bucket (GLEIF's, hyakka's ASN/prefix data).

## Querying the result

```python
from pyiceberg.catalog.rest import RestCatalog
cat = RestCatalog(name="r2",
                   uri="https://catalog.cloudflarestorage.com/4da88288dc30d9ee257f319d3c33ecf0/cloud-itonami-datalake",
                   warehouse="4da88288dc30d9ee257f319d3c33ecf0_cloud-itonami-datalake",
                   token="...")  # CF_CATALOG_TOKEN, or Keychain gftd.cf/API_TOKEN
cat.load_table(("cloud_itonami", "dns_resolution")).scan(
    row_filter="domain == 'example.com'"
).to_arrow().to_pandas()
```

## Sibling collectors under the same ADR

- `cloud-itonami/yabai-actor` — Certificate Transparency logs (crt.sh),
  existing, unchanged by this repo.
- `cloud-itonami-zone-ingest` (design only, not built) — CZDS zone files
  (.com/.net/.org NS delegations). Blocked on an ICANN CZDS application +
  per-registry Zone Data Access Agreement, which is an external approval
  process the repo's operator has to complete — an agent should not sign
  that on the owner's behalf. `kotoba-lang/zone` (existing RFC 1035
  zone-file parser) is the library to use once access exists; do not write
  a second one.
