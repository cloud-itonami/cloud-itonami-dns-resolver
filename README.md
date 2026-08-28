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
| Default `--n` | 200 | 2000 |

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

# then tick against it (2000 domains by default):
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
