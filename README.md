# cloud-itonami-dns-resolver

Bounded, G7-gated active DNS resolution against a slice of [Tranco's daily
top-1M domain list](https://tranco-list.eu/), landing in Cloudflare R2 Data
Catalog (Apache Iceberg, `cloud_itonami.dns_resolution`) — not in any wiki
claim graph. This repo does not do enough on its own to be "SecurityTrails
for the whole world"; it is one honestly-scoped, free, legal piece of that
ambition. See below for what it actually covers and what it does not.

Design: `90-docs/adr/2608280900-world-scale-dns-domain-collection.edn`
(superproject `com-junkawasaki/root`).

## What this collects, and what it does not

| | in scope | out of scope |
|---|---|---|
| Data | A / AAAA / MX / NS records for domains on Tranco's top-1M list | RDAP/WHOIS registrant data (name, org, address — see 2607309950's refusal of RDAP bulk search, which stands) |
| Coverage | The Tranco list (actively-visited sites), cursored N domains per tick | The other ~349,000,000+ registered domains that never appear on Tranco. **"the world's most-visited ~1M domains", not "the world's domains".** |
| Source | Free, no key: Tranco CSV + Google's DNS-over-HTTPS resolver | Commercial passive-DNS feeds (Farsight/DNSDB, SecurityTrails' own API) — out of budget scope, would need a paid contract this repo does not have |

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

## Pipeline

```
Tranco top-1M.csv.zip (daily, free)
  -> unzip (shells out to `unzip`; no JS zip dependency)
  -> cursor N domains (data/state.edn tracks the rank offset, wraps at EOF)
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
for LLM proposal/veto loops; this has no LLM in it at all).

```sh
# refuses, no network call:
nbb scripts/resolve_tick.cljs

# one bounded live tick, 200 domains by default:
DNS_RESOLVER_OPERATOR_GATE=open nbb scripts/resolve_tick.cljs --live [--n 200]

# ledger (all ticks so far) -> R2 Data Catalog:
nbb scripts/export_and_sync.cljs --root <path to com-junkawasaki/root>
```

## Layout

```
scripts/resolve_tick.cljs      one bounded tick: Tranco slice -> DoH -> ledger
scripts/export_and_sync.cljs   ledger -> cloud_itonami.dns_resolution (Iceberg)
data/ledger/<date>/<id>.edn    canonical, reviewable EDN — the source of truth
data/state.edn                 cursor + last-tick metadata (gitignored — local/operator state)
test/                          pure-function tests (CSV parsing, row shaping) — no network
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
