# SEC EDGAR Adapter

Downloads XBRL financial filings from SEC EDGAR and turns them into graph
content. This file covers the clients, processors, and enrichment; the Dagster
orchestration that drives them is in
[`pipeline/README.md`](pipeline/README.md).

SEC is a shared repository — `manifest.py` declares its identity, plans, rate
limits, and endpoint access, and `config/shared_repositories.py` registers it.

## EDGAR rate limits

SEC enforces **10 requests/second**. The downloader targets **5 req/s** by
default (`requests_per_second: float = 5.0`), through the token-bucket
`AsyncRateLimiter` in `client/rate_limiter.py`. This is proactive: it paces
requests rather than reacting to 429s, because reacting means the corpus-scale
backfill spends its time in backoff. A `Retry-After` on a 429 is still honored,
and `RateMonitor` reports observed request and byte rates so you can tell whether
you are actually running at the configured pace.

Raising the rate is the fastest way to get the platform's EDGAR access
throttled. Treat 5 req/s as the ceiling in practice, not the floor.

## Components

**Clients** (`client/`)

| Class | Purpose |
|-------|---------|
| `edgar_client()` (`edgar.py`) | xbrlkit's `EdgarClient` on the platform's User-Agent: the ticker map, the submissions header and its paged history, EDGAR's throttle ridden out |
| `load_filing()` (`arelle.py`) | xbrlkit's cache-first Arelle load on the platform's cache directory (`ARELLE_CACHE_DIR`, seeded from the schema bundle at image build) |
| `SECDownloader` (`downloader.py`) | Bulk download of XBRL ZIPs to S3, rate-limited; discovery through xbrlkit's `EftsClient` |

**Processors** (`processors/`)

| Class | Purpose |
|-------|---------|
| `XBRLGraphProcessor` | One XBRL filing → parquet files, on xbrlkit's model and property-graph projection |
| `XBRLDuckDBGraphProcessor` | Unified staging + materialization |
| `DuckDBStager` | S3 Parquet → DuckDB (stage 1) |
| `LadybugMaterializer` | DuckDB → LadybugDB (stage 2) |
| `SECMetadataLoader` | Filer and report metadata, cached |

The two ingestion stages are decoupled on purpose — a failed LadybugDB
materialization must not discard hours of DuckDB staging work — and
`XBRLDuckDBGraphProcessor` subclasses both for callers that run them together.
There is no path from S3 straight into LadybugDB; staging is always in between.

**The graph tables are xbrlkit's.** `XBRLGraphProcessor` loads the filing with
`load_filing` (xbrlkit's loader), parses it with `xbrlkit.parse.to_xbrl_model`, and projects it with
`xbrlkit.serialize.lpg.to_graph_tables` — the same ids, columns and DDL that
`xbrlkit build --format lpg` writes into a single-filing `.lbug`, so a filing
projected there and a filing ingested here are the same rows. What stays in the
adapter is what needs the platform: text-block externalization to the CDN
(`textblock.py`), semantic enrichment (`enrichment.py`), the schema-aware parquet
writer (`parquet.py`) and association classification (`classify.py`), all of
which run on top of the projected tables.

Supporting modules: `constants.py` (`SHARED_NODE_TABLES`, `QUARTER_END_DAYS`),
`processing.py` (`process_single_filing_to_memory()`), `consolidation.py`
(`consolidate_parquet_from_disk()`, `merge_with_existing_s3()`,
`atomic_s3_upload()`), `ids.py` (naming helpers — graph ids are minted by the
xbrlkit projection),
`schema.py`, `dataframe.py`, `parquet.py`, `textblock.py` (S3 externalization
for oversized text), `classify.py`.

**Text extraction**

| Class | Purpose |
|-------|---------|
| `NarrativeExtractor` (`xbrlkit.text`) | Item sections (Business, Risk Factors, MD&A, Cybersecurity) from 10-K/10-Q HTML via heading detection; a long section comes back as parts. Lives in the public `xbrlkit` package since 2026-09 so the toolkit and the platform read a filing with one parser |
| `iXBRLParser` (`xbrlkit.text`) | iXBRL disclosure sections (`ix:nonNumeric` TextBlock elements, continuation chains resolved) with XBRL element metadata for graph cross-reference; a long section comes back as parts |

**MCP resolvers** (`mcp/`) — `report_resolver.py` resolves ticker plus form code
to the latest relevant filing, backing `financial-statement-analysis`'s
auto-resolve; `element_resolver.py` backs `resolve-element`.

## Enrichment (inline, per filing)

`SemanticEnricher` (`enrichment.py`) runs during the Process stage and adds
semantic metadata:

- **Canonical concepts** — maps XBRL element qnames to canonical concepts (e.g.
  `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` → `revenue`)
  using fastembed cosine similarity against the curated `ConceptTaxonomy` in
  `taxonomy/`.
- **Structure classification** — assigns `canonical_type` to Structure nodes:
  `income_statement`, `balance_sheet`, `cash_flow_statement`,
  `equity_statement`.
- **Association classification** — creates Classification nodes linking
  Associations to disclosure types (AssetsRollUp, RevenueBreakdown, …).
  `classify_associations()` does this offline via a temporary LadybugDB with
  Cypher pattern matching.
- **Confidence refinement** — applies the knowledge artifacts below to suppress
  weak semantic matches and boost well-connected elements.

**Embeddings are transient.** They are computed to assign canonical concepts and
types, then discarded — the per-element `embedding` vector is not persisted, and
there is no element-vector search tier. Indexing millions of elements produced
long-tail noise dominated by single-filing filer extensions, so `resolve-element`
works from canonical concepts with a text-label fallback instead. The persisted
outputs are `canonical_concept` and `canonical_type`.

## Knowledge artifacts (offline, corpus-level)

Dagster jobs run `ArcExtractor` → graph construction → `StatementClassifier`
over the full DuckDB corpus to produce three parquet artifacts:

- `element_knowledge.parquet` — pagerank, neighborhood agreement, BFS depth, and
  statement type per element
- `structure_profiles.parquet` — element-composition fingerprints per structure
  type
- `structure_consensus.parquet` — canonical-type consensus across structures

| Class | Purpose |
|-------|---------|
| `ArcExtractor` | Edges, filing counts, disclosure types out of DuckDB |
| `build_element_graph_from_arrow()` | Zero-copy DuckDB → Arrow → CSR via `Graph.fromCSR()` |
| `StatementClassifier` | BFS plus heuristic classification into statement types |
| `ElementKnowledgeBuilder` | Builds `element_knowledge.parquet` |
| `StructureKnowledgeBuilder` | Builds `structure_profiles.parquet` and `structure_consensus.parquet` |
| `DuckDBAnalyticsContext` | Sync context manager for analytics over DuckDB staging files |

Graph construction uses [icebug](https://github.com/Ladybug-Memory/icebug), a
networkit fork optimized for columnar memory. `Graph.fromCSR()` takes zero-copy
Arrow arrays, so the path is DuckDB SQL (node indexing, edge dedup) →
`fetch_arrow_table()` → numpy COO→CSR → `fromCSR()`, with no Python
per-element loop. That avoids triple-buffering the data across DuckDB, Python
lists, and the graph.

In production the `sec_knowledge_artifacts` asset downloads the staging DuckDB
file from `s3://{user-bucket}/shared-repositories/databases/sec.duckdb`, builds
the artifacts on the Fargate task, and uploads them to
`s3://{SHARED_PROCESSED_BUCKET}/sec/artifacts/`. The DuckDB file itself is
published by the `sec_duckdb_s3_publish` job after staging. Locally, artifacts live
in `data/artifacts/`. `SemanticEnricher` checks local disk first and downloads
from S3 when missing — a fresh environment with no artifacts still enriches, just
without confidence refinement.

## Usage

```python
from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

client = SECClient(cik="0000320193")
submissions = client.get_submissions()

processor = XBRLGraphProcessor(
    report_uri="https://www.sec.gov/Archives/...",
    entityId="0000320193",
    sec_filer=sec_filer_dict,
    sec_report=sec_report_dict,
    output_dir="/tmp/output",
)
processor.process()
```

In practice the adapter runs through Dagster:

```bash
just sec-load NVDA 2025
just sec-health
just sec-reset
```

## Schema cache

Arelle resolves a filing's DTS by fetching a few hundred schemas and linkbases,
and the two smallest hosts (xbrl.org, w3.org) throttle a cold cache within a few
dozen filings. The load is xbrlkit's, which serves the DTS from a persistent
cache in Arelle's layout, spaces fetches per host, waits out a `Retry-After`,
never re-validates a cached file (Arelle's weekly recheck, one request per file
per process on a warm cache, was the throttle generator), and fails a filing
loudly with `DtsResolutionError` when a document cannot be resolved — the
pipeline records it and retries later rather than indexing a filing missing the
concepts a schema declared.

The image seeds `ARELLE_CACHE_DIR` from `arelle/bundles/arelle-schemas-latest.tar.gz`
at build (`xbrlkit cache extract`). The bundle carries every file from the
throttling hosts (about 125 files, 120 KB); the tolerant hosts (xbrl.sec.gov,
xbrl.fasb.org) fill in on first use. To refresh it:

```bash
uv run xbrlkit cache download --cache-dir /tmp/arelle-seed --years 2022-2026
uv run xbrlkit cache bundle --cache-dir /tmp/arelle-seed --host www.xbrl.org --host www.w3.org \
  --out robosystems/adapters/sec/arelle/bundles/arelle-schemas-$(date +%Y%m%d).tar.gz
# then repoint the arelle-schemas-latest.tar.gz symlink
```

## Configuration

`config.py` holds the XBRL processing settings —
`XBRL_COLUMN_STANDARDIZATION` (column name mapping),
`XBRL_EXTERNALIZATION_THRESHOLD` (size at which text goes to S3), and
`XBRL_STANDARDIZED_FILENAMES` (output naming) — plus three enrichment flags:

| Flag | Default | Effect |
|------|---------|--------|
| `XBRL_SEMANTIC_ENRICHMENT` | `True` | fastembed canonical concept mapping and structure classification |
| `XBRL_ASSOCIATION_CLASSIFICATION` | `True` | Association-level disclosure classification (creates Classification nodes) |
| `XBRL_GRAPH_REFINEMENT` | `True` | Knowledge-artifact confidence refinement |

## Related

- [`pipeline/README.md`](pipeline/README.md) — Dagster stages, jobs, sensors
- [`../README.md`](../README.md) — adapter patterns and the manifest contract
- [`../../schemas/README.md`](../../schemas/README.md) — graph schema definitions
