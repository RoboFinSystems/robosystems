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
| `SECClient` (`edgar.py`) | EDGAR API — company lookup, filing metadata, submissions |
| `ArelleClient` (`arelle.py`) | XBRL processing via the Arelle library |
| `SECDownloader` (`downloader.py`) | Bulk download of XBRL ZIPs to S3, rate-limited |
| `EFTSClient` (`efts.py`) | EFTS full-text filing discovery |

**Processors** (`processors/`)

| Class | Purpose |
|-------|---------|
| `XBRLGraphProcessor` | One XBRL filing → parquet files |
| `XBRLDuckDBGraphProcessor` | Unified staging + materialization |
| `DuckDBStager` | S3 Parquet → DuckDB (stage 1) |
| `LadybugMaterializer` | DuckDB → LadybugDB (stage 2) |
| `SECMetadataLoader` | Filer and report metadata, cached |

The two ingestion stages are decoupled on purpose — a failed LadybugDB
materialization must not discard hours of DuckDB staging work — and
`XBRLDuckDBGraphProcessor` subclasses both for callers that run them together.
There is no path from S3 straight into LadybugDB; staging is always in between.

Supporting modules: `constants.py` (`SHARED_NODE_TABLES`, `QUARTER_END_DAYS`),
`processing.py` (`process_single_filing_to_memory()`), `consolidation.py`
(`consolidate_parquet_from_disk()`, `merge_with_existing_s3()`,
`atomic_s3_upload()`), `ids.py` (`create_entity_id()`, `create_fact_id()`, …),
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
