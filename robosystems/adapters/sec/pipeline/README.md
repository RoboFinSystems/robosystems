# SEC Pipeline

Dagster orchestration for the SEC adapter: assets, jobs, sensors, and the
nightly schedule. The adapter code these assets drive — clients, processors,
enrichment — is documented in [`../README.md`](../README.md).

`dagster/definitions.py` collects everything here through one entry point:

```python
from robosystems.adapters.sec.pipeline import get_dagster_components

components = get_dagster_components()
# {"assets": [...], "jobs": [...], "sensors": [...], "schedules": [...]}
```

## Stages

The chain is **Download → Process → Stage → Materialize → Publish**, with text
indexing branching off Process in parallel.

### 1. Download — `sec_raw_filings`

Discovers filings through the EFTS API and downloads XBRL ZIPs to S3,
rate-limited to stay well inside EDGAR's 10 req/s ceiling (see
[`../README.md`](../README.md)).

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_raw_filings --partition 2024-Q1
```

### 2. Process — `sec_processed_filings`

Turns raw XBRL into parquet part files, one per table per batch, and runs inline
semantic enrichment. **One batch per run, then the container exits** — up to 250
filings (`SEC_PROCESS_BATCH_SIZE`), after which the sensor re-triggers if pending
files remain. Exiting is the memory-release mechanism, not a failure.

Memory is the binding constraint: the small batch keeps Arrow concatenation
under roughly 325 MB peak, shared tables (Element, Label, …) are deduped within
the batch in pure Arrow, and `del` plus `gc.collect()` runs after each table
upload. Cross-batch dedup is DuckDB's job at the staging stage, not this one.

**Spot resilience.** Each filing's results are cached to S3 as a zip immediately
after processing, so a restart restores from cache rather than reprocessing. A
SIGTERM handler stops the loop and attempts a best-effort flush; if the two-minute
spot window allows, filings are consolidated and marked success, and if it does
not, the cache covers them on the next run. Nothing is lost either way.

**Public artifacts.** While the model is in hand, the processor also writes
the filing's portable representations to the public-data bucket, in the
folder its externalized text blocks use (`{year}/{cik}/{accession}/`): the
`holon.jsonld` (text blocks as their CDN URLs), the Project Tavi compiled
model `tavi.json` with its `tavi.gaps.json` sidecar, the primary document as
filed, and a `manifest.json` naming what was written. Gated by
`SEC_FILING_ARTIFACTS_ENABLED`; never fails the filing
(`processors/artifacts.py`). A backfill of the artifacts is a reprocess.

Output layout:

```
s3://{bucket}/sec/processed/filed=2024-Q1/nodes/Entity/part_{uuid}.parquet
```

One part file per table per batch, UUID-named so runs never collide. DuckDB reads
both this layout and the older flat `TABLE.parquet` form.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_processed_filings --partition 2024-Q1
```

### 3. Stage — `sec_duckdb_staged` / `sec_duckdb_incremental_staged`

Loads processed parquet into persistent DuckDB. A full rebuild dedups with
`CREATE TABLE … GROUP BY + FIRST()`; the incremental path uses
`INSERT INTO … WHERE NOT EXISTS`.

```bash
uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_staged
uv run dagster asset materialize -m robosystems.dagster --select sec_duckdb_incremental_staged
```

### 4. Materialize — `sec_graph_materialized`

Full LadybugDB rebuild from DuckDB staging.
`sec_historical_materialized` covers the historical corpus separately.

```bash
uv run dagster asset materialize -m robosystems.dagster --select sec_graph_materialized
```

### 5. Text indexing — `sec_narratives_indexed`, `sec_ixbrl_disclosures_indexed`

Both depend on `sec_processed_filings` and run parallel to the DuckDB branch,
indexing into OpenSearch for hybrid BM25 + KNN search.

| Asset | Source | Content |
|-------|--------|---------|
| `sec_narratives_indexed` | Raw filing ZIPs | Item sections — MD&A, Risk Factors, Business, Cybersecurity — from 10-K/10-Q |
| `sec_ixbrl_disclosures_indexed` | Raw ZIPs + Fact parquets | iXBRL disclosure sections with XBRL element metadata and a CDN `content_url` for graph cross-reference |

### 6. Publish

- `sec_lbug_s3_published` — the `.lbug` file for the replica fleet
- `sec_duckdb_s3_published` — the `.duckdb` file the offline knowledge-artifact
  build consumes
- `sec_lbug_r2_published` — a Cloudflare R2 copy for zero-egress subscriber
  downloads (manual or weekly)

`sec_knowledge_artifacts` builds the corpus-level artifacts from the published
DuckDB file.

### 7. Catalog — `sec_filing_catalog`

The public pages' index, without a database. Folds the processed Report,
Entity and ENTITY_HAS_REPORT parquet of every partition from `start_year`
on, joined to each filing's `manifest.json`, into `companies/{ticker}.json`
per filer and `companies/index.json` for the corpus, on the public CDN.
Files are regenerated whole — every filer with a filing in the run's
partitions, all of them on `full_rebuild`, the index always — so overlapping
runs cannot corrupt one. Also writes `robots.txt` when it is missing.
Chained off staging by `sec_post_stage_index_sensor`, beside the text index;
the job is `sec_catalog` (a job may not share its asset's name).

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_filing_catalog --partition 2026-Q3
```

## Nightly chain

```
9pm EST — sec_incremental_download_schedule
  → download (current quarter, plus previous at a quarter boundary)

sec_incremental_pipeline_sensor
  → process (250-filing batches, looping; spot-safe via the S3 cache)
  → stage (DuckDB INSERT with NOT EXISTS dedup)
  [waits for all partitions to drain at quarter boundaries]

sec_stage_to_materialize_sensor
  → materialize (full LadybugDB rebuild)

sec_post_stage_index_sensor
  → text indexing
  → filer catalog (companies/*.json + index.json on the public CDN)

sec_post_materialize_publish_sensor
  → lbug S3 publish
  → duckdb S3 publish (sequential, to avoid overloading the instance)
  → replica refresh (rolling, min_healthy=100%, ~15 min warmup)
```

**All sensors start STOPPED.** Enable them in the Dagster UI when you want the
automated chain; nothing runs on its own after a fresh deploy.

| Sensor / schedule | Triggers | Role |
|-------------------|----------|------|
| `sec_incremental_download_schedule` | `sec_download_job` | 9pm EST weekdays |
| `sec_incremental_pipeline_sensor` | `sec_process_job`, `sec_incremental_stage_job` | download → process (batched loop) → stage |
| `sec_wake_to_stage_sensor` | staging jobs | resumes the chain after a sleep window |
| `sec_stage_to_materialize_sensor` | `sec_materialize_job` | stage → full graph rebuild |
| `sec_post_stage_index_sensor` | `sec_narratives_index_job`, `sec_ixbrl_index_job` | stage → OpenSearch indexing |
| `sec_index_retry_sensor` | index jobs | retries failed index runs |
| `sec_post_materialize_publish_sensor` | `sec_lbug_s3_publish_job`, `sec_duckdb_s3_publish_job`, `shared_replicas_refresh_job` | materialize → publish → replica refresh |
| `sec_master_sleep_on_failure_sensor` | — | halts the chain on failure instead of looping |
| `sec_processing_sensor` | `sec_process_job` | backfill: discovers pending SourceFiles across all quarters, polls every 5 min |

`sec_processing_sensor` is for bulk and manual processing, not the nightly path.

## Jobs

Seventeen jobs are exported. The nightly path uses `sec_download_job`,
`sec_process_job`, `sec_incremental_stage_job`, `sec_materialize_job`,
`sec_lbug_s3_publish_job`, `sec_duckdb_s3_publish_job`, plus the two index jobs.
The rest cover the historical corpus (`sec_historical_stage_job`,
`sec_historical_materialize_job`, `sec_historical_staged_materialize_job`,
`sec_historical_duckdb_s3_publish_job`, `sec_historical_lbug_s3_publish_job`),
staged variants (`sec_stage_job`, `sec_staged_materialize_job`), R2 publishing
(`sec_lbug_r2_publish_job`), and artifacts (`sec_artifact_generation_job`).

## Run configurations

All in `configs.py`:

| Config | Asset | Key options |
|--------|-------|-------------|
| `SECDownloadConfig` | `sec_raw_filings` | `form_types`, `tickers`, `dry_run` |
| `SECProcessConfig` | `sec_processed_filings` | `batch_size`, `continue_on_error`, `form_types` |
| `SECStageConfig` | `sec_duckdb_staged` | `reset_staging`, `year`, `start_year`/`end_year` |
| `SECIncrementalStageConfig` | `sec_duckdb_incremental_staged` | `year`, `quarter` |
| `SECHistoricalStageConfig` | historical staging | year range |
| `SECMaterializeConfig` | `sec_graph_materialized` | `rebuild_graph`, `batch_materialization`, `materialization_batch_size` |
| `SECArtifactConfig` | `sec_knowledge_artifacts` | artifact build options |
| `SECNarrativeIndexConfig` | `sec_narratives_indexed` | `graph_id`, `part_size`, `form_types`, `force_reindex`, `skip_embeddings` |
| `SECiXBRLIndexConfig` | `sec_ixbrl_disclosures_indexed` | `graph_id`, `part_size`, `form_types`, `force_reindex`, `skip_embeddings` |

Partitioning and corpus bounds are also here: `sec_quarter_partitions`,
`SEC_QUARTERS`, `SEC_START_YEAR`, `SEC_PRIMARY_START_YEAR`,
`SEC_HISTORICAL_FORM_TYPES`, `SEC_HISTORICAL_END_YEAR`, `SEC_FORM_TYPE_BATCHES`.

## Cross-package imports

This pipeline reaches back into platform modules (adapter → platform):
`dagster/assets/shared_repositories/` for the replica refresh asset, and
`dagster/jobs/shared_repository` for the replica refresh job the publish sensor
triggers.

## Related

- [`../README.md`](../README.md) — SEC adapter internals
- [`../../../dagster/README.md`](../../../dagster/README.md) — orchestration patterns
- `dagster/definitions.py` — where `get_dagster_components()` is collected
