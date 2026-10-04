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

The holon, the Tavi model and the filed document are stored gzipped, as
`Content-Encoding: gzip` under the same key and media type — the CDN compresses
neither `application/ld+json` nor anything over 10 MB itself, and serves a
stored encoding as it is. A browser or an HTTP library decodes it before
anything reads a byte; **boto3 does not**, so a reader that takes these objects
straight from the bucket goes through `S3Client.download_string`, which does.
The manifest stays plain (the catalog reads it with raw boto3) and its `bytes`
are the decoded sizes. The bytes are deterministic (`gzip_artifact`: level 6,
no timestamp), so a reprocess still rewrites the same object. Every write to
the public bucket names `INTELLIGENT_TIERING` on the PUT rather than leaving it
to the bucket's lifecycle rule; the opt-in Archive tiers need an async restore
and must never be enabled there.

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
  downloads (manual or weekly). Just before the cut it counts the graph on the
  master and, once the backup succeeds, writes `sec.stats.json` beside the
  archive, paired to it by compressed size
- `sec_lbug_hf_published` — copies the R2 snapshot to the public Hugging Face
  dataset (manual only) and rewrites the dataset card from `dataset_card.md`
  and those stats. A missing or mismatched stats file stops the run before the
  copy starts; re-run the R2 publish to fix it

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

### 8. Filed documents — `sec_current_reports`, `sec_filing_documents`

The documents the XBRL path does not bring, fetched from EDGAR once and served
from the public bucket (`documents.py`). Neither goes into OpenSearch: the
`describe-filing` / `search-text` / `read-text` tools read a filing whole from
its folder, as `information-block` does.

- **`sec_current_reports`** — 8-K earnings releases. EFTS lists the quarter's
  8-Ks with the items each reports (a window over EFTS' 10k ceiling is
  halved); those reporting a wanted item (2.02 / 7.01 by default) from a filer
  the corpus holds have their `-xbrl.zip` fetched — one request, the exhibits
  ride inside — into `sec/8k/filed={quarter}/` in the raw bucket, apart from
  the `sec/year=` tree the process stage reads. The 8-K and its EX-99 exhibits
  (classified by the 8-K's own exhibit index, then the file's heading, then
  its name) go to the filing's public folder with a `manifest.json`, and the
  filer's releases list `current-reports/{cik}.json` gains the filing — how an
  8-K is found, since it is in neither the graph nor the catalog. A zip
  already in raw is read from there, so a re-run fetches nothing from EDGAR
  but the EFTS pages. A filing already published is listed from its manifest,
  so a run that ended before its lists were written is repaired by the next;
  a manifest that cannot be read, or names no files, is published again; and a
  list that would not change is not rewritten. A combined 8-K is published
  once, under its first registrant, and listed under every registrant the
  corpus holds: each entry's `folder` says where the files are.
- **`sec_filing_documents`** — the primary document of a filing processed
  before inline XBRL. Its zip held the instance only, so its folder has the
  holon and the Tavi but not the document; the manifest names it, one request
  fetches it, and the manifest lists it. The work list comes from the
  processed Report rows and the manifests, so EDGAR sees only the document
  requests. A reprocess keeps the fetched document in the manifest.

Both are quarter-partitioned and run a selected range in one run, a quarter at
a time. Every job that pulls from EDGAR (these two and `sec_download`) carries
the `edgar` run tag, which `dagster.yaml` limits to one at a time, and the two
assets refuse to start while another pull is running — a run launched past
the queue (the Materialize button, `dagster asset materialize`) carries no
tag, so launch them through their jobs. The releases lists are read-merge-
written, which is safe only because of that: never run the capture by hand
beside the nightly chain.

```bash
uv run dagster job launch -m robosystems.dagster \
  -j sec_current_reports_capture --partition 2026-Q3
```

## Nightly chain

```
9pm ET — sec_incremental_download_schedule
  → download (the current Eastern-time quarter only)

sec_incremental_pipeline_sensor
  → process (250-filing batches, looping; spot-safe via the S3 cache)
  → shared master wake, once the partition has drained

sec_wake_to_stage_sensor
  → stage (DuckDB INSERT with NOT EXISTS dedup)

sec_stage_to_materialize_sensor
  → materialize (full LadybugDB rebuild)

sec_post_stage_index_sensor
  → text indexing
  → filer catalog (companies/*.json + index.json on the public CDN)

sec_current_reports_sensor
  → 8-K earnings releases, the last 7 days (after the download, never beside it)

sec_post_materialize_publish_sensor
  → lbug S3 publish
  → duckdb S3 publish (sequential, to avoid overloading the instance)
  → replica refresh (rolling, min_healthy=100%, ~15 min warmup)
```

The download's quarter travels down the chain as the `quarter` run tag, so
stage, index and catalog all work on the quarter that was downloaded, even when
a run finishes after midnight on a quarter's last day.

## Intraday

The graph is rebuilt once a night. What is read from a filing's public folder
does not need the graph, so two schedules bring it forward to the day of
filing. Both are tagged `mode=intraday` and neither wakes the master.

```
09:45, 13:45, 17:45 ET — sec_intraday_download_schedule
  → download (today and the two days before, within the quarter)

sec_incremental_pipeline_sensor
  → process, only when the download found something new
  → filer catalog, once the partition has drained   (no wake, no stage)

every 30 min, 06:00–20:30 ET — sec_current_reports_intraday_schedule
  → 8-K earnings releases, today and the two days before
```

- **Same day:** a new 10-K / 10-Q's holon and document are published by the
  process stage and listed by the catalog, so `describe-filing`, `search-text`,
  `read-text`, the viewer and the filer pages have it; an 8-K is readable
  within the half hour, plus whatever lag EFTS adds.
- **Next morning:** the graph (`financial-statement-analysis`,
  `build-fact-grid`, Cypher), `disclosures` / `information-block` (they look
  the report up in the graph), and document search (indexing stays on the
  nightly chain). For those hours a filing is known to the text tools and not
  to the numbers tools.
- **The night is unchanged.** The nightly download reads the whole quarter and
  the nightly stage re-reads the whole quarter, so everything an intraday pass
  processed is staged and materialized with the rest. A nightly download that
  finds an intraday process run still going asks for its own: an intraday run
  ends at the catalog, and the `quarter` run-queue limit holds the nightly one
  behind it.
- **Cost.** The look-back (`since_days` on the download) is what keeps a pass
  cheap: a whole-quarter download refreshes the submissions of every filer in
  the quarter, thousands of EDGAR requests late in a quarter, where a pass
  refreshes the filers of three dates. A pass that finds nothing new stops
  after the download. An 8-K tick pages the 8-Ks of its three dates, lists the
  corpus, and reads the manifest of each release it wants; it fetches and
  lists only what is new. The `edgar` run-queue limit serializes both
  schedules with each other and with the nightly download, and an 8-K tick
  that finds a capture still queued or running is skipped.
- **What the look-back does not reach.** A filing dated on a Friday after the
  last pass, or on a quarter's last day, is outside Monday's or the new
  quarter's window: those are the nightly run's, which reads the whole quarter.
  The 8-K schedule stops at 20:30 so the nightly chain's week-long capture is
  not queued behind it, and that capture is asked for even with an intraday one
  in the queue.
- **A listing pass can be late.** If the catalog is already running when a
  pass finishes processing, that pass is not listed until the next one that
  finds something, or the night. One more run each way for the failure alarm
  to see: an intraday run that fails is healed by the next, but it alarms.

**All sensors and schedules start STOPPED.** Enable them in the Dagster UI
when you want the automated chain; nothing runs on its own after a fresh deploy.

| Sensor / schedule | Triggers | Role |
|-------------------|----------|------|
| `sec_incremental_download_schedule` | `sec_download_job` | 9pm ET weekdays, the whole quarter |
| `sec_intraday_download_schedule` | `sec_download_job` | 09:45, 13:45, 17:45 ET weekdays, a three-date look-back |
| `sec_current_reports_intraday_schedule` | `sec_current_reports_job` | every 30 min, 06:00–20:30 ET weekdays, a three-date look-back |
| `sec_incremental_pipeline_sensor` | `sec_process_job`, `shared_master_wake_job`, `sec_filing_catalog_job` | download → process (batched loop) → wake the shared master once drained (nightly), or → the filer catalog (intraday) |
| `sec_wake_to_stage_sensor` | `sec_incremental_stage_job` | master awake → stage the tagged quarter |
| `sec_stage_to_materialize_sensor` | `sec_materialize_job` | stage → full graph rebuild |
| `sec_post_stage_index_sensor` | `sec_narratives_index_job`, `sec_ixbrl_index_job` | stage → OpenSearch indexing |
| `sec_post_materialize_publish_sensor` | `sec_lbug_s3_publish_job`, `sec_duckdb_s3_publish_job`, `shared_replicas_refresh_job` | materialize → publish → replica refresh |
| `sec_master_sleep_on_failure_sensor` | — | halts the chain on failure instead of looping |
| `sec_current_reports_sensor` | `sec_current_reports_job` | download → the week's 8-K earnings releases |
| `sec_processing_sensor` | `sec_process_job` | backfill: discovers pending SourceFiles across all quarters, polls every 5 min |

`sec_processing_sensor` is for bulk and manual processing, not the nightly path.

## Jobs

Twenty-one jobs are exported. The nightly path uses `sec_download_job`,
`sec_process_job`, `sec_incremental_stage_job`, `sec_materialize_job`,
`sec_lbug_s3_publish_job`, `sec_duckdb_s3_publish_job`, the two index jobs,
`sec_filing_catalog_job` and `sec_current_reports_job`;
`sec_filing_documents_job` is a backfill run by hand.
The rest cover the historical corpus (`sec_historical_stage_job`,
`sec_historical_materialize_job`, `sec_historical_staged_materialize_job`,
`sec_historical_duckdb_s3_publish_job`, `sec_historical_lbug_s3_publish_job`),
staged variants (`sec_stage_job`, `sec_staged_materialize_job`), R2 publishing
(`sec_lbug_r2_publish_job`), and artifacts (`sec_artifact_generation_job`).

## Run configurations

All in `configs.py`:

| Config | Asset | Key options |
|--------|-------|-------------|
| `SECDownloadConfig` | `sec_raw_filings` | `form_types`, `tickers`, `since_days`, `dry_run` |
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
