# Pipeline Guide

RoboSystems uses [Dagster](https://dagster.io) for all data orchestration. Data flows from external sources through adapters, staging (DuckDB), and into the knowledge graph (LadybugDB).

**Related documentation:**
- [Architecture Overview](Architecture-Overview) - System architecture
- [Bootstrap Guide](Bootstrap-Guide) - Deployment instructions
- [Custom Graph Schema](Custom-Graph-Schema) - Build a custom adapter and graph
- [SEC XBRL Pipeline](SEC-XBRL-Pipeline) - The SEC ingestion pipeline in depth

---

## Current Pipelines

| Pipeline   | Status           |
| ---------- | ---------------- |
| SEC        | Production-ready |
| QuickBooks | Production-ready |

A third connector is live but is not a pipeline in this sense: **`external`** ("External Integration") registers a source namespace on a graph for an integration the platform does not run. It is registration and telemetry rather than execution config — the integration itself runs outside the platform, holds its own source credentials, and writes through the public API. That is the connector side of the [integration lane](#adding-custom-data-sources) below; the implementation is `operations/providers/external_provider.py`.

The authoritative, live list of connectors is served rather than documented: `GET /v1/graphs/{graph_id}/connections/options` returns each provider with its configuration and supported features.

---

## Three Pipeline Patterns

RoboSystems uses three distinct patterns based on data source characteristics. Patterns A and B ingest from external systems the platform doesn't own; Pattern C materializes the platform's own OLTP writes into the graph for analytical queries.

### Pattern A: Arelle-Based (SEC)

```
XBRL Files → Arelle Processing → Parquet → DuckDB Staging → LadybugDB
            (semantic extraction)   (already graph-shaped)
```

- **Used for**: SEC EDGAR filings (XBRL format)
- **Why**: Arelle extracts XBRL semantics (concepts, contexts, facts) directly into graph-compatible structures
- **No dbt needed**: Output is already graph-shaped

### Pattern B: dbt-Based (QuickBooks, Custom)

```
API JSON → S3 Raw → dbt transforms → S3 Processed → DuckDB Staging → LadybugDB
          (chunks)  (JSON → Parquet)  (graph-shaped)
```

- **Used for**: API-based integrations (QuickBooks, custom ERPs)
- **Why**: Raw JSON needs transformation into graph-compatible node/relationship Parquet
- **dbt provides**: SQL-based transforms, testing, documentation

### Pattern C: OLTP-Based (Extensions Materialization)

```
Extensions PostgreSQL → postgres_scanner → DuckDB Staging → LadybugDB
(schema-per-graph)      (direct scan)      (graph-shaped)
```

- **Used for**: RoboLedger and RoboInvestor writes — schedules, closing entries, reports, portfolio positions, anything a user or AI writes through the `/extensions/*/operations/*` command surface
- **Why**: The platform owns this data — it's not derived from an external system, it's transactional state that needs to round-trip into the graph so analytical queries can see it alongside SEC/QuickBooks data
- **Trigger**: Event-driven rather than scheduled. Writes to the extensions database fire a `mark_graph_stale` signal; a blue/green sensor picks it up and rebuilds the affected graph slice
- **No Parquet intermediate**: DuckDB's `postgres_scanner` extension reads directly from the extensions schema via `CREATE TABLE ... AS SELECT * FROM postgres_scan(...)`, skipping the S3 round-trip that Patterns A and B need
- **Not an adapter**: Unlike SEC and QuickBooks, there's no `adapters/extensions/` directory — this pipeline is platform-internal. See [RoboLedger Operations](RoboLedger-Operations) and `operations/roboledger/views/` for details

---

## Adapter Structure

Each adapter is self-contained: client, processors, and pipeline (Dagster orchestration) all live together. `dagster/definitions.py` collects adapter pipelines via the `get_dagster_components()` discovery pattern.

```
robosystems/adapters/
├── base.py                     # SharedRepositoryManifest dataclass
├── sec/                        # SEC EDGAR adapter (self-contained)
│   ├── manifest.py             # SEC_MANIFEST (plans, rates, endpoints, credits)
│   ├── client/                 # EDGAR API, EFTS, Arelle, Downloader
│   ├── processors/             # XBRL processing, metadata, ingestion
│   │   ├── metadata.py         # SECMetadataLoader with caching
│   │   ├── xbrl_graph.py       # XBRLGraphProcessor
│   │   ├── processing.py       # Single filing processing
│   │   ├── consolidation.py    # Parquet consolidation
│   │   └── ingestion/          # DuckDB/LadybugDB ingestion
│   └── pipeline/               # Dagster orchestration
│       ├── __init__.py         # get_dagster_components() discovery
│       ├── configs.py          # Run configurations
│       ├── download.py         # sec_raw_filings asset
│       ├── process.py          # sec_processed_filings asset
│       ├── stage.py            # DuckDB staging assets
│       ├── materialize.py      # LadybugDB materialization
│       ├── artifact.py         # Precomputed Parquet knowledge artifacts
│       ├── text_index.py       # OpenSearch text indexing (textblocks, narratives, iXBRL)
│       ├── s3_publish.py       # Publish .lbug to S3 for replicas
│       ├── duckdb_s3_publish.py # Publish .duckdb staging DBs to S3
│       ├── r2_publish.py       # Publish .lbug to Cloudflare R2 (zero-egress downloads)
│       ├── entity_sync/        # Entity metadata sync assets
│       ├── jobs.py             # Job definitions
│       └── sensors.py          # Sensors + schedule
└── quickbooks/                 # QuickBooks adapter
    ├── client/                 # QB OAuth client
    ├── dbt/                    # dbt transforms (JSON → graph-shaped Parquet)
    └── pipeline/               # Dagster extract/transform/load assets

robosystems/dagster/            # Platform orchestration (collector)
├── definitions.py              # Collects platform + adapter pipelines
├── reporting.py                # Report asset materializations from outside Dagster jobs
├── resources/                  # Shared Dagster resources (DB, S3, Graph)
├── assets/
│   ├── graphs.py               # User graph operation assets
│   └── shared_repositories/    # S3 publish + replica refresh
├── jobs/                       # Platform jobs (billing, infrastructure, graph, backups)
└── sensors/
    ├── graph_lifecycle.py      # Provisioning, expiry, suspension, deprovisioning
    ├── materialization.py      # Stale-graph rebuild
    ├── invoice_billing.py      # Subscription renewal + invoicing
    ├── scheduled_obligation_promoter.py
    ├── usage_monitor.py
    └── worker_reaper.py
```

The platform side carries far more than the SEC publish-and-refresh chain this page traces: scheduled billing, instance health, registry and storage cleanup, the nightly fleet backup (`0 3 * * *`, fanned out one run per customer graph), and the sensors above. [`dagster/README.md`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/dagster/README.md) carries the full job and sensor inventory with each one's cron or trigger; treat it, not this page, as the list.

---

## Dagster Architecture

Dagster runs on ECS Fargate (orchestration only). Heavy compute happens on the shared master via Graph API.

```
┌──────────────────────────────────────────────────────────────┐
│  DAGSTER (ECS Fargate) - Orchestration                       │
│  ├── Daemon: 1024 CPU / 2048 MB (singleton, runs migrations) │
│  ├── Webserver: 512 CPU / 1024 MB (desired count 0 by        │
│  │     default; scaled to 1 on demand for tunnel access)     │
│  ├── Capacity: 80% SPOT + 20% On-Demand                      │
│  └── Jobs: Download, process Arelle, call Graph API          │
├──────────────────────────────────────────────────────────────┤
│  SHARED MASTER (graph-ladybug.yaml) - Heavy Compute          │
│  ├── Memory-optimized EC2 + dynamic EBS (Volume Mgr Lambda)  │
│  ├── DuckDB staging + LadybugDB materialization              │
│  ├── node_type: shared_master (in DynamoDB registry)         │
│  └── Single source of truth for shared repositories          │
├──────────────────────────────────────────────────────────────┤
│  SHARED REPLICAS (graph-ladybug-replicas.yaml)               │
│  ├── ASG: Min=2, Max=10, TargetTracking on CPU               │
│  ├── S3 download: Pull .lbug + .duckdb from S3 on boot      │
│  ├── Read-only; sized below the master (no DuckDB staging)   │
│  └── ALB on port 8001, health check /health                  │
└──────────────────────────────────────────────────────────────┘
```

**Key insight**: Dagster orchestrates (lightweight), Graph API computes (heavy). This keeps Dagster simple and reuses existing graph infrastructure.

Instance types are deliberately omitted above — they live in
[`.github/configs/graph.yml`](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml),
which the GHA deployment layer provisions from directly. The diagram carries the shape; the config
carries the sizes, and it has been resized more than once.

### What Dagster Does vs What Graph API Does

| Task                      | Dagster (Fargate)  | Graph API (Shared Master) |
| ------------------------- | ------------------ | ------------------------- |
| Download XBRL             | ✅ Orchestrates    | -                         |
| Process with Arelle       | ✅ Runs on Fargate | -                         |
| Parquet to S3             | ✅ Handles         | -                         |
| DuckDB staging            | -                  | ✅ Graph API handles      |
| LadybugDB materialization | -                  | ✅ Graph API handles      |
| S3 publish (.lbug/.duckdb)| ✅ Orchestrates    | ✅ Uploads files          |
| Replica refresh           | ✅ AWS API calls   | -                         |

### S3 Publish → Replica Refresh Flow

```
1. Dagster sensor: sec_post_materialize_publish_sensor
   ├── Publish .lbug to S3 (LadybugDB graph database)
   └── Publish .duckdb to S3 (DuckDB with embeddings for vector search)

2. Dagster sensor: triggers shared_repository_refresh_replicas_job
   └── Rolling ASG instance refresh (min_healthy=100%, max_healthy=200%)

3. New replicas boot alongside old ones
   ├── Download .lbug + .duckdb from S3 (~15 min for ~85GB)
   ├── Start Graph API, pass health check
   └── Register with ALB, old instance terminated
```

---

## Adding a Shared Repository

> The consumer-facing concept — the `ladybug-shared` tier, subscription plans, and how a user subscribes to and queries a shared repository — is covered in [Shared Repositories](Shared-Repositories). This section is the producer side: defining one.

Shared repositories (like SEC) are defined by adapter manifests. Each manifest declares everything about a repo: identity, data source, schema, rate limits, plans/pricing, endpoint access, and credit costs.

**To add a new shared repository:**

1. Create `adapters/{name}/manifest.py` with a `SharedRepositoryManifest` (see `adapters/sec/manifest.py` as a template)
2. Add one import + `_register()` call to `config/shared_repositories.py` → `_load_manifests()`

No billing config files, no DB migrations, no hardcoded lists to update.

**Registry** (`config/shared_repositories.py`): Lazy-loads manifests on first access. Provides the query API used by billing, middleware, and operations — `get_manifest()`, `get_plan_details()`, `get_rate_limits()`, `is_shared_repository()`, etc.

See [PR #308](https://github.com/RoboFinSystems/robosystems/pull/308) and [RFC #191](https://github.com/RoboFinSystems/robosystems/issues/191) for the broader extensible adapter design.

## Adding Custom Data Sources

**The supported route is an integration, not an in-core adapter**: a program in its own repository that writes through the public API — see [Building Custom Integrations](Building-Custom-Integrations) and the [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template). Integrations survive every platform release, work against managed and self-hosted deployments alike, and need no fork. The in-core adapter registry (this page's pipelines) is maintained exclusively by the platform team — platform-operated deployments run an unmodified core.

For **self-hosted forks only**, the `custom_*/` namespace remains available as a merge boundary for in-core additions:

1. Create `adapters/custom_myservice/` with `client/`, `processors/`, and `pipeline/`
2. Implement `pipeline/__init__.py` with `get_dagster_components()` returning `{"assets": [...], "jobs": [...], "sensors": [...], "schedules": [...]}`
3. Import and collect in `dagster/definitions.py` (see the `# === FORK` comment)

See the [Adapters README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md) for details.

---

## Unified Ingestion Pattern

All pipelines use DuckDB staging regardless of source. Patterns A and B enter staging from S3 Parquet; Pattern C enters staging via `postgres_scanner` reading the extensions database directly.

```
┌─────────────────────────────────────────────────────────────────┐
│              UNIFIED INGESTION (ALL PIPELINES)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Pattern A/B: Parquet in S3  ──┐                                │
│  Pattern C:   Extensions DB  ──┤                                │
│                                │                                │
│                                ▼                                │
│  1. GraphFile.create()                                          │
│     Register file/scan in PostgreSQL (provenance tracking)      │
│           │                                                     │
│           ▼                                                     │
│  2. client.create_table(s3_files=[...])  or postgres_scan(...)  │
│     Load source → DuckDB staging table (queryable for debug)    │
│           │                                                     │
│           ▼                                                     │
│  3. client.materialize_table(file_ids=[...])                    │
│     DuckDB → LadybugDB (incremental by file_id)                 │
│           │                                                     │
│           ▼                                                     │
│  4. GraphFile.mark_graph_ingested()                             │
│     Track completion in PostgreSQL                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Why DuckDB:**

- Validation layer before graph ingestion
- Handles S3 Parquet natively (httpfs extension) and PostgreSQL natively (postgres_scanner extension)
- Same pattern for SEC, QuickBooks, custom adapters, and platform-owned OLTP
- Scale proof: If it handles SEC (1TB+), it handles any company's data

**Graph API endpoints:**

- `POST /databases/{graph_id}/tables` - Create staging table
- `POST /databases/{graph_id}/tables/query` - SQL validation
- `POST /databases/{graph_id}/tables/{table_name}/materialize` - Materialize to graph

---

## Company Graphs vs Shared Repositories

| Aspect        | Shared Repos (SEC)       | Company Graphs (QuickBooks) |
| ------------- | ------------------------ | --------------------------- |
| Write pattern | Nightly incremental + rebuild | Incremental daily      |
| Read pattern  | High volume, cacheable   | Low volume, fresh           |
| Scaling       | Horizontal (replicas)    | Vertical (bigger instance)  |
| Data size     | 1TB+                     | 1-100GB                     |
| DuckDB role   | Transport (full rebuild) | Staging (incremental)       |

---

## Compute Infrastructure

| Component                  | Approach      | EBS Pattern           | Reason                                              |
| -------------------------- | ------------- | --------------------- | --------------------------------------------------- |
| **Dagster (all jobs)**     | ECS Fargate   | None (ephemeral)      | Orchestration only. Heavy compute via Graph API.    |
| **Shared master**          | Raw EC2 + ASG | Dynamic (Vol Manager) | Graph API handles DuckDB + LadybugDB.               |
| **Customer graph writers** | Raw EC2 + ASG | Dynamic per-customer  | Volume Manager assigns EBS per customer allocation. |
| **Shared replicas**        | Raw EC2 + ASG | S3 download at boot   | All replicas identical. Download .lbug/.duckdb from S3.|

### Why Fargate for Dagster

**Dagster's role is orchestration, not compute:**

- Download XBRL files (small, fits in Fargate's 200GB ephemeral)
- Process with Arelle → parquet (CPU-bound, Fargate handles)
- Upload parquet to S3 (network I/O)
- Call Graph API for materialization (HTTP call, master does work)
- AWS API calls for replica refresh (lightweight)

**Benefits:**
- Simpler infrastructure (no EC2 capacity provider)
- No EBS management for Dagster
- Scale to zero automatically
- Cost efficient (pay only for orchestration time)

### Why Raw EC2 for Graph Instances

**Graph instances need:**
- Large EBS volumes (100GB-1TB+)
- Volume Manager Lambda coordination (dynamic assignment)
- Long-running processes (Graph API serving requests)

**ECS doesn't help here** - core complexity (EBS management) remains regardless.

---

## Key Architectural Decisions

| Decision                          | Rationale                                                                       |
| --------------------------------- | ------------------------------------------------------------------------------- |
| **Dagster for all orchestration** | One system (eliminated Celery), one UI for pipelines + billing + infrastructure |
| **Fargate-only Dagster**          | Dagster orchestrates, Graph API computes; no EC2 capacity provider complexity   |
| **DuckDB-only staging**           | Standardized validation layer for all pipelines                                 |
| **Three pipeline patterns**       | Arelle for XBRL semantics (SEC), dbt for API JSON transforms (QuickBooks), postgres_scanner for OLTP materialization |
| **GitHub repos for adapters**     | Customization is the value prop; dbt projects don't fit pip                     |
| **S3 publish for replicas**       | Dagster publishes to S3, triggers rolling refresh; replicas download on boot    |

---

## Lambda Functions

Some functions must stay as Lambda (event-driven, not orchestration):

| Lambda                       | Reason                                               |
| ---------------------------- | ---------------------------------------------------- |
| `graph_volume_manager.py`    | Called synchronously from EC2 userdata during boot   |
| `graph_volume_monitor.py`    | Triggered by CloudWatch alarms (SNS)                 |
| `graph_volume_detachment.py` | ASG lifecycle hook handler                           |
| `postgres_rotation.py`       | AWS Secrets Manager rotation                         |
| `valkey_rotation.py`         | AWS Secrets Manager rotation                         |
| `api_key_rotation.py`        | AWS Secrets Manager rotation                         |

---

## Reference

- Dagster README: `/robosystems/dagster/README.md`
- SEC Pipeline README: `/robosystems/adapters/sec/pipeline/README.md`
- Adapter READMEs: `/robosystems/adapters/*/README.md`
- CloudFormation: `/cloudformation/dagster.yaml`, `/cloudformation/graph-ladybug-replicas.yaml`
