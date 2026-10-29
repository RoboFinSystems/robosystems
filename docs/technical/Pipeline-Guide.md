# Pipeline Guide

RoboSystems uses [Dagster](https://dagster.io) to orchestrate every pipeline that feeds the knowledge graph: SEC XBRL filings, connector syncs, and ledger writes. Data flows from external sources through adapters and DuckDB staging into the knowledge graph (LadybugDB); connector data reaches the graph by way of the extensions PostgreSQL database.

**Related documentation:**
- [Architecture Overview](Architecture-Overview.md) - System architecture
- [Bootstrap Guide](Bootstrap-Guide.md) - Deployment instructions
- [Custom Graph Schema](Custom-Graph-Schema.md) - Define a custom graph schema and load data into it through the staging path
- [Building Custom Integrations](Building-Custom-Integrations.md) - Connect a custom data source through the public API, outside the core
- [SEC XBRL Pipeline](SEC-XBRL-Pipeline.md) - The SEC ingestion pipeline in depth

---

## Current Pipelines

| Pipeline   | Status           | Flag                            | Pattern                                   |
| ---------- | ---------------- | ------------------------------- | ----------------------------------------- |
| SEC        | Production-ready | `SEC_PIPELINE_ENABLED`          | [A](#pattern-a-arelle-based-sec)          |
| QuickBooks | Production-ready | `CONNECTION_QUICKBOOKS_ENABLED` | [B](#pattern-b-dbt-based-quickbooks), then C |
| Mercury    | Off by default   | `CONNECTION_MERCURY_ENABLED`    | [Bank feed](#bank-feeds-mercury-plaid), then C |
| Plaid      | Off by default   | `CONNECTION_PLAID_ENABLED`      | [Bank feed](#bank-feeds-mercury-plaid), then C |

`dagster/definitions.py` loads a pipeline's assets and jobs only when its flag is on. The code defaults are on for SEC and QuickBooks and off for the two bank feeds; a fresh deployment seeded by `just setup-aws` starts with all four off (see [Bootstrap Guide → Turn on what you need](Bootstrap-Guide.md#turn-on-what-you-need)).

One more connector is live but is not a pipeline in this sense: **`external`** ("External Integration") registers a source namespace on a graph for an integration the platform does not run. It is registration and telemetry rather than execution config — the integration itself runs outside the platform, holds its own source credentials, and writes through the public API. That is the connector side of the [integration lane](#adding-custom-data-sources) below; the implementation is `operations/providers/external_provider.py`.

The authoritative, live list of connectors is served rather than documented: `GET /v1/graphs/{graph_id}/connections/options` returns each provider with its configuration and supported features.

---

## Three Pipeline Patterns

RoboSystems uses three distinct patterns based on data source characteristics. Pattern A writes an external source (SEC) straight into the graph. Pattern B and the bank feeds land external data in the extensions PostgreSQL database, beside the platform's own OLTP writes; Pattern C materializes that database into the graph for analytical queries.

### Pattern A: Arelle-Based (SEC)

```
XBRL Files → Arelle Processing → Parquet → DuckDB Staging → LadybugDB
            (semantic extraction)   (already graph-shaped)
```

- **Used for**: SEC EDGAR filings (XBRL format)
- **Why**: Arelle extracts XBRL semantics (concepts, contexts, facts) directly into graph-compatible structures
- **No dbt needed**: Output is already graph-shaped

### Pattern B: dbt-Based (QuickBooks)

```
QB API → Parquet → dbt-duckdb → Extensions PostgreSQL → (Pattern C) → LadybugDB
        (extract)  (transform)  (load: the graph's schema)
```

- **Used for**: QuickBooks, and the model for a self-hosted fork's API-based `custom_*` adapters. A custom source on a platform-operated deployment is an integration instead — see [Adding Custom Data Sources](#adding-custom-data-sources)
- **Assets** (job `qb_sync`): `qb_extract` pulls the QuickBooks API into raw Parquet in a per-graph work directory; `qb_transform` runs the dbt project (`adapters/quickbooks/dbt/`) over it with dbt-duckdb, producing the ledger's OLTP tables in a DuckDB file; `qb_load` upserts those rows into the graph's schema in the extensions database (`operations/extensions/loader.py`) and marks the graph stale
- **Why**: QuickBooks data becomes ledger rows — events, entries, agents — in the same PostgreSQL schema the ledger's own writes use, so reads, close and reconciliations see one book. The graph is a projection of that schema, rebuilt by Pattern C
- **Trigger**: On demand — a sync request (`POST /v1/graphs/{graph_id}/connections/{connection_id}/sync`). There is no schedule
- **dbt provides**: SQL-based transforms, testing, documentation

### Bank feeds (Mercury, Plaid)

```
Bank API → capture into the Inbox (extensions PostgreSQL) → (Pattern C) → LadybugDB
```

- **Off by default**: each feed is behind its own flag (`CONNECTION_MERCURY_ENABLED`, `CONNECTION_PLAID_ENABLED`) and needs its provider credentials in the deployment's secret
- **One asset each**: `mercury_feed` (job `mercury_sync`) and `plaid_feed` (job `plaid_sync`) pull the feed, link or create a chart account per bank account, and capture each posted transaction as an event in the ledger's Inbox with a suggested account. No dbt and no DuckDB; the shared contract lives in `adapters/bank_feed/`
- **Nothing posts on capture**: a captured line reaches the books only once it is classified and committed. The sync marks the graph stale, and Pattern C carries the change into the graph
- **Trigger**: On demand, the same sync request as QuickBooks

### Pattern C: OLTP-Based (Extensions Materialization)

```
Extensions PostgreSQL → postgres_scanner → DuckDB Staging → LadybugDB
(schema-per-graph)      (direct scan)      (graph-shaped)
```

- **Used for**: Everything in the extensions database — RoboLedger and RoboInvestor writes (schedules, closing entries, reports, portfolio positions, anything a user or AI writes through the `/extensions/*/operations/*` command surface), plus the rows QuickBooks and the bank feeds load there
- **Why**: The extensions database is the system of record for a company graph; the graph is its analytical projection, so queries can see ledger data alongside SEC data
- **Trigger**: Event-driven rather than scheduled. Each write calls `mark_graph_stale`; the stale-graph sensor (`dagster/sensors/materialization.py`) batches bursts of writes — it waits for them to settle, a few minutes at most — and submits one materialization. `POST /v1/graphs/{graph_id}/operations/materialize` runs one on demand
- **Blue-green**: an existing graph is rebuilt whole into a fresh copy and swapped in, so readers never see a half-built graph; a graph's first build goes in place
- **No Parquet intermediate**: DuckDB's `postgres_scanner` reads the extensions schema directly — each staging table is a `CREATE TABLE ... AS SELECT ... FROM postgres_scan(...)` shaped to the graph schema — skipping the S3 round-trip Pattern A needs
- **Not an adapter**: there's no `adapters/extensions/` directory — this pipeline is platform-internal. The materializer is `operations/extensions/materialize.py`, beside `loader.py` (the connector load) and `staleness.py`; see also [RoboLedger Operations](RoboLedger-Operations.md)

---

## Adapter Structure

Each adapter is self-contained: client, processors, and pipeline (Dagster orchestration) all live together. `dagster/definitions.py` collects adapter pipelines via the `get_dagster_components()` discovery pattern, each behind its flag.

```
robosystems/adapters/
├── base.py          # SharedRepositoryManifest dataclass
├── sec/             # SEC EDGAR: manifest, clients, processors, pipeline/
├── quickbooks/      # QuickBooks: OAuth client, dbt/ project, pipeline/ (extract → transform → load)
├── bank_feed/       # Shared bank-feed contract: account linking, capture, sync bookkeeping
├── mercury/         # Mercury bank feed: client/, pipeline/
└── plaid/           # Plaid bank feed: client/, pipeline/

robosystems/dagster/ # Platform orchestration (collector)
├── definitions.py   # Collects platform + adapter pipelines
├── reporting.py     # Report asset materializations from outside Dagster jobs
├── resources/       # Shared Dagster resources (DB, S3, Graph)
├── assets/          # Graph operation assets, shared-repository publish + replica refresh
├── jobs/            # Platform jobs and their schedules
└── sensors/         # Platform sensors (graph lifecycle, stale-graph rebuild, billing, …)
```

The trees inside each package change often, so they are documented beside the code: the [Adapters README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md) for each adapter, the [SEC pipeline README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/sec/pipeline/README.md) for the SEC assets, and [`dagster/README.md`](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/dagster/README.md) for the platform side. The platform side carries far more than the SEC publish-and-refresh chain this page traces: scheduled billing, instance health, registry and storage cleanup, the nightly fleet backup (`0 3 * * *`, fanned out one run per customer graph), and the sensors. `dagster/README.md` carries the full job and sensor inventory with each one's cron or trigger; treat it, not this page, as the list.

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
│  ├── ASG: sized by SHARED_REPLICAS_* variables; target       │
│  │     tracking on CPU and memory                            │
│  ├── S3 download: Pull .lbdb + .duckdb from S3 on boot       │
│  ├── Read-only; sized below the master (no DuckDB staging)   │
│  └── ALB on port 8001, health check /health                  │
└──────────────────────────────────────────────────────────────┘
```

**Key insight**: Dagster orchestrates (lightweight), Graph API computes (heavy). This keeps Dagster simple and reuses existing graph infrastructure.

Instance types and replica counts are deliberately omitted above. Replica counts come from the `SHARED_REPLICAS_*` GitHub variables (`SHARED_REPLICAS_MIN_INSTANCES_*`, `SHARED_REPLICAS_MAX_INSTANCES_*`, `SHARED_REPLICAS_DESIRED_CAPACITY_*`); instance types live in
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
| S3 publish (.lbdb/.duckdb)| ✅ Orchestrates    | ✅ Uploads files          |
| Replica refresh           | ✅ AWS API calls   | -                         |

### S3 Publish → Replica Refresh Flow

```
1. Dagster sensor: sec_post_materialize_publish_sensor
   ├── Publish .lbdb to S3 (LadybugDB graph database)
   └── Publish .duckdb to S3 (DuckDB with embeddings for vector search)

2. Dagster sensor: triggers shared_repository_refresh_replicas_job
   └── Rolling ASG instance refresh (min_healthy=100%, max_healthy=200%)

3. New replicas boot alongside old ones
   ├── Download .lbdb + .duckdb from S3 (~15 min for ~85GB)
   ├── Start Graph API, pass health check
   └── Register with ALB, old instance terminated
```

---

## Adding a Shared Repository

> The consumer-facing concept — the `ladybug-shared` tier, subscription plans, and how a user subscribes to and queries a shared repository — is covered in [Shared Repositories](Shared-Repositories.md). This section is the producer side: defining one.

Shared repositories (like SEC) are defined by adapter manifests. Each manifest declares everything about a repo: identity, data source, schema, rate limits, plans/pricing, endpoint access, and credit costs.

**To add a new shared repository:**

1. Create `adapters/{name}/manifest.py` with a `SharedRepositoryManifest` (see `adapters/sec/manifest.py` as a template)
2. Add one import + `_register()` call to `config/shared_repositories.py` → `_load_manifests()`

No billing config files, no DB migrations, no hardcoded lists to update.

**Registry** (`config/shared_repositories.py`): Lazy-loads manifests on first access. Provides the query API used by billing, middleware, and operations — `get_manifest()`, `get_plan_details()`, `get_rate_limits()`, `is_shared_repository()`, etc.

See [PR #308](https://github.com/RoboFinSystems/robosystems/pull/308) and [RFC #191](https://github.com/RoboFinSystems/robosystems/issues/191) for the broader extensible adapter design.

## Adding Custom Data Sources

**The supported route is an integration, not an in-core adapter**: a program in its own repository that writes through the public API — see [Building Custom Integrations](Building-Custom-Integrations.md) and the [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template). Integrations survive every platform release, work against managed and self-hosted deployments alike, and need no fork. The in-core adapter registry (this page's pipelines) is maintained exclusively by the platform team — platform-operated deployments run an unmodified core.

For **self-hosted forks only**, the `custom_*/` namespace remains available as a merge boundary for in-core additions:

1. Create `adapters/custom_myservice/` with `client/`, `processors/`, and `pipeline/`
2. Implement `pipeline/__init__.py` with `get_dagster_components()` returning `{"assets": [...], "jobs": [...], "sensors": [...], "schedules": [...]}`
3. Import and collect in `dagster/definitions.py` (see the `# FORK:` comment)

See the [Adapters README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md) for details.

---

## Unified Ingestion Pattern

All pipelines use DuckDB staging regardless of source. Parquet in S3 — the SEC pipeline, and files uploaded to a graph through the [staging path](Custom-Graph-Schema.md) — enters staging through `httpfs`; the extensions database (Pattern C, which carries QuickBooks, the bank feeds and every ledger write) enters through `postgres_scanner`.

```
┌─────────────────────────────────────────────────────────────────┐
│              UNIFIED INGESTION (ALL PIPELINES)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Parquet in S3 (SEC, file uploads)  ──┐                         │
│  Extensions PostgreSQL (Pattern C)  ──┤                         │
│                                       ▼                         │
│  1. Stage: source → DuckDB staging table (queryable for debug)  │
│     Parquet:    client.create_table(s3_files=[...]); an         │
│                 uploaded file is registered first (GraphFile)   │
│     Pattern C:  CREATE TABLE ... AS SELECT ...                  │
│                   FROM postgres_scan(...)                       │
│           │                                                     │
│           ▼                                                     │
│  2. client.materialize_table(...)                               │
│     DuckDB → LadybugDB                                          │
│     Uploads:    incremental by file_id                          │
│     Pattern C:  the whole projection, blue-green                │
│           │                                                     │
│           ▼                                                     │
│  3. Record completion                                           │
│     Uploads:    GraphFile.mark_graph_ingested()                 │
│     Pattern C:  clear the graph's stale flag                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**Why DuckDB:**

- Validation layer before graph ingestion
- Handles S3 Parquet natively (httpfs extension) and PostgreSQL natively (postgres_scanner extension)
- Same pattern for SEC, uploaded files, and everything in the extensions database (QuickBooks, bank feeds, ledger writes)
- Scale proof: If it handles SEC (1TB+), it handles any company's data

**Graph API endpoints:**

- `POST /databases/{graph_id}/tables` - Create staging table
- `POST /databases/{graph_id}/tables/query` - SQL validation
- `POST /databases/{graph_id}/tables/{table_name}/materialize` - Materialize to graph

---

## Company Graphs vs Shared Repositories

| Aspect        | Shared Repos (SEC)       | Company Graphs (ledger, QuickBooks, bank feeds) |
| ------------- | ------------------------ | ----------------------------------------------- |
| Write pattern | Nightly incremental + rebuild | On-demand connector syncs and ledger writes into PostgreSQL; the graph is rebuilt blue-green, batched by the stale-graph sensor |
| Read pattern  | High volume, cacheable   | Low volume, fresh           |
| Scaling       | Horizontal (replicas)    | Vertical (bigger instance)  |
| Data size     | 1TB+                     | 1-100GB                     |
| DuckDB role   | Transport (full rebuild) | Staging (`postgres_scan` of the graph's schema) |

---

## Compute Infrastructure

| Component                  | Approach      | EBS Pattern           | Reason                                              |
| -------------------------- | ------------- | --------------------- | --------------------------------------------------- |
| **Dagster (all jobs)**     | ECS Fargate   | None (ephemeral)      | Orchestration only. Heavy compute via Graph API.    |
| **Shared master**          | Raw EC2 + ASG | Dynamic (Vol Manager) | Graph API handles DuckDB + LadybugDB.               |
| **Customer graph writers** | Raw EC2 + ASG | Dynamic per-customer  | Volume Manager assigns EBS per customer allocation. |
| **Shared replicas**        | Raw EC2 + ASG | S3 download at boot   | All replicas identical. Download .lbdb/.duckdb from S3.|

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
| **Three pipeline patterns**       | Arelle for XBRL semantics (SEC); dbt-duckdb to turn API JSON into ledger rows in PostgreSQL (QuickBooks); postgres_scanner to materialize the extensions database — ledger writes, QuickBooks, bank feeds — into the graph |
| **PostgreSQL before the graph**   | Connector data lands where the ledger's own writes live, so one book serves reads, close and reconciliations; the graph is a rebuildable projection of it |
| **Custom sources are integrations** | A custom source lives in its own repository and writes through the public API; the in-core adapter registry is platform-maintained, so every deployment runs an unmodified core and upgrades cleanly |
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
