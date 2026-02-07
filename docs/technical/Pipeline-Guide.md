# Pipeline Guide

RoboSystems uses [Dagster](https://dagster.io) for all data orchestration. Data flows from external sources through adapters, staging (DuckDB), and into the knowledge graph (LadybugDB).

**Related documentation:**
- [Architecture Overview](Architecture-Overview.md) - System architecture
- [Bootstrap Guide](Bootstrap-Guide.md) - Deployment instructions

---

## Current Pipelines

| Pipeline   | Status           | Tracking                                                                 |
| ---------- | ---------------- | ------------------------------------------------------------------------ |
| SEC        | Production-ready | [#117](https://github.com/RoboFinSystems/robosystems/issues/117)         |
| QuickBooks | In Development   | [#118](https://github.com/RoboFinSystems/robosystems/issues/118)         |
| Plaid      | Scaffolded       | Coming soon                                                              |

**Infrastructure:**
- Shared replicas deployment: [#113](https://github.com/RoboFinSystems/robosystems/issues/113)

---

## Two Pipeline Patterns

RoboSystems uses two distinct patterns based on data source characteristics:

### Pattern A: Arelle-Based (SEC)

```
XBRL Files → Arelle Processing → Parquet → DuckDB Staging → LadybugDB
            (semantic extraction)   (already graph-shaped)
```

- **Used for**: SEC EDGAR filings (XBRL format)
- **Why**: Arelle extracts XBRL semantics (concepts, contexts, facts) directly into graph-compatible structures
- **No dbt needed**: Output is already graph-shaped

### Pattern B: dbt-Based (QuickBooks, Plaid, Custom)

```
API JSON → S3 Raw → dbt transforms → S3 Processed → DuckDB Staging → LadybugDB
          (chunks)  (JSON → Parquet)  (graph-shaped)
```

- **Used for**: API-based integrations (QuickBooks, Plaid, custom ERPs)
- **Why**: Raw JSON needs transformation into graph-compatible node/relationship Parquet
- **dbt provides**: SQL-based transforms, testing, documentation

---

## Adapter Structure

```
robosystems/adapters/
├── base.py                     # SharedRepositoryManifest dataclass
├── sec/                        # SEC EDGAR adapter (active)
│   ├── manifest.py             # SEC_MANIFEST (plans, rates, endpoints, credits)
│   ├── client/                 # EDGAR API, EFTS, Arelle, Downloader
│   ├── metadata.py             # SECMetadataLoader with caching
│   └── processors/             # XBRL processing
│       ├── constants.py        # Shared constants (SHARED_NODE_TABLES)
│       ├── xbrl_graph.py       # XBRLGraphProcessor
│       ├── processing.py       # Single filing processing
│       ├── consolidation.py    # Parquet consolidation
│       └── ingestion/          # DuckDB/LadybugDB ingestion
├── quickbooks/                 # QuickBooks adapter (stubbed)
│   ├── client/                 # QB OAuth client
│   └── processors/             # Transaction processing (stubbed)
└── plaid/                      # Plaid adapter (stubbed)
    ├── client/                 # Plaid API client
    └── processors/             # Transaction sync (stubbed)

robosystems/dagster/
├── assets/
│   ├── graphs.py               # User graph operation assets
│   └── sec/                    # SEC pipeline assets (modular)
│       ├── configs.py          # Configuration classes
│       ├── download.py         # sec_raw_filings
│       ├── process.py          # sec_processed_filings
│       ├── stage.py            # DuckDB staging assets
│       ├── materialize.py      # LadybugDB materialization
│       ├── entity_update.py    # Entity incremental update
│       └── backup.py           # SEC backup asset
├── jobs/                       # 30+ job definitions
├── sensors/                    # Event-driven triggers
└── resources/                  # Database, S3, Graph resources
```

---

## Dagster Architecture

Dagster runs on ECS Fargate (orchestration only). Heavy compute happens on the shared master via Graph API.

```
┌──────────────────────────────────────────────────────────────┐
│  DAGSTER (ECS Fargate) - Orchestration                       │
│  ├── Daemon: 512 CPU / 1024 MB (singleton, runs migrations)  │
│  ├── Webserver: 512 CPU / 1024 MB (1-3 replicas, auto-scale) │
│  ├── Capacity: 80% SPOT + 20% On-Demand                      │
│  └── Jobs: Download, process Arelle, call Graph API          │
├──────────────────────────────────────────────────────────────┤
│  SHARED MASTER (graph-ladybug.yaml) - Heavy Compute          │
│  ├── EC2 r7g.large with dynamic EBS (Volume Manager Lambda)  │
│  ├── DuckDB staging + LadybugDB materialization              │
│  ├── node_type: shared_master (in DynamoDB registry)         │
│  └── Single source of truth for shared repositories          │
├──────────────────────────────────────────────────────────────┤
│  SHARED REPLICAS (graph-ladybug-replicas.yaml)               │
│  ├── ASG: Min=2, Max=10, TargetTracking on CPU               │
│  ├── LaunchTemplate: SnapshotId updated by Dagster job       │
│  ├── Instance: r7g.medium (read-only, smaller than master)   │
│  └── ALB on port 8001, health check /status                  │
└──────────────────────────────────────────────────────────────┘
```

**Key insight**: Dagster orchestrates (lightweight), Graph API computes (heavy). This keeps Dagster simple and reuses existing graph infrastructure.

### What Dagster Does vs What Graph API Does

| Task                      | Dagster (Fargate)  | Graph API (Shared Master) |
| ------------------------- | ------------------ | ------------------------- |
| Download XBRL             | ✅ Orchestrates    | -                         |
| Process with Arelle       | ✅ Runs on Fargate | -                         |
| Parquet to S3             | ✅ Handles         | -                         |
| DuckDB staging            | -                  | ✅ Graph API handles      |
| LadybugDB materialization | -                  | ✅ Graph API handles      |
| EBS snapshots             | ✅ AWS API calls   | Source volume             |
| Replica refresh           | ✅ AWS API calls   | -                         |

### Snapshot → Replica Flow

```
1. Dagster job: create_snapshot()
   └── EBS snapshot of master's volume (tagged with run_id)

2. Dagster job: update_replica_launch_template()
   └── Update ASG launch template with new SnapshotId

3. Dagster job: refresh_replica_instances()
   └── Trigger rolling instance refresh (ASG replaces instances)

4. New replicas boot with updated data
   └── Mount snapshot, start container, register with ALB
```

---

## Adding a Shared Repository

Shared repositories (like SEC) are defined by adapter manifests. Each manifest declares everything about a repo: identity, data source, schema, rate limits, plans/pricing, endpoint access, and credit costs.

**To add a new shared repository:**

1. Create `adapters/{name}/manifest.py` with a `SharedRepositoryManifest` (see `adapters/sec/manifest.py` as a template)
2. Add one import + `_register()` call to `config/shared_repositories.py` → `_load_manifests()`

No billing config files, no DB migrations, no hardcoded lists to update.

**Registry** (`config/shared_repositories.py`): Lazy-loads manifests on first access. Provides the query API used by billing, middleware, and operations — `get_manifest()`, `get_plan_details()`, `get_rate_limits()`, `is_shared_repository()`, etc.

See [PR #308](https://github.com/RoboFinSystems/robosystems/pull/308) and [RFC #191](https://github.com/RoboFinSystems/robosystems/issues/191) for the broader extensible adapter design.

## Adding Custom Adapters

The `custom_*/` namespace is reserved for fork additions that won't conflict with upstream updates. See [RFC #191](https://github.com/RoboFinSystems/robosystems/issues/191) for the full `SourceAdapter` protocol design.

---

## Unified Ingestion Pattern

All pipelines use DuckDB staging regardless of source:

```
┌─────────────────────────────────────────────────────────────────┐
│              UNIFIED INGESTION (ALL PIPELINES)                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ANY Pipeline Output (Parquet in S3)                            │
│           │                                                     │
│           ▼                                                     │
│  1. GraphFile.create()                                          │
│     Register file in PostgreSQL (provenance tracking)           │
│           │                                                     │
│           ▼                                                     │
│  2. client.create_table(s3_files=[...])                         │
│     Load parquet → DuckDB staging table (queryable for debug)   │
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
- Handles S3 Parquet natively (httpfs extension)
- Same pattern for SEC, QuickBooks, Plaid, custom
- Scale proof: If it handles SEC (1TB+), it handles any company's data

**Graph API endpoints:**

- `/databases/{graph_id}/tables` - Create staging table
- `/databases/{graph_id}/tables/query` - SQL validation
- `/databases/{graph_id}/tables/{name}/ingest` - Materialize to graph

---

## Company Graphs vs Shared Repositories

| Aspect        | Shared Repos (SEC)       | Company Graphs (QuickBooks) |
| ------------- | ------------------------ | --------------------------- |
| Write pattern | Rebuild weekly/monthly   | Incremental daily           |
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
| **Shared replicas**        | Raw EC2 + ASG | Snapshot-restored     | All replicas identical. EBS from snapshot at launch.|

### Why Fargate for Dagster

**Dagster's role is orchestration, not compute:**

- Download XBRL files (small, fits in Fargate's 200GB ephemeral)
- Process with Arelle → parquet (CPU-bound, Fargate handles)
- Upload parquet to S3 (network I/O)
- Call Graph API for materialization (HTTP call, master does work)
- AWS API calls for snapshots/replicas (lightweight)

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
| **Two pipeline patterns**         | Arelle for XBRL semantics (SEC), dbt for API JSON transforms (QB/Plaid)         |
| **GitHub repos for adapters**     | Customization is the value prop; dbt projects don't fit pip                     |
| **EBS snapshots for replicas**    | Dagster creates snapshot, updates ASG launch template, triggers refresh         |

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
| `graph_api_rotation.py`      | AWS Secrets Manager rotation                         |
| `postgres_init.py`           | RDS event handler (secrets + database creation)      |

---

## Reference

- Dagster README: `/robosystems/dagster/README.md`
- Adapter READMEs: `/robosystems/adapters/*/README.md`
- CloudFormation: `/cloudformation/dagster.yaml`, `/cloudformation/graph-ladybug-replicas.yaml`
