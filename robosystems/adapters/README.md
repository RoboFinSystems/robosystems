# Adapters Directory

This directory contains external service integrations that connect RoboSystems to third-party APIs and data sources. Each adapter is a self-contained module that handles API client connections, data transformation, and — for shared repositories — a **manifest** that declares the repository's complete configuration.

## Two Types of Adapters

**Shared repository adapters** (e.g., SEC) serve platform-wide public data. They include a `manifest.py` that declares identity, billing plans, rate limits, endpoint access, and credit costs. The manifest is the single source of truth — the registry in `config/shared_repositories.py` collects manifests and provides the query API used by billing, middleware, and operations.

**Private adapters** (e.g., QuickBooks) integrate with per-user external services. They have a client and a Dagster ELT pipeline but no manifest, since they operate on individual user graphs rather than shared platform data.

## Directory Structure

```
adapters/
├── README.md                    # This file
├── __init__.py                  # Core adapter exports
├── base.py                      # SharedRepositoryManifest dataclass
├── sec/                         # SEC EDGAR adapter (shared repository)
│   ├── README.md                # SEC adapter documentation
│   ├── __init__.py              # SEC adapter exports
│   ├── manifest.py              # SEC shared repository manifest
│   ├── config.py                # XBRL processing configuration
│   ├── enrichment.py            # SemanticEnricher (embeddings + classification)
│   ├── client/                  # SEC API clients
│   │   ├── edgar.py             # EDGAR API client
│   │   ├── arelle.py            # Arelle XBRL processor client
│   │   ├── downloader.py        # Bulk file downloads
│   │   └── efts.py              # EFTS filing discovery
│   ├── processors/              # XBRL -> Graph transformation
│   │   ├── metadata.py          # SECMetadataLoader
│   │   ├── constants.py         # Shared constants
│   │   ├── xbrl_graph.py        # XBRLGraphProcessor (main)
│   │   ├── processing.py        # Single filing processing
│   │   ├── consolidation.py     # Parquet consolidation
│   │   ├── classify.py          # Association classification pipeline
│   │   ├── schema.py            # Schema adapter and config
│   │   ├── dataframe.py         # DataFrame management
│   │   ├── parquet.py           # Parquet file output
│   │   ├── textblock.py         # S3 externalization
│   │   ├── ids.py               # ID generation utilities
│   │   └── ingestion/           # DuckDB/LadybugDB ingestion
│   │       ├── staging.py       # DuckDBStager
│   │       ├── materializer.py  # LadybugMaterializer
│   │       ├── direct_copy.py   # LadybugDirectCopier
│   │       └── processor.py     # XBRLDuckDBGraphProcessor
│   ├── knowledge/               # Offline knowledge artifact generation
│   │   ├── __init__.py          # Package exports
│   │   ├── extractors.py        # DuckDB data extraction (edges, filing counts)
│   │   ├── graphs.py            # NetworkX graph construction
│   │   ├── classifiers.py       # Statement type classification (BFS + heuristics)
│   │   ├── artifact.py          # Artifact builders (element knowledge, structure profiles)
│   │   └── framework.py         # DuckDBAnalyticsContext (sync context manager)
│   ├── taxonomy/                # Canonical concept mappings
│   │   ├── __init__.py          # CanonicalConcept, get_element_taxonomy, get_structure_taxonomy
│   │   ├── concepts.py          # Concept type definitions
│   │   ├── structures.py        # Structure type definitions
│   │   ├── balance_sheet.py     # Balance sheet concept mappings
│   │   ├── cash_flow.py         # Cash flow concept mappings
│   │   └── income_statement.py  # Income statement concept mappings
│   └── pipeline/                # Dagster orchestration
│       ├── README.md            # Pipeline documentation
│       ├── __init__.py          # get_dagster_components() discovery
│       ├── configs.py           # Run configurations
│       ├── download.py          # sec_raw_filings asset
│       ├── process.py           # sec_processed_filings asset
│       ├── stage.py             # DuckDB staging assets
│       ├── materialize.py       # LadybugDB materialization assets
│       ├── artifact.py          # Knowledge artifact generation assets
│       ├── s3_publish.py        # LadybugDB → S3 publish assets
│       ├── duckdb_s3_publish.py # DuckDB → S3 publish assets
│       ├── r2_publish.py        # LadybugDB → Cloudflare R2 publish assets
│       ├── vector_publish.py    # Vector embedding publish assets
│       ├── text_index.py        # OpenSearch text/iXBRL indexing assets
│       ├── entity_sync/         # Entity (company) metadata sync sub-pipeline
│       ├── jobs.py              # 18 SEC job definitions
│       └── sensors.py           # 6 sensors + 1 schedule
└── quickbooks/                  # QuickBooks adapter (private)
    ├── __init__.py              # Exports QBClient only
    ├── client/                  # QuickBooks API client
    │   └── api.py               # QBClient - OAuth client
    ├── pipeline/                # Dagster ELT pipeline
    │   ├── configs.py           # Run configurations
    │   ├── event_action_mapping.py # QB txn type → REA event/action mapping
    │   ├── extract.py           # QBClient extraction (CDC + SyncToken delta sync)
    │   ├── jobs.py              # qb_sync_job
    │   ├── load.py              # Materialize DuckDB tables to user graph
    │   ├── transform.py         # Invoke dbt models
    │   └── utils.py             # Pipeline helpers
    └── dbt/                     # dbt-on-DuckDB project (raw QB → RoboLedger schema)
```

## Shared Repository Manifest Pattern

Shared repository adapters declare a `SharedRepositoryManifest` (defined in `base.py`) that contains:

| Field | Purpose |
|-------|---------|
| `id`, `name`, `description` | Identity (id doubles as graph_id) |
| `data_source_type`, `data_source_url`, `sync_frequency` | Data source metadata |
| `schema_type`, `schema_extensions` | Graph schema configuration |
| `has_semantic_enrichment` | MCP capability flags |
| `plans` | Billing plans with pricing, credits, and features |
| `rate_limits` | Per-plan rate limits (queries, MCP, agent, downloads) |
| `allowed_endpoints`, `blocked_endpoints` | Endpoint access control |
| `credit_costs` | Per-operation credit costs |
| `graph_tier`, `graph_instance_id` | Infrastructure placement |

The import chain is carefully designed to avoid circular dependencies:

```
config/shared_repositories.py → adapters/{name}/manifest.py → adapters/base.py
```

`base.py` has zero imports from the rest of the codebase. The registry uses lazy loading — manifests are only imported on first access.

## Adapter Pattern

Each adapter follows a consistent structure:

1. **Client** - API connection and authentication
2. **Processors** - Data transformation for graph ingestion
3. **Manifest** (shared repos only) - Complete repository configuration
4. **Enrichment** (optional) - Semantic enrichment and classification during processing
5. **Knowledge** (optional) - Offline corpus-level artifact generation for confidence refinement
6. **Taxonomy** (optional) - Canonical concept and structure type mappings
7. **Models** (optional) - Service-specific data models

## Available Adapters

### SEC EDGAR (`sec/`) — Shared Repository

Financial filing data from the SEC. Declared as a shared repository via `sec/manifest.py`.

The SEC adapter has three processing layers:

1. **Core pipeline** (`client/`, `processors/`, `pipeline/`) — Downloads XBRL filings from EDGAR, transforms them into graph nodes/relationships, stages in DuckDB, and materializes into LadybugDB.

2. **Enrichment** (`enrichment.py`, `taxonomy/`) — `SemanticEnricher` runs inline during filing processing to add semantic metadata: canonical concept mapping via fastembed embeddings, Structure-level `canonical_type` classification (income_statement, balance_sheet, etc.), and Association-level disclosure classification. Controlled by the `XBRL_SEMANTIC_ENRICHMENT`, `XBRL_ASSOCIATION_CLASSIFICATION`, and `XBRL_GRAPH_REFINEMENT` flags in `adapters/sec/config.py`.

3. **Knowledge artifacts** (`knowledge/`) — Offline Dagster jobs that analyze the full DuckDB corpus to generate confidence-refinement artifacts (`element_knowledge.parquet`, `structure_profiles.parquet`, `structure_consensus.parquet`). These artifacts are loaded at enrichment time to refine classification confidence — crushing bad semantic matches and boosting well-connected elements.

```python
from robosystems.adapters.sec import (
    SECClient,              # EDGAR API client
    ArelleClient,           # XBRL processing via Arelle
    XBRLGraphProcessor,     # Process filings to graph
    XBRLDuckDBGraphProcessor,  # DuckDB-based processing
)

# Fetch and process SEC filings
client = SECClient()
filings = client.get_filings(cik="0000320193", form_type="10-K")
```

### QuickBooks (`quickbooks/`) — Private

Small business accounting integration, structured as a **dbt-on-DuckDB ELT pipeline**:

- **`client/api.py`** — `QBClient`, the OAuth-authenticated QuickBooks Online API client. This is the only symbol exported from `__init__.py`.
- **`pipeline/`** — a full Dagster ELT pipeline: `extract.py` (CDC + SyncToken delta sync via `QBClient`), `transform.py` (invokes the dbt models), `load.py` (materializes the resulting DuckDB tables into the user's LadybugDB graph), plus `event_action_mapping.py` (QB transaction types → REA event/action), `configs.py`, `jobs.py` (`qb_sync_job`), and `utils.py`.
- **`dbt/`** — a dbt project (staging + ledger models) that transforms raw QuickBooks data into the RoboLedger schema (transactions, entries, line items, elements, agents, dimensions).

```python
from robosystems.adapters.quickbooks import QBClient  # the only export

# Initialize QuickBooks client
client = QBClient(realm_id="123456", qb_credentials=credentials)
```

**Outbound write-back.** QuickBooks-connected graphs support outbound write-back governed by `Connection.write_policy` (`native` / `qb_authoritative` / `hybrid`; QB defaults to `qb_authoritative`). Locally-authored events are pushed back to QuickBooks via the `execute-event-block` operation, which stamps `metadata.qb_external_id` so subsequent CDC + SyncToken delta sync can de-duplicate against the source. The close-review "outbox" surfaces `will_publish_to_qb` on the period-drafts read so a user sees what closing the period will write to QuickBooks before committing. See the [Connection model](../models/core/README.md) and the roboledger operations surface for details.

## Usage with Dagster

Adapters are used by Dagster assets to process data:

```python
from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

# Dagster assets use adapters directly
# See: robosystems/dagster/assets/sec/
client = SECClient()
filings = client.get_filings(cik="0000320193", form_type="10-K")
```

For local development:
```bash
just sec-load NVDA 2025    # Load company via Dagster pipeline
```

**Note:** Both the SEC and QuickBooks adapters have active Dagster pipelines — SEC via `sec/pipeline/`, QuickBooks via `quickbooks/pipeline/jobs.py` (`qb_sync_job`).

## Adding New Adapters

### Shared Repository Adapter

For platform-wide public data sources (like SEC):

1. Create directory: `adapters/{name}/`
2. Create `manifest.py` with a `SharedRepositoryManifest` instance (import from `adapters/base`)
3. Add one import + `_register()` call to `_load_manifests()` in `config/shared_repositories.py`
4. Add client module: `client/{api}.py`
5. Add processors: `processors/{type}.py`
6. Add Dagster pipeline: `pipeline/` with `get_dagster_components()` (see `sec/pipeline/` for example)
7. Export in `__init__.py`
8. Add tests in `tests/adapters/{name}/`
9. Import pipeline in `dagster/definitions.py`

### Private Adapter

For per-user external service integrations (like QuickBooks):

1. Create directory: `adapters/{service_name}/`
2. Add client module: `client/{api}.py`
3. Add processors: `processors/{type}.py`
4. Add Dagster pipeline: `pipeline/` with `get_dagster_components()` (optional)
5. Export in `__init__.py`
6. Add tests in `tests/adapters/{service_name}/`
7. Import pipeline in `dagster/definitions.py` (if pipeline added)

## Custom Data Sources: Integrations First

**The supported route for connecting your own data sources is an integration, not an in-core adapter.** An integration is a program in its own repository that writes through the public API with an API key — start from [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template), and see [`robosystems-marketing-integration`](https://github.com/RoboFinSystems/robosystems-marketing-integration) for a real one. Integrations survive every platform release, work identically against managed and self-hosted deployments, and hold their own source credentials. The platform's own adapters prove the surface: the QuickBooks adapter writes through the same public event envelope an external integration would use.

The adapter registry in this directory is **maintained exclusively by the platform team** — both classes keep expanding (connection adapters like QuickBooks; shared-repository adapters like SEC), but platform-operated deployments run an unmodified core, so this directory is not a contribution surface for custom sources. Want a source supported natively? Open a [discussion](https://github.com/orgs/RoboFinSystems/discussions).

## Custom Adapters on Self-Hosted Forks

If you fork this repository and operate your **own** deployment in your **own** infrastructure, the adapter directory remains a **merge boundary**: in-core additions live in the `custom_*` namespace, which upstream never touches, so updates merge conflict-free. This pattern applies only to deployments you run yourself.

```
adapters/
├── sec/                 # ← Upstream maintains, shared repository
├── quickbooks/          # ← Upstream maintains, private adapter
│
└── custom_*/            # ← Self-hosted-fork namespace (upstream NEVER touches)
    ├── custom_erp/      #    Your custom ERP integration
    └── custom_bank/     #    Your bank API integration
```

**To add a custom data source in your self-hosted fork:**

1. Create `adapters/custom_myservice/` following the same client/processors/pipeline structure
2. Add `pipeline/` with `get_dagster_components()` returning assets, jobs, sensors, schedules
3. Import pipeline in `dagster/definitions.py` (see the `# === FORK` comment)

**Merge-conflict-free updates:**

```bash
git remote add upstream https://github.com/RoboFinSystems/robosystems.git
git fetch upstream
git merge upstream/main  # Clean merge - your custom_*/ directories untouched
```

Even on a self-hosted deployment, consider the integration route first — it needs no fork at all.

## Related Documentation

- **[Dagster Assets](/robosystems/dagster/README.md)** - Data pipeline orchestration
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[Shared Repository Registry](/robosystems/config/README.md)** - Registry and billing accessors
