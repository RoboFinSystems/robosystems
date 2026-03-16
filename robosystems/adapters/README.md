# Adapters Directory

This directory contains external service integrations that connect RoboSystems to third-party APIs and data sources. Each adapter is a self-contained module that handles API client connections, data transformation, and — for shared repositories — a **manifest** that declares the repository's complete configuration.

## Two Types of Adapters

**Shared repository adapters** (e.g., SEC) serve platform-wide public data. They include a `manifest.py` that declares identity, billing plans, rate limits, endpoint access, and credit costs. The manifest is the single source of truth — the registry in `config/shared_repositories.py` collects manifests and provides the query API used by billing, middleware, and operations.

**Private adapters** (e.g., QuickBooks, Plaid) integrate with per-user external services. They have clients and processors but no manifest, since they operate on individual user graphs rather than shared platform data.

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
│   │   ├── __init__.py          # ConceptTaxonomy registry
│   │   ├── concepts.py          # Concept type definitions
│   │   ├── structures.py        # Structure type definitions
│   │   ├── balance_sheet.py     # Balance sheet concept mappings
│   │   ├── cash_flow.py         # Cash flow concept mappings
│   │   └── income_statement.py  # Income statement concept mappings
│   └── pipeline/                # Dagster orchestration
│       ├── __init__.py          # get_dagster_components() discovery
│       ├── configs.py           # Run configurations
│       ├── download.py          # sec_raw_filings asset
│       ├── process.py           # sec_processed_filings asset
│       ├── stage.py             # DuckDB staging assets
│       ├── materialize.py       # LadybugDB materialization assets
│       ├── jobs.py              # 12 SEC job definitions
│       └── sensors.py           # 6 sensors + 1 schedule
├── quickbooks/                  # QuickBooks adapter (private, stubbed)
│   ├── __init__.py              # QuickBooks adapter exports
│   ├── client/                  # QuickBooks API client
│   │   └── api.py               # OAuth client
│   └── processors/              # Transaction processing
│       ├── transactions.py      # Transaction sync (stubbed)
│       └── uri_utils.py         # URI generation utilities
└── plaid/                       # Plaid adapter (private, stubbed)
    ├── __init__.py              # Plaid adapter exports
    ├── client/                  # Plaid API client
    │   └── api.py               # PlaidClient
    └── processors/              # Banking data transformation
        ├── transactions.py      # PlaidTransactionsProcessor
        └── uri_utils.py         # URI generation utilities
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

2. **Enrichment** (`enrichment.py`, `taxonomy/`) — `SemanticEnricher` runs inline during filing processing to add semantic metadata: canonical concept mapping via fastembed embeddings, Structure-level `canonical_type` classification (income_statement, balance_sheet, etc.), and Association-level disclosure classification. Controlled by feature flags `XBRL_SEMANTIC_ENRICHMENT`, `XBRL_ASSOCIATION_CLASSIFICATION`, and `XBRL_GRAPH_REFINEMENT`.

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

Small business accounting integration:

```python
from robosystems.adapters.quickbooks import (
    QBClient,                # QuickBooks OAuth client
    QBTransactionsProcessor, # Transaction sync (stubbed)
    qb_entity_uri,          # URI generation utilities
)

# Initialize QuickBooks client
client = QBClient(realm_id="123456", qb_credentials=credentials)
```

### Plaid (`plaid/`) — Private

Banking and financial institution connections:

```python
from robosystems.adapters.plaid import (
    PlaidClient,                 # Plaid API client
    PlaidTransactionsProcessor,  # Transaction sync to graph
    plaid_account_element_uri,   # URI generation utilities
)

# Initialize Plaid client with access token
client = PlaidClient(access_token="access-sandbox-xxx")
accounts = client.get_accounts()
transactions = client.sync_transactions(cursor=None)
```

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

**Note:** Currently only the SEC adapter has active Dagster assets. QuickBooks and Plaid adapters are stubbed for future implementation.

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

For per-user external service integrations (like QuickBooks, Plaid):

1. Create directory: `adapters/{service_name}/`
2. Add client module: `client/{api}.py`
3. Add processors: `processors/{type}.py`
4. Add Dagster pipeline: `pipeline/` with `get_dagster_components()` (optional)
5. Export in `__init__.py`
6. Add tests in `tests/adapters/{service_name}/`
7. Import pipeline in `dagster/definitions.py` (if pipeline added)

## Fork-Friendly Custom Adapters

The adapter directory structure is designed as a **merge boundary** for forks. Custom adapters live in isolated namespaces that upstream never touches, enabling conflict-free updates.

```
adapters/
├── sec/                 # ← Upstream maintains, shared repository
├── quickbooks/          # ← Upstream maintains, stubbed
├── plaid/               # ← Upstream maintains, stubbed
│
└── custom_*/            # ← Fork namespace (upstream NEVER touches)
    ├── custom_erp/      #    Your custom ERP integration
    ├── custom_bank/     #    Your bank API integration
    └── custom_crm/      #    Your CRM integration
```

**To add a custom data source in your fork:**

1. Create `adapters/custom_myservice/` following the same client/processors/pipeline structure
2. Add `pipeline/` with `get_dagster_components()` returning assets, jobs, sensors, schedules
3. Import pipeline in `dagster/definitions.py` (see the `# === FORK` comment)

**Merge-conflict-free updates:**

```bash
git remote add upstream https://github.com/RoboFinSystems/robosystems.git
git fetch upstream
git merge upstream/main  # Clean merge - your custom_*/ directories untouched
```

The `custom_*` namespace convention ensures that `git pull upstream main` never conflicts with your additions.

## Related Documentation

- **[Dagster Assets](/robosystems/dagster/README.md)** - Data pipeline orchestration
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
- **[Shared Repository Registry](/robosystems/config/README.md)** - Registry and billing accessors
