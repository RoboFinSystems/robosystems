# Adapters Directory

This directory contains external service integrations that connect RoboSystems to third-party APIs and data sources. Each adapter is a self-contained module that handles both the API client connection and any data transformation (processors) needed for that service.

## Directory Structure

```
adapters/
├── README.md                    # This file
├── __init__.py                  # Core adapter exports
├── sec/                         # SEC EDGAR adapter
│   ├── __init__.py              # SEC adapter exports
│   ├── client/                  # SEC API clients
│   │   ├── edgar.py             # EDGAR API client
│   │   └── arelle.py            # Arelle XBRL processor client
│   └── processors/              # XBRL → Graph transformation
│       ├── xbrl_graph.py        # XBRLGraphProcessor (main)
│       ├── ingestion.py         # XBRLDuckDBGraphProcessor
│       ├── schema.py            # Schema adapter and config generator
│       ├── dataframe.py         # DataFrame management
│       ├── parquet.py           # Parquet file output
│       ├── textblock.py         # S3 externalization
│       └── ids.py               # ID generation and naming utilities
├── quickbooks/                  # QuickBooks adapter
│   ├── __init__.py              # QuickBooks adapter exports
│   ├── client/                  # QuickBooks API client
│   │   └── api.py               # OAuth client
│   └── processors/              # Transaction processing
│       ├── transactions.py      # Transaction sync (stubbed)
│       └── uri_utils.py         # URI generation utilities
└── plaid/                       # Plaid adapter (banking)
    ├── __init__.py              # Plaid adapter exports
    ├── client/                  # Plaid API client
    │   └── api.py               # PlaidClient
    └── processors/              # Banking data transformation
        ├── transactions.py      # PlaidTransactionsProcessor
        └── uri_utils.py         # URI generation utilities
```

## Adapter Pattern

Each adapter follows a consistent structure:

1. **Client** - API connection and authentication
2. **Processors** - Data transformation for graph ingestion
3. **Models** (optional) - Service-specific data models

## Available Adapters

### SEC EDGAR (`sec/`)

Financial filing data from the SEC:

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

### QuickBooks (`quickbooks/`)

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

### Plaid (`plaid/`)

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
# See: robosystems/dagster/assets/sec.py
client = SECClient()
filings = client.get_filings(cik="0000320193", form_type="10-K")
```

For local development:
```bash
just sec-load NVDA 2025    # Load company via Dagster pipeline
```

## Adding New Adapters

To add a new external service:

1. Create directory: `adapters/{service_name}/`
2. Add client module: `client/{api}.py`
3. Add processors: `processors/{type}.py`
4. Export in `__init__.py`
5. Add tests in `tests/adapters/{service_name}/`
6. Create Dagster assets in `dagster/assets/{service_name}.py`

## Fork-Friendly Custom Adapters

The adapter directory structure is designed as a **merge boundary** for forks. Custom adapters live in isolated namespaces that upstream never touches, enabling conflict-free updates.

```
adapters/
├── sec/                 # ← Upstream maintains (don't modify in fork)
├── quickbooks/          # ← Upstream maintains (don't modify in fork)
├── plaid/               # ← Upstream maintains (don't modify in fork)
│
└── custom_*/            # ← Fork namespace (upstream NEVER touches)
    ├── custom_erp/      #    Your custom ERP integration
    ├── custom_bank/     #    Your bank API integration
    └── custom_crm/      #    Your CRM integration
```

**To add a custom data source in your fork:**

1. Create `adapters/custom_myservice/` following the same client/processors structure
2. Add a Dagster asset file in `dagster/assets/custom_myservice.py`
3. Import in `dagster/definitions.py`

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
