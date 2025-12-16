# Adapters Directory

This directory contains external service integrations that connect RoboSystems to third-party APIs and data sources. Each adapter is a self-contained module that handles both the API client connection and any data transformation (processors) needed for that service.

## Directory Structure

```
adapters/
├── README.md                    # This file
├── __init__.py                  # Core adapter exports
├── sec/                         # SEC EDGAR adapter
│   ├── __init__.py              # SEC adapter exports
│   ├── client/                  # SEC API client
│   │   └── edgar.py             # EDGAR API client
│   ├── arelle/                  # Arelle XBRL integration
│   │   └── client.py            # Arelle subprocess client
│   └── processors/              # XBRL data transformation
│       ├── graph.py             # XBRLGraphProcessor
│       ├── duckdb_graph.py      # DuckDB-based processor
│       ├── schema_adapter.py    # Schema generation
│       └── ...                  # Supporting utilities
├── quickbooks/                  # QuickBooks adapter
│   ├── __init__.py              # QuickBooks adapter exports
│   ├── client/                  # QuickBooks API client
│   │   └── api.py               # OAuth client
│   └── processors/              # Transaction processing
│       ├── transactions.py      # Transaction sync (stubbed)
│       └── uri_utils.py         # URI generation utilities
└── plaid/                       # Plaid adapter (banking)
    └── ...
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

Banking and financial institution connections.

## Usage in Operations

Adapters are used by operations to orchestrate business workflows:

```python
from robosystems.operations.pipelines.sec_xbrl_filings import SECXBRLFilingPipeline
from robosystems.adapters.sec import SECClient, XBRLGraphProcessor

# Operations coordinate adapters
pipeline = SECXBRLFilingPipeline(graph_id="kg123")
await pipeline.process_company("AAPL", fiscal_year=2024)
```

## Adding New Adapters

To add a new external service:

1. Create directory: `adapters/{service_name}/`
2. Add client module: `client/{api}.py`
3. Add processors: `processors/{type}.py`
4. Export in `__init__.py`
5. Add tests in `tests/adapters/{service_name}/`

## Related Documentation

- **[Operations](/robosystems/operations/README.md)** - Business workflow orchestration
- **[SEC XBRL Tasks](/robosystems/tasks/sec_xbrl/README.md)** - SEC pipeline tasks
- **[Schemas](/robosystems/schemas/README.md)** - Graph schema definitions
