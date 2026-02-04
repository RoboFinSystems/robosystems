# SEC EDGAR Adapter

This adapter provides integration with SEC EDGAR for downloading and processing XBRL financial filings.

## Directory Structure

```
sec/
├── README.md                    # This file
├── __init__.py                  # Adapter exports
├── config.py                    # XBRL processing configuration
├── metadata.py                  # SECMetadataLoader for filer/report metadata
├── client/                      # API clients
│   ├── edgar.py                 # SECClient - EDGAR API
│   ├── arelle.py                # ArelleClient - XBRL processing
│   ├── downloader.py            # SECDownloader - bulk file downloads
│   └── efts.py                  # EFTS API for filing discovery
└── processors/                  # Data transformation
    ├── __init__.py              # Processor exports
    ├── constants.py             # Shared constants (SHARED_NODE_TABLES, etc.)
    ├── xbrl_graph.py            # XBRLGraphProcessor - filing to parquet
    ├── processing.py            # Single filing processing helpers
    ├── consolidation.py         # Parquet consolidation and S3 merge
    ├── schema.py                # Schema adapter and config generator
    ├── dataframe.py             # DataFrame management
    ├── parquet.py               # Parquet file output
    ├── textblock.py             # S3 externalization for large text
    ├── ids.py                   # ID generation and naming utilities
    └── ingestion/               # DuckDB/LadybugDB ingestion
        ├── __init__.py          # Ingestion exports
        ├── models.py            # Result models and constants
        ├── staging.py           # DuckDBStager - S3 to DuckDB
        ├── materializer.py      # LadybugMaterializer - DuckDB to graph
        ├── direct_copy.py       # LadybugDirectCopier - S3 to graph
        └── processor.py         # XBRLDuckDBGraphProcessor (unified)
```

## Key Components

### Clients

| Class | Purpose |
|-------|---------|
| `SECClient` | EDGAR API for company lookup and filing metadata |
| `ArelleClient` | XBRL processing via Arelle library |
| `SECDownloader` | Bulk download of XBRL ZIP files to S3 |

### Processors

| Class | Purpose |
|-------|---------|
| `XBRLGraphProcessor` | Process single XBRL filing to parquet files |
| `XBRLDuckDBGraphProcessor` | Unified processor for staging and materialization |
| `DuckDBStager` | Stage parquet files to DuckDB |
| `LadybugMaterializer` | Materialize from DuckDB to LadybugDB |
| `LadybugDirectCopier` | Direct S3 to LadybugDB copy (bypasses DuckDB) |
| `SECMetadataLoader` | Load filer/report metadata with caching |

### Helper Modules

| Module | Purpose |
|--------|---------|
| `constants.py` | `SHARED_NODE_TABLES`, `QUARTER_END_DAYS` |
| `processing.py` | `process_single_filing_to_memory()` |
| `consolidation.py` | `consolidate_parquet_from_disk()`, `merge_with_existing_s3()`, `atomic_s3_upload()` |
| `ids.py` | `create_entity_id()`, `create_fact_id()`, etc. |

## Usage

### Basic Filing Processing

```python
from robosystems.adapters.sec import (
    SECClient,
    XBRLGraphProcessor,
    SECMetadataLoader,
)

# Fetch company info
client = SECClient(cik="0000320193")  # Apple
submissions = client.get_submissions()

# Process a filing
processor = XBRLGraphProcessor(
    report_uri="https://www.sec.gov/Archives/...",
    entityId="0000320193",
    sec_filer=sec_filer_dict,
    sec_report=sec_report_dict,
    output_dir="/tmp/output",
)
processor.process()  # Outputs parquet files
```

### Batch Ingestion (via Dagster)

The adapter is primarily used through Dagster assets. See [dagster/assets/sec/](../../dagster/assets/sec/README.md) for pipeline documentation.

```bash
# Load filings for a company
just sec-load NVDA 2025

# Run full SEC pipeline
uv run dagster asset materialize -m robosystems.dagster --select sec_raw_filings
```

## Data Flow

```
SEC EDGAR API
     │
     ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Download   │───▶│   Process   │───▶│    Stage    │
│ (ZIP files) │    │ (Parquet)   │    │  (DuckDB)   │
└─────────────┘    └─────────────┘    └─────────────┘
                                            │
                         ┌──────────────────┴──────────────────┐
                         ▼                                     ▼
                  ┌─────────────┐                       ┌─────────────┐
                  │ Materialize │                       │ Direct Copy │
                  │  (DuckDB →  │                       │  (S3 →      │
                  │  LadybugDB) │                       │  LadybugDB) │
                  └─────────────┘                       └─────────────┘
```

## Configuration

Key configuration in `config.py`:
- `XBRL_COLUMN_STANDARDIZATION` - Column name mapping
- `XBRL_EXTERNALIZATION_THRESHOLD` - Size threshold for S3 externalization
- `XBRL_STANDARDIZED_FILENAMES` - Output file naming

## Related Documentation

- [Dagster SEC Assets](../../dagster/assets/sec/README.md) - Pipeline orchestration
- [Schemas](../../schemas/README.md) - Graph schema definitions
