# SEC EDGAR Adapter

This adapter provides integration with SEC EDGAR for downloading and processing XBRL financial filings.

## Directory Structure

```
sec/
├── README.md                    # This file
├── __init__.py                  # Adapter exports
├── config.py                    # XBRL processing configuration
├── manifest.py                  # Shared repository manifest
├── client/                      # API clients
│   ├── edgar.py                 # SECClient - EDGAR API
│   ├── arelle.py                # ArelleClient - XBRL processing
│   ├── downloader.py            # SECDownloader - bulk file downloads
│   └── efts.py                  # EFTS API for filing discovery
├── enrichment.py                # SemanticEnricher (embeddings + classification)
├── processors/                  # Data transformation
│   ├── __init__.py              # Processor exports
│   ├── metadata.py              # SECMetadataLoader for filer/report metadata
│   ├── constants.py             # Shared constants (SHARED_NODE_TABLES, etc.)
│   ├── xbrl_graph.py            # XBRLGraphProcessor - filing to parquet
│   ├── processing.py            # Single filing processing helpers
│   ├── consolidation.py         # Parquet consolidation and S3 merge
│   ├── classify.py              # Association classification pipeline
│   ├── schema.py                # Schema adapter and config generator
│   ├── dataframe.py             # DataFrame management
│   ├── parquet.py               # Parquet file output
│   ├── textblock.py             # S3 externalization for large text
│   ├── ids.py                   # ID generation and naming utilities
│   └── ingestion/               # DuckDB/LadybugDB ingestion
│       ├── __init__.py          # Ingestion exports
│       ├── models.py            # Result models and constants
│       ├── staging.py           # DuckDBStager - S3 to DuckDB
│       ├── materializer.py      # LadybugMaterializer - DuckDB to graph
│       ├── direct_copy.py       # LadybugDirectCopier - S3 to graph
│       └── processor.py         # XBRLDuckDBGraphProcessor (unified)
├── knowledge/                   # Offline knowledge artifact generation
│   ├── __init__.py              # Package exports
│   ├── extractors.py            # DuckDB data extraction (edges, filing counts)
│   ├── graphs.py                # NetworkX graph construction
│   ├── classifiers.py           # Statement type classification (BFS + heuristics)
│   ├── artifact.py              # Artifact builders (element knowledge, structure profiles)
│   └── framework.py             # DuckDBAnalyticsContext (sync context manager)
├── taxonomy/                    # Canonical concept mappings
│   ├── __init__.py              # ConceptTaxonomy registry
│   ├── concepts.py              # Concept type definitions
│   ├── structures.py            # Structure type definitions
│   ├── balance_sheet.py         # Balance sheet concept mappings
│   ├── cash_flow.py             # Cash flow concept mappings
│   └── income_statement.py      # Income statement concept mappings
└── pipeline/                    # Dagster orchestration
    ├── __init__.py              # get_dagster_components() discovery
    ├── README.md                # Pipeline documentation
    ├── configs.py               # Run configurations
    ├── download.py              # sec_raw_filings asset
    ├── process.py               # sec_processed_filings asset
    ├── stage.py                 # DuckDB staging assets
    ├── materialize.py           # LadybugDB materialization assets
    ├── entity_update.py         # Entity incremental update asset
    ├── backup.py                # Backup asset
    ├── jobs.py                  # 12 SEC job definitions
    └── sensors.py               # 6 sensors + 1 schedule
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

### Enrichment & Classification

| Class | Purpose |
|-------|---------|
| `SemanticEnricher` | Inline enrichment during filing processing — fastembed embeddings, canonical concept mapping, structure `canonical_type` classification, association disclosure classification |
| `ConceptTaxonomy` | Registry of canonical concept mappings per statement type (balance sheet, income statement, cash flow) |
| `classify_associations()` | Offline association classification via TempLadybugDB — Cypher pattern matching for disclosure mechanics |

### Knowledge Artifacts

| Class | Purpose |
|-------|---------|
| `ArcExtractor` | Extracts deduplicated edges, filing counts, disclosure types from DuckDB |
| `TaxonomyGraph` | Builds NetworkX directed graph from XBRL arc relationships |
| `StatementClassifier` | BFS + heuristic classification of elements into statement types |
| `ElementKnowledgeBuilder` | Generates `element_knowledge.parquet` (pagerank, statement type, disclosure type) |
| `StructureProfileBuilder` | Generates `structure_profiles.parquet` and `structure_consensus.parquet` |
| `DuckDBAnalyticsContext` | Sync context manager for running analytics on DuckDB staging files |

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

The adapter is primarily used through Dagster assets. See [pipeline/](pipeline/README.md) for pipeline documentation.

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
└─────────────┘    └──────┬──────┘    └─────────────┘
                          │                  │
                   ┌──────▼──────┐           │
                   │   Enrich    │    ┌──────┴──────────────────┐
                   │ (Semantic + │    ▼                         ▼
                   │  Classify)  │  ┌─────────────┐     ┌─────────────┐
                   └─────────────┘  │ Materialize │     │ Direct Copy │
                                    │  (DuckDB →  │     │  (S3 →      │
                   ┌─────────────┐  │  LadybugDB) │     │  LadybugDB) │
                   │  Knowledge  │  └─────────────┘     └─────────────┘
                   │ (Artifacts) │
                   │  offline    │
                   └─────────────┘
```

### Enrichment (inline, per-filing)

During the Process step, `SemanticEnricher` adds semantic metadata:
- **Canonical concepts**: Maps XBRL element qnames to canonical concepts (e.g. `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` → `revenue`) using fastembed cosine similarity
- **Structure classification**: Assigns `canonical_type` to Structure nodes (income_statement, balance_sheet, cash_flow_statement, equity_statement)
- **Association classification**: Creates Classification nodes linking Associations to disclosure types (AssetsRollUp, RevenueBreakdown, etc.)
- **Confidence refinement**: Uses knowledge artifacts to crush bad semantic matches and boost well-connected elements

### Knowledge Artifacts (offline, corpus-level)

Dagster jobs run `ArcExtractor` → `TaxonomyGraph` → `StatementClassifier` over the full DuckDB corpus to produce:
- `element_knowledge.parquet` — pagerank, neighborhood agreement, BFS depth, statement type per element
- `structure_profiles.parquet` — element composition fingerprints per structure type
- `structure_consensus.parquet` — canonical type consensus across structures

These artifacts are stored in `data/artifacts/` and loaded by `SemanticEnricher` at runtime.

## Configuration

Key configuration in `config.py`:
- `XBRL_COLUMN_STANDARDIZATION` - Column name mapping
- `XBRL_EXTERNALIZATION_THRESHOLD` - Size threshold for S3 externalization
- `XBRL_STANDARDIZED_FILENAMES` - Output file naming

### Feature Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `XBRL_SEMANTIC_ENRICHMENT` | `True` | Enable fastembed-based canonical concept mapping and structure classification |
| `XBRL_ASSOCIATION_CLASSIFICATION` | `True` | Enable association-level disclosure classification (creates Classification nodes) |
| `XBRL_GRAPH_REFINEMENT` | `True` | Enable knowledge artifact-based confidence refinement |

## Related Documentation

- [SEC Pipeline](pipeline/README.md) - Dagster pipeline orchestration
- [Schemas](../../schemas/README.md) - Graph schema definitions
