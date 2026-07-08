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
├── narrative_extractor.py       # NarrativeExtractor - Item section detection from 10-K/10-Q HTML
├── ixbrl_parser.py              # iXBRLParser - disclosure sections with XBRL element metadata
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
│   ├── extractors.py            # DuckDB data extraction (edges, filing counts, Arrow export)
│   ├── graphs.py                # Icebug graph construction (zero-copy Arrow → CSR)
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
    ├── text_index.py            # OpenSearch text indexing (textblocks, narratives, iXBRL)
    ├── backup.py                # Backup asset
    ├── jobs.py                  # 18 SEC job definitions
    └── sensors.py               # 6 sensors + 1 schedule
```

## Key Components

### Clients

| Class           | Purpose                                          |
| --------------- | ------------------------------------------------ |
| `SECClient`     | EDGAR API for company lookup and filing metadata |
| `ArelleClient`  | XBRL processing via Arelle library               |
| `SECDownloader` | Bulk download of XBRL ZIP files to S3            |

### Processors

| Class                      | Purpose                                           |
| -------------------------- | ------------------------------------------------- |
| `XBRLGraphProcessor`       | Process single XBRL filing to parquet files       |
| `XBRLDuckDBGraphProcessor` | Unified processor for staging and materialization |
| `DuckDBStager`             | Stage parquet files to DuckDB                     |
| `LadybugMaterializer`      | Materialize from DuckDB to LadybugDB              |
| `LadybugDirectCopier`      | Direct S3 to LadybugDB copy (bypasses DuckDB)     |
| `SECMetadataLoader`        | Load filer/report metadata with caching           |

### Text Extraction

| Class | Purpose |
| ----- | ------- |
| `NarrativeExtractor` | Extract Item sections (Business, Risk Factors, MD&A, Cybersecurity, etc.) from 10-K/10-Q HTML using heuristic section detection |
| `iXBRLParser` | Extract iXBRL disclosure sections (`ix:nonNumeric TextBlock` elements) with XBRL element metadata for graph cross-reference |

### Enrichment & Classification

| Class                     | Purpose                                                                                                                                                                        |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `SemanticEnricher`        | Inline enrichment during filing processing — fastembed embeddings, canonical concept mapping, structure `canonical_type` classification, association disclosure classification |
| `ConceptTaxonomy`         | Registry of canonical concept mappings per statement type (balance sheet, income statement, cash flow)                                                                         |
| `classify_associations()` | Offline association classification via TempLadybugDB — Cypher pattern matching for disclosure mechanics                                                                        |

### Knowledge Artifacts

| Class                              | Purpose                                                                           |
| ---------------------------------- | --------------------------------------------------------------------------------- |
| `ArcExtractor`                     | Extracts edges, filing counts, disclosure types from DuckDB (Arrow + legacy)      |
| `build_element_graph_from_arrow()` | Zero-copy DuckDB → Arrow → CSR graph construction via icebug `Graph.fromCSR()`    |
| `StatementClassifier`              | BFS + heuristic classification of elements into statement types                   |
| `ElementKnowledgeBuilder`          | Generates `element_knowledge.parquet` (pagerank, statement type, disclosure type) |
| `StructureKnowledgeBuilder`        | Generates `structure_profiles.parquet` and `structure_consensus.parquet`          |
| `DuckDBAnalyticsContext`           | Sync context manager for running analytics on DuckDB staging files                |

### Helper Modules

| Module             | Purpose                                                                             |
| ------------------ | ----------------------------------------------------------------------------------- |
| `constants.py`     | `SHARED_NODE_TABLES`, `QUARTER_END_DAYS`                                            |
| `processing.py`    | `process_single_filing_to_memory()`                                                 |
| `consolidation.py` | `consolidate_parquet_from_disk()`, `merge_with_existing_s3()`, `atomic_s3_upload()` |
| `ids.py`           | `create_entity_id()`, `create_fact_id()`, etc.                                      |

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
└──────┬──────┘    └──────┬──────┘    └─────────────┘
       │                  │                  │
       │           ┌──────▼──────┐           │
       │           │   Enrich    │    ┌──────┴──────────────────┐
       │           │ (Semantic + │    ▼                         ▼
       │           │  Classify)  │  ┌─────────────┐     ┌─────────────┐
       │           └─────────────┘  │ Materialize │     │ Direct Copy │
       │                            │  (DuckDB →  │     │  (S3 →      │
       │           ┌─────────────┐  │  LadybugDB) │     │  LadybugDB) │
       │           │  Knowledge  │  └─────────────┘     └─────────────┘
       │           │ (Artifacts) │
       │           │  offline    │
       │           └─────────────┘
       │
       ▼
┌─────────────┐
│ Text Index  │
│ (OpenSearch)│
│ Narratives, │
│ iXBRL, Text │
│ Blocks      │
└─────────────┘
```

### Enrichment (inline, per-filing)

During the Process step, `SemanticEnricher` adds semantic metadata. Embeddings
are computed **transiently** to assign canonical concepts/types — the per-element
`embedding` vector is **not persisted** and the LanceDB element-vector "semantic
search" tier was retired (it indexed ~8M elements dominated by single-filing filer
extensions and returned long-tail noise; `resolve-element` is now canonical-concept
→ text-label fallback). The persisted outputs are `canonical_concept` /
`canonical_type`, not vectors.

- **Canonical concepts**: Maps XBRL element qnames to canonical concepts (e.g. `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` → `revenue`) using fastembed cosine similarity against the curated `ConceptTaxonomy`
- **Structure classification**: Assigns `canonical_type` to Structure nodes (income_statement, balance_sheet, cash_flow_statement, equity_statement)
- **Association classification**: Creates Classification nodes linking Associations to disclosure types (AssetsRollUp, RevenueBreakdown, etc.)
- **Confidence refinement**: Uses knowledge artifacts to crush bad semantic matches and boost well-connected elements

### Knowledge Artifacts (offline, corpus-level)

Dagster jobs run `ArcExtractor` → icebug graph → `StatementClassifier` over the full DuckDB corpus to produce:

- `element_knowledge.parquet` — pagerank, neighborhood agreement, BFS depth, statement type per element
- `structure_profiles.parquet` — element composition fingerprints per structure type
- `structure_consensus.parquet` — canonical type consensus across structures

Graph construction uses [icebug](https://github.com/Ladybug-Memory/icebug) (a fork of networkit optimized for columnar memory). The `Graph.fromCSR()` method accepts zero-copy Arrow arrays, enabling a DuckDB → Arrow → CSR pipeline that eliminates Python per-element loops: DuckDB SQL (node indexing + edge dedup) → `fetch_arrow_table()` → numpy COO→CSR → `fromCSR()`. This avoids triple-buffering data (DuckDB + Python lists + graph) and is ~5x more memory efficient at scale via icebug's read-only columnar storage.

In prod, the `sec_knowledge_artifacts` Dagster asset downloads the DuckDB staging file from S3 (`s3://{user-bucket}/shared-repositories/databases/sec.duckdb`), builds artifacts on the Fargate task, then uploads them to `s3://{SHARED_PROCESSED_BUCKET}/sec/artifacts/`. The DuckDB file is published by the `sec_duckdb_s3_publish_job` after staging completes.

Artifacts are stored in `data/artifacts/` locally and `s3://{SHARED_PROCESSED_BUCKET}/sec/artifacts/` in prod. `SemanticEnricher` checks local disk first, then downloads from S3 if missing.

## Configuration

Key configuration in `config.py`:

- `XBRL_COLUMN_STANDARDIZATION` - Column name mapping
- `XBRL_EXTERNALIZATION_THRESHOLD` - Size threshold for S3 externalization
- `XBRL_STANDARDIZED_FILENAMES` - Output file naming

### Feature Flags

| Flag                              | Default | Purpose                                                                           |
| --------------------------------- | ------- | --------------------------------------------------------------------------------- |
| `XBRL_SEMANTIC_ENRICHMENT`        | `True`  | Enable fastembed-based canonical concept mapping and structure classification     |
| `XBRL_ASSOCIATION_CLASSIFICATION` | `True`  | Enable association-level disclosure classification (creates Classification nodes) |
| `XBRL_GRAPH_REFINEMENT`           | `True`  | Enable knowledge artifact-based confidence refinement                             |

## Related Documentation

- [SEC Pipeline](pipeline/README.md) - Dagster pipeline orchestration
- [Schemas](../../schemas/README.md) - Graph schema definitions
