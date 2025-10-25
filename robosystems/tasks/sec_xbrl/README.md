# SEC XBRL Processing Pipeline

This module contains all SEC-specific tasks for processing XBRL filings and ingesting them into the Kuzu graph database.

## Module Structure

```
sec_xbrl/
├── __init__.py           # Module exports
├── orchestration.py      # Phase-based pipeline orchestration
├── consolidation.py      # Parquet file consolidation for performance
├── ingestion.py          # Kuzu database ingestion
└── maintenance.py        # Database reset and cleanup tasks
```

## Pipeline Architecture

### Overview

The SEC pipeline processes XBRL filings through four stages:

1. **Download** - Fetch XBRL files from SEC (rate-limited)
2. **Process** - Convert XBRL to parquet (unlimited parallelism)
3. **Consolidate** - Combine small parquet files into optimal sizes (parallel)
4. **Ingestion** - Load into Kuzu from consolidated files

### Stage Details

#### Stage 1: Download
- Downloads XBRL ZIP files from SEC to S3 raw bucket
- Rate-limited to respect SEC API limits
- Queue: `shared-extraction` (rate-limited workers)

#### Stage 2: Process
- Converts XBRL to parquet using XBRLGraphProcessor
- Uses modular XBRL processing components (id_utils, naming_utils, dataframe_manager, parquet_writer, textblock_externalizer)
- Outputs to `processed/year={year}/nodes/` and `processed/year={year}/relationships/`
- Queue: `shared-processing` (unlimited parallelism)
- Generates many small files (50KB each)

#### Stage 3: Consolidate
- Combines small parquet files into 256MB consolidated files
- Streaming processing to avoid memory issues
- Outputs to `consolidated/year={year}/nodes/` and `consolidated/year={year}/relationships/`
- Queue: `shared-processing` (up to 30 parallel tasks)
- 100-1000x reduction in file count

#### Stage 4: Ingestion
- Uses Kuzu's native S3 COPY FROM with consolidated files
- Reads from `consolidated/` when use_consolidated=True
- IGNORE_ERRORS=true handles any duplicates
- Loads nodes first, then relationships
- Queue: `shared-ingestion` (single-threaded)
- 3-5x faster than processing many small files

## Key Components

### orchestration.py
Phase-based orchestration tasks:
- `plan_phased_processing()` - Creates processing plan with phases
- `start_phase()` - Starts a specific phase (download/process/consolidate/ingest)
- `download_company_filings()` - Downloads XBRL files for a company
- `process_company_filings()` - Processes XBRL to parquet
- `get_phase_status()` - Gets current pipeline status

### consolidation.py
File consolidation for performance:
- `consolidate_parquet_files()` - Consolidates files for a specific table/year
- `orchestrate_consolidation_phase()` - Orchestrates all consolidation tasks
- Streaming processing to avoid memory issues
- Target file size: 256MB for optimal Kuzu performance

### ingestion.py
Kuzu bulk loading:
- `ingest_sec_data()` - Main ingestion task
- Configures Kuzu httpfs extension for S3
- Handles LocalStack/production S3 endpoints
- Ensures proper connection cleanup

## Usage

### Run Full Pipeline
```python
from robosystems.tasks.sec_xbrl.orchestration import plan_phased_processing, start_phase

# Create plan for processing
plan = plan_phased_processing.delay(
    start_year=2024,
    end_year=2025,
    max_companies=10,
)

# Execute phases
start_phase.delay(phase="download")
start_phase.delay(phase="process")
start_phase.delay(phase="consolidate")
start_phase.delay(phase="ingest")
```

### Run Ingestion Only
```python
from robosystems.tasks.sec_xbrl import ingest_sec_data

# Ingest existing processed files
result = ingest_sec_data.delay(
    pipeline_run_id="manual_ingest",
    year=2025,
    bucket="robosystems-sec-processed",
)
```

### Command Line
```bash
# Full pipeline
python robosystems/scripts/sec_pipeline.py full --year 2025 --companies 10 --filings 5

# Ingestion only
python robosystems/scripts/sec_pipeline.py ingest --year 2025

# Monitor progress
python robosystems/scripts/sec_pipeline.py monitor --pipeline-id sec_bulk_2025_123456
```

## S3 Structure

```
robosystems-sec-processed/
└── processed/
    └── year=2025/                # Final processed files
        ├── nodes/
        │   ├── Entity_{timestamp}_{cik}_{accession}.parquet
        │   ├── Report_{timestamp}_{cik}_{accession}.parquet
        │   └── Fact_{timestamp}_{cik}_{accession}.parquet
        └── relationships/
            ├── ENTITY_HAS_REPORT_{timestamp}_{cik}_{accession}.parquet
            └── REPORT_HAS_FACT_{timestamp}_{cik}_{accession}.parquet
```

## Configuration

### Queue Settings (workers.yml)
- `shared-extraction`: Max 2 workers (SEC rate limiting)
- `shared-processing`: Max 30 workers (heavy processing)
- `shared-ingestion`: Max 1 worker (Kuzu single-threaded)

### Environment Variables
- `SEC_PROCESSED_BUCKET`: S3 bucket for processed files
- `SEC_RAW_BUCKET`: S3 bucket for raw XBRL files
- `QUEUE_SHARED_PROCESSING`: Processing queue name
- `QUEUE_SHARED_INGESTION`: Ingestion queue name
- `QUEUE_SHARED_EXTRACTION`: Extraction queue name

## Safety Features

1. **Phase Synchronization**: Ensures phases complete in order
2. **IGNORE_ERRORS**: Handles duplicates automatically during ingestion
3. **Queue Management**: Separate queues prevent resource conflicts
4. **Connection Cleanup**: Prevents Kuzu database locking
5. **Error Isolation**: Failed companies don't block pipeline

## Performance

- **Direct Ingestion**: Eliminates consolidation overhead
- **Native S3 Loading**: Kuzu reads directly from S3 using wildcards
- **Unlimited Parallelism**: Process phase has no concurrency limits
- **Deduplication**: IGNORE_ERRORS handles duplicates efficiently

## Monitoring

Track pipeline progress via:
- Celery Flower dashboard
- CloudWatch metrics
- Redis pipeline tracking keys
- Graph API logs

## Error Recovery

If processing fails for specific companies:
1. Check logs for the failed company
2. Use `smart_retry_failed_companies()` to retry failures
3. Other companies continue processing
4. Pipeline provides detailed failure report

## XBRL Processing Architecture

The XBRLGraphProcessor has been modularized into specialized components in `robosystems/processors/xbrl/`:

### Core Components

1. **id_utils.py** - Deterministic UUIDv7 generation
   - `create_entity_id()`, `create_report_id()`, `create_fact_id()`, etc.
   - Ensures consistent IDs across processing runs

2. **naming_utils.py** - String transformation utilities
   - `camel_to_snake()` - Convert PascalCase to snake_case
   - `make_plural()` - Pluralization for table names
   - `convert_schema_name_to_filename()` - Schema to file mapping

3. **dataframe_manager.py** - DataFrame lifecycle management
   - Schema-driven DataFrame initialization
   - Type mapping and validation
   - Deduplication strategies

4. **parquet_writer.py** - Schema-aware file I/O
   - Standardized filename generation
   - Type prefix support
   - Column standardization
   - Handles both nodes/ and relationships/ directories

5. **textblock_externalizer.py** - S3 externalization for large text
   - Content-based caching (SHA-256 hashing)
   - Batch upload optimization
   - Handles HTML and plain text content
   - CDN URL support for public access

### Benefits

- **Testability**: Each component can be tested in isolation
- **Maintainability**: Clear separation of concerns
- **Reusability**: Components can be used beyond SEC processing
- **Performance**: Optimized batch operations and caching

## Future Improvements

Potential optimizations:
- Parallel node ingestion for independent types
- Streaming ingestion as files become available
- Adaptive batch sizing based on data characteristics
- Incremental processing for daily updates