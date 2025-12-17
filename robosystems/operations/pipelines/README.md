# Pipeline Operations - Migrated to Dagster

Data pipelines have been migrated to Dagster for improved orchestration, observability, and scalability.

## New Location

- **SEC Pipeline**: `robosystems/dagster/assets/sec.py`
- **SEC Jobs**: `robosystems/dagster/jobs/sec.py`

## Usage

### Local Development
```bash
# Load a single company
just sec-load NVDA 2025

# Reset SEC database
just sec-reset

# Materialize existing processed files
just sec-materialize
```

### Production
- Use Dagster UI at `dagster.robosystems.app`
- Schedule: `sec_daily_rebuild_schedule` runs daily at 2 AM

## Architecture

The Dagster SEC pipeline uses 5 assets:

1. `sec_companies_list` - Fetch company list from SEC
2. `sec_raw_filings` - Download XBRL ZIPs (year-partitioned)
3. `sec_processed_filings` - Process to parquet (year-partitioned)
4. `sec_duckdb_staging` - Create DuckDB staging tables
5. `sec_graph_materialized` - Materialize to LadybugDB

## Adapters (Still in Use)

The following adapters remain in `robosystems/adapters/sec/`:

- **SECClient** (`client/edgar.py`) - SEC EDGAR API client
- **XBRLGraphProcessor** (`processors/graph.py`) - XBRL to graph processing
- **XBRLDuckDBGraphProcessor** (`processors/duckdb_graph_ingestion.py`) - DuckDB staging and LadybugDB ingestion
