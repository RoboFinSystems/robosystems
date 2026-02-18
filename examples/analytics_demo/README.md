# SEC Graph Analytics Demo

Graph analytics for XBRL financial data using icebug (networkit).

## Prerequisites

A DuckDB staging database is required. Either:

- **Local pipeline**: `just sec-load NVDA 2025` (small, single company)
- **Download from S3**: Full SEC database (~38 GB) for production-scale analysis

## Usage

```bash
# Run both stages on sec.duckdb (default)
just demo-analytics

# Classification only
just demo-analytics sec 1

# Normalization only
just demo-analytics sec 2

# Use historical database
just demo-analytics sec_historical

# Lower memory for large databases on laptops
uv run examples/analytics_demo/main.py sec --memory 2GB
```

The `--memory` flag controls DuckDB's memory limit (default: 4GB). DuckDB spills to disk when it hits this limit. Lower it if your machine struggles with the full 38GB database.

## Stages

### Stage 1: Statement Classification

Classifies XBRL elements into financial statement categories (Income Statement, Balance Sheet, Cash Flow, Equity) using BFS from known root elements through calculation/presentation arc graphs.

**Output**: Summary tables showing element counts per statement type, top elements per statement, multi-statement elements, and structural metrics.

### Stage 2: Element Normalization

Clusters equivalent elements across companies using community detection (PLM) and identifies canonical elements via PageRank. Works best with 5+ companies; single-company results are degraded.

**Output**: Cluster membership tables and predicted missing equivalence links.

## Scale Reference

On the full SEC database (~38 GB):

- ~4.8M calculation arcs
- ~43.7M presentation arcs
- ~2.1M element graph nodes, ~3.6M edges
