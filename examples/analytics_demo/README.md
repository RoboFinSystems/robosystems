# SEC Graph Analytics Demo

Graph analytics for XBRL financial data using icebug (networkit).

## Prerequisites

Load SEC data into DuckDB first:

```bash
just sec-load NVDA 2025
```

## Usage

```bash
# Run both stages
just demo-analytics

# Classification only
just demo-analytics stage=1

# Normalization only
just demo-analytics stage=2

# Custom database path
just demo-analytics db=./path/to/sec.duckdb
```

## Stages

### Stage 1: Statement Classification

Classifies XBRL elements into financial statement categories (Income Statement, Balance Sheet, Cash Flow, Equity) using BFS from known root elements through calculation/presentation arc graphs.

### Stage 2: Element Normalization

Clusters equivalent elements across companies using community detection (PLM) and identifies canonical elements via PageRank. Works best with 5+ companies; single-company results are degraded.
