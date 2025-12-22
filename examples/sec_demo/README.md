# SEC Demo - Quick Start Guide

## Overview

This demo showcases RoboSystems' integration with the SEC (Securities and Exchange Commission) XBRL financial data repository. It provides access to real public company financial statements and allows you to query financial data across thousands of publicly traded companies.

**What you get:**
- Access to the SEC shared repository (thousands of public companies)
- Real XBRL financial data from 10-K and 10-Q filings
- Balance sheets, income statements, and cash flow data
- Financial facts with complete dimensional context
- Example queries for financial analysis

## Quick Start

```bash
# Make sure RoboSystems is running
just start

# Load and query NVIDIA's 2025 SEC data (includes example queries)
just demo-sec NVDA 2025

# Load data without running queries
just demo-sec NVDA 2025 true

# Query SEC data with examples
just demo-sec-query

# Run all available query examples
just demo-sec-query true
```

## What The Demo Does

When you run `just demo-sec NVDA 2025`, it automatically:

1. **Sets up credentials** - Creates a user account and API key (or reuses existing)
2. **Loads SEC data** - Fetches NVIDIA's 2025 10-K/10-Q filings from EDGAR
3. **Creates subscription** - Subscribes you to the SEC shared repository
4. **Processes XBRL** - Extracts entities, reports, facts, elements, and dimensions
5. **Runs queries** - Executes example queries to demonstrate capabilities (unless skipped)

## Available Companies

You can query any publicly traded US company with SEC filings. Popular examples:

**Technology:**
- NVDA - NVIDIA Corporation
- AAPL - Apple Inc.
- MSFT - Microsoft Corporation
- GOOGL - Alphabet Inc.
- META - Meta Platforms
- TSLA - Tesla Inc.

**Financial:**
- JPM - JPMorgan Chase
- BAC - Bank of America
- WFC - Wells Fargo
- GS - Goldman Sachs

**Consumer:**
- AMZN - Amazon
- WMT - Walmart
- HD - Home Depot
- NKE - Nike

**Healthcare:**
- JNJ - Johnson & Johnson
- UNH - UnitedHealth Group
- PFE - Pfizer

Use the company's ticker symbol with the `just demo-sec` command.

## Advanced Usage

### Load Specific Company and Year

```bash
# Load Apple's 2024 financials
just demo-sec AAPL 2024

# Load Microsoft's 2023 financials
just demo-sec MSFT 2023

# Skip the query examples (faster)
just demo-sec TSLA 2025 true
```

### Run Query Examples

```bash
# Interactive query selection
just demo-sec-query

# Run all preset queries
just demo-sec-query true

# Or run directly with Python
cd examples/sec_demo
uv run query_examples.py --all
```

### Individual Steps

For more control, you can run the scripts directly:

```bash
# Setup credentials
just demo-user

# Load SEC data and create subscription
cd examples/sec_demo
uv run main.py --ticker NVDA --year 2025 --skip-queries

# Run queries separately
uv run query_examples.py --all
```

## Available Preset Queries

### Summary
Overview of node and relationship counts in the SEC repository.

```cypher
MATCH (n)
WITH labels(n) AS label, count(n) AS count
RETURN label, count
ORDER BY count DESC
```

### Entities
List public companies with their basic information.

```cypher
MATCH (e:Entity)
WHERE e.entity_type = 'operating'
RETURN
    e.ticker AS ticker,
    e.name AS company_name,
    e.cik AS cik,
    e.industry AS industry,
    e.state_of_incorporation AS state
ORDER BY e.ticker
LIMIT 20
```

### Recent Reports
Most recent SEC filings by entity.

```cypher
MATCH (e:Entity)-[:ENTITY_HAS_REPORT]->(r:Report)
RETURN
    e.ticker AS ticker,
    e.name AS company,
    r.form AS form_type,
    r.report_date AS report_date,
    r.filing_date AS filing_date
ORDER BY r.filing_date DESC
LIMIT 25
```

### Report Types
Count of reports by form type (10-K, 10-Q, etc.).

```cypher
MATCH (r:Report)
RETURN
    r.form AS form_type,
    count(*) AS report_count
ORDER BY report_count DESC
```

### Financial Facts
Sample of financial facts and their metadata.

```cypher
MATCH (r:Report)-[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(e:Element)
WHERE f.numeric_value IS NOT NULL
RETURN
    r.report_date AS report_date,
    e.qname AS element,
    f.numeric_value AS value,
    f.unit_ref AS unit,
    f.decimals AS decimals
LIMIT 20
```

### Balance Sheet
View a company's balance sheet line items.

```cypher
MATCH (e:Entity {ticker: 'NVDA'})-[:ENTITY_HAS_REPORT]->(r:Report)
      -[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE r.form = '10-K'
  AND el.qname IN [
    'us-gaap:Assets',
    'us-gaap:AssetsCurrent',
    'us-gaap:Liabilities',
    'us-gaap:LiabilitiesCurrent',
    'us-gaap:StockholdersEquity'
  ]
RETURN
    r.report_date AS period_end,
    el.qname AS account,
    f.numeric_value AS amount
ORDER BY r.report_date DESC, el.qname
```

### Income Statement
Quarterly revenue and net income trends.

```cypher
MATCH (e:Entity {ticker: 'NVDA'})-[:ENTITY_HAS_REPORT]->(r:Report)
      -[:REPORT_HAS_FACT]->(f:Fact)-[:FACT_HAS_ELEMENT]->(el:Element)
WHERE r.form = '10-Q'
  AND el.qname IN ['us-gaap:Revenues', 'us-gaap:NetIncomeLoss']
RETURN
    r.report_date AS quarter_end,
    el.qname AS metric,
    f.numeric_value AS amount
ORDER BY r.report_date DESC, el.qname
```

## Custom Queries

Run your own Cypher queries against the SEC data:

```bash
# Using just command
just graph-query sec "MATCH (e:Entity {ticker: 'AAPL'}) RETURN e"

# Using the query script
cd examples/sec_demo
uv run query_examples.py --query "MATCH (e:Entity) WHERE e.ticker STARTS WITH 'A' RETURN e.ticker, e.name LIMIT 10"
```

## Understanding SEC Data

### Graph Schema

**Nodes:**
- `Entity`: Public companies (ticker, name, CIK, industry, etc.)
- `Report`: SEC filings (10-K, 10-Q, 8-K, etc.)
- `Fact`: Individual financial facts/line items
- `Element`: XBRL taxonomy elements (Assets, Revenue, etc.)
- `Dimension`: Contextual dimensions (segment, geography, product line)

**Relationships:**
- `ENTITY_HAS_REPORT`: Links companies to their filings
- `REPORT_HAS_FACT`: Links reports to financial facts
- `FACT_HAS_ELEMENT`: Links facts to taxonomy elements
- `FACT_HAS_DIMENSION`: Links facts to contextual dimensions

### XBRL Elements

Common XBRL elements you'll encounter:
- `us-gaap:Assets` - Total assets
- `us-gaap:AssetsCurrent` - Current assets
- `us-gaap:Liabilities` - Total liabilities
- `us-gaap:StockholdersEquity` - Shareholders' equity
- `us-gaap:Revenues` - Total revenue
- `us-gaap:NetIncomeLoss` - Net income
- `us-gaap:CashAndCashEquivalentsAtCarryingValue` - Cash position

### Report Types

- **10-K**: Annual report with complete financial statements
- **10-Q**: Quarterly report with unaudited financials
- **8-K**: Current report for material events
- **20-F**: Annual report for foreign issuers
- **S-1**: Registration statement for IPOs

## Troubleshooting

**Problem:** Connection error or "API unavailable"
**Solution:** Ensure RoboSystems is running:
```bash
just start
just logs robosystems-api  # Check API logs
```

**Problem:** "No credentials found"
**Solution:** Run the credential setup:
```bash
just demo-user
```

**Problem:** Demo fails with authentication error
**Solution:** Recreate credentials:
```bash
just demo-user --force
```

**Problem:** "Company not found" or no data returned
**Solution:**
- Verify the ticker symbol is correct (use uppercase, e.g., NVDA not nvda)
- Check that the company has filed reports for the specified year
- Try a more recent year (companies may not have historical data loaded)

**Problem:** SEC load takes a long time
**Solution:**
- This is normal - processing XBRL data is complex
- The script shows progress as it works
- First-time loads take longer (subsequent loads use cached data)

## Tips

- The SEC repository is shared across all users (not isolated like entity graphs)
- Data is continuously updated as new filings are submitted
- Use `just graph-query sec "<cypher>"` for ad-hoc queries
- The ticker parameter is case-insensitive but conventionally uppercase
- Year parameter is required and should match available fiscal years
- Financial data includes full dimensional context (segments, periods, etc.)
- Check the SEC EDGAR website if you need to verify filing availability

## Success!

After running the demo, you have:

1. User account & API key (shared across all demos)
2. Access to the SEC shared repository
3. Real public company financial data
4. Ready-to-use query examples for financial analysis

Explore public company financials with `just graph-query sec "<query>"` or build your own financial analysis tools!

## Learn More

### SEC XBRL Resources

- **[SEC EDGAR](https://www.sec.gov/edgar)** - Official SEC filing system
- **[XBRL US](https://xbrl.us/)** - XBRL taxonomy and standards
- **[GAAP Taxonomy](https://www.fasb.org/xbrl)** - US GAAP XBRL elements

### RoboSystems Documentation

- **[Examples README](../README.md)** - Overview of all demos
- **[Main README](../../README.md)** - Platform documentation
- **[SEC Adapter](../../robosystems/adapters/sec/)** - SEC XBRL processing
- **[SEC XBRL Pipeline Wiki](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline)** - Detailed SEC integration guide

## Support

For questions or issues:
- Check the [Examples README](../README.md) for overview of all demos
- Review the [SEC XBRL Pipeline Wiki](https://github.com/RoboFinSystems/robosystems/wiki/SEC-XBRL-Pipeline)
- Open an issue on [GitHub](https://github.com/RoboFinSystems/robosystems/issues)
