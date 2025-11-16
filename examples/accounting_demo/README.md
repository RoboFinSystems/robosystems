# Accounting Demo - Quick Start Guide

## Overview

This demo showcases RoboSystems' graph database capabilities for accounting and financial data. It creates a complete accounting system with:

- **1 Entity**: Acme Consulting LLC (fictional consulting company)
- **20 Accounts**: Complete chart of accounts (Assets, Liabilities, Equity, Revenue, Expenses)
- **~30 Transactions**: 6 months of business transactions per month (rent, consulting revenue, salaries, expenses)
- **~60 Line Items**: Double-entry journal entries

## Quick Start - Run All Steps

```bash
# Make sure RoboSystems is running
just start

# Run the complete demo (all steps automatically)
just demo-accounting

# Or create a new graph explicitly
just demo-accounting "new-graph"

# Or reuse an existing graph
just demo-accounting "reuse-graph"
```

## What The Demo Does

When you run `just demo-accounting`, it automatically:

1. **Sets up credentials** - Creates a user account and API key (or reuses existing)
2. **Creates graph** - Initializes a new graph database with accounting schema
3. **Generates data** - Creates 6 months of realistic accounting transactions
4. **Uploads & ingests** - Loads data into the graph via staging tables
5. **Runs queries** - Executes example queries to demonstrate capabilities

## Advanced Usage - Individual Steps

The `just demo-accounting` command runs `main.py` which executes all steps automatically. For manual control, you can run individual steps:

### Step 1: Setup Credentials

```bash
# Using just command (recommended)
just demo-user

# Or run directly with options
cd examples/accounting_demo
uv run 01_setup_credentials.py --name "Your Name" --email your@email.com
```

**Output**: User created, API key generated, credentials saved to `examples/credentials/config.json`

### Step 2-5: Run All Remaining Steps

```bash
# Run the main demo script
cd examples/accounting_demo
uv run main.py --flags "new-graph"

# Or run each step individually
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

**Output**: Graph created with 6 months of accounting data, example queries executed

## Available Preset Queries

### Chart of Accounts
View the complete chart of accounts:
```cypher
MATCH (e:Element)
WHERE e.classification IS NOT NULL
RETURN e.name, e.classification, e.balance
ORDER BY e.name
```

### Trial Balance
Calculate account balances using double-entry bookkeeping:
```cypher
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
RETURN
    e.name AS account,
    e.classification AS type,
    sum(li.debit_amount) AS total_debits,
    sum(li.credit_amount) AS total_credits,
    sum(li.debit_amount) - sum(li.credit_amount) AS net_balance
ORDER BY e.name
```

### Income Statement
View revenue and expenses:
```cypher
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification IN ['revenue', 'expense']
RETURN
    e.classification AS category,
    e.name AS account,
    sum(li.credit_amount) - sum(li.debit_amount) AS amount
ORDER BY e.classification, e.name
```

### Cash Flow
See all transactions affecting the Cash account:
```cypher
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.name = 'Cash'
RETURN
    t.date AS date,
    t.description AS description,
    li.debit_amount AS cash_in,
    li.credit_amount AS cash_out
ORDER BY t.date DESC
LIMIT 20
```

### Revenue by Month
Analyze revenue trends:
```cypher
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification = 'revenue'
RETURN
    substring(t.date, 1, 7) AS month,
    sum(li.credit_amount) AS total_revenue
ORDER BY month
```

### Profitability Analysis
Monthly profit/loss:
```cypher
MATCH (t:Transaction)-[:TRANSACTION_HAS_LINE_ITEM]->(li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification IN ['revenue', 'expense']
WITH
    substring(t.date, 1, 7) AS month,
    e.classification AS type,
    CASE
        WHEN e.classification = 'revenue' THEN sum(li.credit_amount)
        WHEN e.classification = 'expense' THEN sum(li.debit_amount)
        ELSE 0
    END AS amount
RETURN
    month,
    sum(CASE WHEN type = 'revenue' THEN amount ELSE 0 END) AS revenue,
    sum(CASE WHEN type = 'expense' THEN amount ELSE 0 END) AS expenses,
    sum(CASE WHEN type = 'revenue' THEN amount ELSE -amount END) AS profit
ORDER BY month
```

## Technical Details

### Graph Schema

**Nodes:**
- `Entity`: The business entity (Acme Consulting LLC)
- `Element`: Chart of accounts (individual accounts)
- `Transaction`: Business transactions
- `LineItem`: Journal entry lines (debits and credits)

**Relationships:**
- `TRANSACTION_HAS_LINE_ITEM`: Links transactions to their journal entries
- `LINE_ITEM_RELATES_TO_ELEMENT`: Links journal entries to accounts

### Data Format

All data is uploaded as Parquet files for optimal performance:
- `Entity.parquet`: Business entity metadata
- `Element.parquet`: Chart of accounts
- `Transaction.parquet`: Transaction metadata
- `LineItem.parquet`: Double-entry journal entries
- `TRANSACTION_HAS_LINE_ITEM.parquet`: Transaction-to-line-item relationships
- `LINE_ITEM_RELATES_TO_ELEMENT.parquet`: Line-item-to-account relationships

### Accounting Principles

This demo follows standard accounting principles:
- **Double-entry bookkeeping**: Every transaction has equal debits and credits
- **Account types**: Assets, Liabilities, Equity, Revenue, Expenses
- **Normal balances**: Assets/Expenses are debits, Liabilities/Equity/Revenue are credits
- **Period types**: Balance sheet accounts use "instant", income statement accounts use "duration"

## Advanced Usage

### Custom Queries

Run custom Cypher queries against your accounting graph:
```bash
# Using just command
just graph-query <graph_id> "MATCH (e:Element) WHERE e.classification = 'expense' RETURN e.name, e.balance"

# Or use the query script
cd examples/accounting_demo
uv run 05_query_graph.py --query "MATCH (n) RETURN count(n)"
```

### Generate More Data

To generate more than the default 6 months:
```bash
cd examples/accounting_demo
uv run 03_generate_data.py --months 12 --regenerate
uv run 04_upload_ingest.py
```

### Interactive Mode

Enter interactive mode for ad-hoc queries:
```bash
cd examples/accounting_demo
uv run 05_query_graph.py

# Then type preset names or custom queries
> trial_balance
> cash_flow
> MATCH (t:Transaction) RETURN count(t)
> quit
```

## Learn More

### Graph Database Benefits for Accounting

1. **Relationship Tracking**: Easily trace transactions through accounts
2. **Flexible Queries**: Ad-hoc analysis without predefined reports
3. **Auditability**: Complete transaction history with full lineage
4. **Real-time Analysis**: Query current balances and trends instantly
5. **Complex Relationships**: Support for multi-dimensional analysis

### Integration Patterns

This demo shows patterns used in real RoboSystems integrations:

- **XBRL Processing**: SEC financial filings (`/robosystems/processors/xbrl_graph.py`)
- **QuickBooks Sync**: Small business accounting (`/robosystems/processors/qb_transactions.py`)
- **Custom ERPs**: Any double-entry accounting system

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

**Problem:** Want to start fresh with a new graph
**Solution:** Use the new-graph flag:
```bash
just demo-accounting "new-graph"
```

## Tips

- The `just demo-accounting` command handles all setup automatically
- Credentials are saved in `examples/credentials/config.json` and reused across all demos
- Generated data files are saved in `examples/accounting_demo/data/` for inspection
- Use `just graph-query <graph_id> "<cypher>"` for ad-hoc queries
- Check `just logs robosystems-api` if you encounter issues

## Success!

After running the demo, you have:

1. User account & API key (shared across all demos)
2. Complete accounting graph with realistic data
3. 6 months of transaction history
4. Ready-to-use query examples

Explore the data further with `just graph-query` or the query script!

## Support

For questions or issues:
- Check the [Examples README](../README.md) for overview of all demos
- Review the main [README.md](../../README.md) for platform documentation
- Open an issue on [GitHub](https://github.com/RoboFinSystems/robosystems/issues)
