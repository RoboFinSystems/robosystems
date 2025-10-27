# Accounting Demo - Quick Start Guide

## Overview

This demo showcases RoboSystems' graph database capabilities for accounting and financial data. It creates a complete accounting system with:

- **1 Entity**: Acme Consulting LLC (fictional consulting company)
- **20 Accounts**: Complete chart of accounts (Assets, Liabilities, Equity, Revenue, Expenses)
- **~30 Transactions**: 6 months of business transactions per month (rent, consulting revenue, salaries, expenses)
- **~60 Line Items**: Double-entry journal entries

## 🚀 Quick Start - Run All Steps

```bash
# Make sure RoboSystems is running
just start robosystems

# Navigate to the demo directory
cd examples/accounting_demo

# Run each step in sequence
uv run 01_setup_credentials.py
uv run 02_create_graph.py
uv run 03_generate_data.py
uv run 04_upload_ingest.py
uv run 05_query_graph.py --all
```

## 📋 Step-by-Step Guide

### Step 1: Setup Credentials

Creates a user account and API key, saves credentials to `credentials/config.json`.

```bash
uv run 01_setup_credentials.py

# Options:
uv run 01_setup_credentials.py --name "Your Name"
uv run 01_setup_credentials.py --email your@email.com
uv run 01_setup_credentials.py --force  # Create new credentials
```

**Output**: User created, API key generated, credentials saved

### Step 2: Create Graph

Creates a new graph database for the accounting demo.

```bash
uv run 02_create_graph.py

# Options:
uv run 02_create_graph.py --name "My Accounting Demo"
uv run 02_create_graph.py --reuse  # Reuse existing graph
```

**Output**: Graph created, graph_id saved to credentials

### Step 3: Generate Data

Generates realistic accounting data as Parquet files.

```bash
uv run 03_generate_data.py

# Options:
uv run 03_generate_data.py --months 12  # Generate 12 months of data
uv run 03_generate_data.py --regenerate  # Force regenerate
```

**Output**: 6 Parquet files created in `data/` directory:
- `Entity.parquet` - Business entity
- `Element.parquet` - Chart of accounts
- `Transaction.parquet` - Financial transactions
- `LineItem.parquet` - Journal entry lines
- `TRANSACTION_HAS_LINE_ITEM.parquet` - Relationships
- `LINE_ITEM_RELATES_TO_ELEMENT.parquet` - Relationships

### Step 4: Upload & Ingest

Uploads the Parquet files and ingests them into the graph.

```bash
uv run 04_upload_ingest.py
```

**Output**: All files uploaded, data ingested into graph, verification queries run

### Step 5: Query Graph

Run example queries or enter interactive mode.

```bash
# Run all preset queries
uv run 05_query_graph.py --all

# Run a specific preset
uv run 05_query_graph.py --preset trial_balance
uv run 05_query_graph.py --preset income_statement
uv run 05_query_graph.py --preset cash_flow

# Run a custom query
uv run 05_query_graph.py --query "MATCH (n) RETURN count(n)"

# Interactive mode
uv run 05_query_graph.py
```

## 📊 Available Preset Queries

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

## 🛠️ Technical Details

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

## 🔧 Advanced Usage

### Generate More Data

Generate 12 months of data instead of 6:
```bash
uv run 03_generate_data.py --months 12 --regenerate
uv run 04_upload_ingest.py
```

### Custom Queries

Run custom Cypher queries:
```bash
# Find all expenses over $500
uv run 05_query_graph.py --query "
MATCH (li:LineItem)-[:LINE_ITEM_RELATES_TO_ELEMENT]->(e:Element)
WHERE e.classification = 'expense' AND li.debit_amount > 500
RETURN e.name, li.description, li.debit_amount
ORDER BY li.debit_amount DESC
"
```

### Interactive Mode

Enter interactive mode for ad-hoc queries:
```bash
uv run 05_query_graph.py

# Then type preset names or custom queries
> trial_balance
> cash_flow
> MATCH (t:Transaction) RETURN count(t)
> quit
```

## 📚 Learn More

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

## 🐛 Troubleshooting

**Problem:** Script fails with "No credentials found"
**Solution:** Run step 1 first:
```bash
uv run 01_setup_credentials.py
```

**Problem:** Script fails with "No graph_id found"
**Solution:** Run step 2 first:
```bash
uv run 02_create_graph.py
```

**Problem:** Script fails with "No parquet files found"
**Solution:** Run step 3 first:
```bash
uv run 03_generate_data.py
```

**Problem:** Connection error
**Solution:** Ensure RoboSystems is running:
```bash
just start robosystems
```

**Problem:** Import errors
**Solution:** Install dev dependencies:
```bash
just install
```

## 💡 Tips

- All scripts can be run independently after their dependencies are met
- Credentials and data are saved locally and reused across runs
- Use `--force` or `--regenerate` flags to start fresh
- The demo uses auto-generated test data - perfect for exploring the API
- Check the generated Parquet files in `data/` to see the data structure

## 🎉 Success!

After running all steps, you have:

1. ✅ User account & API key
2. ✅ Complete accounting graph
3. ✅ 6 months of transaction data
4. ✅ Ready-to-use query examples

Happy querying!

## 📞 Support

For questions or issues:
- Check the main project [README.md](../../README.md)
- Review the [CLAUDE.md](../../CLAUDE.md) development guide
- Open an issue on GitHub
