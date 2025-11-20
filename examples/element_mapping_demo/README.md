# Element Mapping Demo - Chart of Accounts to US-GAAP

## Overview

This demo showcases RoboSystems' **Element Mapping System** - the ability to aggregate granular Chart of Accounts elements into standardized reporting taxonomy elements (US-GAAP). It demonstrates:

- **Element Mapping Structures**: Create mappings between CoA and US-GAAP taxonomies
- **Multiple Aggregation Methods**: SUM, AVERAGE, WEIGHTED_AVERAGE, FIRST, LAST, CALCULATED
- **View Generation with Mappings**: Generate financial reports with automatic element aggregation
- **Real-world Use Case**: Transform 10+ bank accounts into a single "Cash and Cash Equivalents" line

## What Is Element Mapping?

Element mapping solves a critical accounting challenge: **aggregating granular bookkeeping accounts into standardized reporting concepts**.

**Example:**
```
Chart of Accounts (10 accounts):          US-GAAP Report (1 line):
- Checking Account - Operations  $50,000
- Checking Account - Payroll     $25,000
- Savings Account - Reserve      $15,000   →  Cash and Cash Equivalents: $100,000
- Money Market Account           $10,000
- ... (6 more accounts)
```

**Why This Matters:**
- **QuickBooks/Xero** use detailed accounts for operations
- **US-GAAP/IFRS** require standardized reporting elements
- **Element mappings** bridge this gap automatically

## Quick Start - Run The Demo

```bash
# Make sure RoboSystems is running
just start

# Run the complete element mapping demo
cd examples/element_mapping_demo
uv run main.py

# Or run individual steps
uv run 06_create_subgraph.py     # Create subgraph workspace with mappings
uv run 07_test_views.py          # Test view generation with mappings
```

## What The Demo Does

The demo builds on the accounting demo data and adds element mapping capabilities:

1. **Setup & Data** - Creates accounting graph with transactions (Steps 1-5, same as accounting demo)
2. **Create Mappings** - Builds element mapping structures:
   - Maps all checking/savings accounts → `us-gaap:CashAndCashEquivalents`
   - Maps all revenue accounts → `us-gaap:RevenueFromContractWithCustomer`
   - Maps all expense accounts → `us-gaap:OperatingExpenses`
3. **Generate Views** - Creates financial reports with mappings applied:
   - **Without mapping**: Shows all 20+ granular CoA accounts
   - **With mapping**: Shows aggregated US-GAAP concepts

## Demo Steps Explained

### Step 6: Create Subgraph Workspace

Creates a subgraph workspace and mapping structures that define how Chart of Accounts elements aggregate to US-GAAP:

```bash
uv run 06_create_subgraph.py
```

**What it creates:**
- **Cash Mapping**: All bank accounts → `us-gaap:CashAndCashEquivalents` (SUM aggregation)
- **Revenue Mapping**: All revenue accounts → `us-gaap:RevenueFromContractWithCustomer` (SUM)
- **Expense Mapping**: All expense accounts → `us-gaap:OperatingExpenses` (SUM)

**Example mapping structure:**
```json
{
  "name": "Cash Accounts to US-GAAP",
  "taxonomy_uri": "qb:chart-of-accounts",
  "target_taxonomy_uri": "us-gaap:2024",
  "associations": [
    {
      "source_element": "qb:CheckingAccount1",
      "target_element": "us-gaap:CashAndCashEquivalents",
      "aggregation_method": "sum",
      "weight": 1.0
    },
    {
      "source_element": "qb:SavingsAccount1",
      "target_element": "us-gaap:CashAndCashEquivalents",
      "aggregation_method": "sum",
      "weight": 1.0
    }
  ]
}
```

### Step 7: Generate Views with Mappings

Demonstrates view generation both with and without element mappings:

```bash
uv run 07_test_views.py
```

**Comparison:**

| Without Mapping | With Mapping |
|----------------|--------------|
| 20+ granular CoA accounts | 3-5 US-GAAP concepts |
| "Checking Account - Operations" | "Cash and Cash Equivalents" |
| "Consulting Revenue - Project A" | "Revenue From Contract With Customer" |
| "Office Rent" | "Operating Expenses" |

## Aggregation Methods

The demo showcases multiple aggregation methods:

### 1. SUM (Default)
Add all source element values together:
```
Checking 1: $50,000
Checking 2: $25,000
Savings:    $15,000
------------------------
Total:      $90,000
```

### 2. AVERAGE
Calculate mean value across source elements:
```
Account A: $100,000
Account B: $200,000
Account C: $150,000
------------------------
Average:   $150,000
```

### 3. WEIGHTED_AVERAGE
Weight source elements by importance:
```
Major Account (weight 3.0):  $300,000
Minor Account (weight 1.0):  $100,000
------------------------
Weighted Avg: $250,000  [(300k*3 + 100k*1) / 4]
```

### 4. FIRST / LAST
Take first or last value (useful for non-additive facts like headcount):
```
Q1 Headcount: 50
Q2 Headcount: 55
Q3 Headcount: 60
------------------------
FIRST: 50, LAST: 60
```

### 5. CALCULATED
Use formulas for complex calculations (future enhancement):
```
Formula: "Assets - Liabilities"
Result: Calculated equity value
```

## API Endpoints Used

The demo uses these RoboSystems API endpoints:

### Mapping Management
```bash
# Create mapping structure
POST /v1/graphs/{graph_id}/views/mappings
{
  "name": "CoA to US-GAAP",
  "taxonomy_uri": "qb:chart-of-accounts",
  "target_taxonomy_uri": "us-gaap:2024"
}

# List all mappings
GET /v1/graphs/{graph_id}/views/mappings

# Get specific mapping
GET /v1/graphs/{graph_id}/views/mappings/{structure_id}

# Delete mapping
DELETE /v1/graphs/{graph_id}/views/mappings/{structure_id}
```

### Association Management
```bash
# Add association to mapping
POST /v1/graphs/{graph_id}/views/mappings/{structure_id}/associations
{
  "source_element": "qb:CheckingAccount1",
  "target_element": "us-gaap:CashAndCashEquivalents",
  "aggregation_method": "sum",
  "weight": 1.0
}

# Update association
PATCH /v1/graphs/{graph_id}/views/mappings/{structure_id}/associations/{assoc_id}
{
  "weight": 2.0,
  "aggregation_method": "weighted_average"
}

# Delete association
DELETE /v1/graphs/{graph_id}/views/mappings/{structure_id}/associations/{assoc_id}
```

### View Generation with Mapping
```bash
# Create view with element mapping applied
POST /v1/graphs/{graph_id}/views
{
  "source": {
    "type": "transactions",
    "period_start": "2025-01-01",
    "period_end": "2025-06-30"
  },
  "mapping_structure_id": "mapping_abc123",  # ← Apply mapping!
  "view_config": {
    "rows": [{"type": "element"}],
    "columns": [{"type": "period"}]
  }
}
```

## Real-World Use Cases

### 1. QuickBooks to US-GAAP Reporting
**Challenge:** QuickBooks has 100+ accounts, US-GAAP needs 20-30 line items

**Solution:**
```python
# Map all revenue accounts
create_mapping("QuickBooks Revenue → US-GAAP Revenue")
  - Consulting Revenue - Project A → RevenueFromContractWithCustomer
  - Consulting Revenue - Project B → RevenueFromContractWithCustomer
  - Training Revenue → RevenueFromContractWithCustomer
```

### 2. Multi-Entity Consolidation
**Challenge:** Consolidate 5 subsidiaries with different chart of accounts

**Solution:**
```python
# Create mapping for each subsidiary
create_mapping("Subsidiary A CoA → Parent US-GAAP")
create_mapping("Subsidiary B CoA → Parent US-GAAP")

# Generate consolidated report
create_view(mapping_structure_id="consolidated_mapping")
```

### 3. Industry-Specific Reporting
**Challenge:** Healthcare provider needs both internal and regulatory reports

**Solution:**
```python
# Internal reporting (detailed CoA)
create_view()  # No mapping

# Regulatory reporting (standardized taxonomy)
create_view(mapping_structure_id="healthcare_regulatory_mapping")
```

## Technical Architecture

### Graph Database Schema

**Existing Nodes:**
- `Element`: Chart of Accounts and taxonomy concepts
- `Transaction`: Business transactions
- `LineItem`: Journal entries

**New Nodes (Reused from existing schema):**
- `Structure`: Element mapping container (type="ElementMapping")
- `Association`: Element-to-element mapping with aggregation rules

**Relationships:**
- `STRUCTURE_HAS_ASSOCIATION`: Links mapping to associations
- `ASSOCIATION_HAS_FROM_ELEMENT`: Source element (CoA)
- `ASSOCIATION_HAS_TO_ELEMENT`: Target element (US-GAAP)

### Processing Flow

```
1. Retrieve Facts
   ↓
2. Apply Element Mapping (if mapping_structure_id provided)
   - Group facts by target element
   - Aggregate values using specified method
   - Replace source element IDs with target element IDs
   ↓
3. Build FactGrid
   ↓
4. Generate Pivot Table
   ↓
5. Return View Response
```

## Advanced Usage

### Custom Weighted Aggregation

```python
# Create mapping with weighted averages
mapping = create_mapping_structure("Weighted Cash Positions")

# Major accounts weighted more heavily
create_association(
  source_element="qb:CheckingOperations",
  target_element="us-gaap:Cash",
  aggregation_method="weighted_average",
  weight=5.0  # High weight for main operating account
)

create_association(
  source_element="qb:PettyCash",
  target_element="us-gaap:Cash",
  aggregation_method="weighted_average",
  weight=0.1  # Low weight for petty cash
)
```

### Multi-Level Mappings

```python
# Level 1: Detailed CoA → Intermediate grouping
mapping_l1 = create_mapping("CoA → Departmental")
  - Sales Account A → Sales Department
  - Sales Account B → Sales Department
  - Engineering Account A → Engineering Department

# Level 2: Intermediate → US-GAAP
mapping_l2 = create_mapping("Departmental → US-GAAP")
  - Sales Department → Revenue
  - Engineering Department → R&D Expenses
```

### Formula-Based Calculations (Future)

```python
# Calculated aggregations using formulas
create_association(
  source_element="qb:GrossRevenue",
  target_element="us-gaap:NetRevenue",
  aggregation_method="calculated",
  formula="GrossRevenue - Returns - Discounts"
)
```

## Comparison: Before vs After

### Before Element Mapping
```
Balance Sheet (20+ lines):
  Checking Account - Operations    $50,000
  Checking Account - Payroll        $25,000
  Savings Account - Reserve         $15,000
  Money Market Account              $10,000
  Petty Cash                        $500
  Accounts Receivable - A           $30,000
  Accounts Receivable - B           $20,000
  ... (13 more accounts)
```

### After Element Mapping
```
Balance Sheet (US-GAAP, 5 lines):
  Cash and Cash Equivalents         $100,500
  Accounts Receivable               $50,000
  Property, Plant & Equipment       $75,000
  Total Assets                      $225,500
```

## Learn More

### Related RoboSystems Features

1. **View Engine** (`/robosystems/operations/views/`)
   - FactGrid builder
   - Pivot table generation
   - Aspect filtering

2. **XBRL Processing** (`/robosystems/processors/xbrl_graph.py`)
   - SEC filing ingestion
   - Taxonomy management
   - Presentation linkbases

3. **QuickBooks Integration** (`/robosystems/processors/qb_transactions.py`)
   - Transaction import
   - Chart of accounts sync
   - Structure/Association pattern (same as element mapping!)

### XBRL Connection

Element mapping follows XBRL taxonomy architecture:
- **Structures** = Presentation/Calculation linkbases
- **Associations** = Element-to-element relationships
- **Aggregation methods** = Calculation weights and arcroles

## Troubleshooting

**Problem:** Mapping not applied to view
**Solution:** Ensure `mapping_structure_id` is provided in CreateViewRequest:
```python
create_view(
  source=...,
  view_config=...,
  mapping_structure_id="mapping_abc123"  # Don't forget this!
)
```

**Problem:** Association not found
**Solution:** Verify source elements exist in graph:
```bash
just graph-query <graph_id> "MATCH (e:Element {uri: 'qb:CheckingAccount1'}) RETURN e"
```

**Problem:** Aggregated values incorrect
**Solution:** Check aggregation method matches use case:
- Monetary amounts: Use SUM
- Ratios/percentages: Use WEIGHTED_AVERAGE
- Headcount/shares: Use FIRST or LAST

## Success!

After running this demo, you have:

1. ✅ Understanding of element mapping architecture
2. ✅ Working examples of all aggregation methods
3. ✅ Real-world mapping structures (CoA → US-GAAP)
4. ✅ Comparative views (with/without mappings)
5. ✅ API knowledge for production implementation

## Next Steps

- **Extend mappings**: Add more sophisticated aggregation rules
- **Multi-period analysis**: Track mapping evolution over time
- **Custom taxonomies**: Create industry-specific reporting frameworks
- **Automated mapping**: Use ML to suggest element mappings

## Support

For questions or issues:
- Review the main [Examples README](../README.md)
- Check [RoboSystems Documentation](../../README.md)
- Open an issue on [GitHub](https://github.com/RoboFinSystems/robosystems/issues)
