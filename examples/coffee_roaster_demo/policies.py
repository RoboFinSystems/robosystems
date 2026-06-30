"""Accounting policy documents for Driftline Coffee Roasters.

Uploaded to the document store (PostgreSQL + OpenSearch) so the AI can search
and cite them during the month-end close. Tailored to a DTC + wholesale coffee
roaster: inventory/COGS, deferred subscription revenue, and the wholesale AR
aging that drives this episode's working-capital reveal.
"""

CLOSE_PROCEDURES = """# Month-End Close Procedures — Driftline Coffee Roasters

## Overview

Driftline Coffee Roasters is a specialty coffee roaster selling direct-to-consumer (DTC subscriptions and one-off web orders) and wholesale (regional grocery chains and cafés). The books are maintained on the accrual basis. Close the books by the 8th business day of the following month.

## Close Checklist

### 1. Bank Reconciliation
- Reconcile Operating Checking to the bank statement
- Verify all deposits (DTC payouts, wholesale collections) and payments are recorded

### 2. Inventory & Cost of Goods Sold
- Reconcile Inventory — Green Coffee (1400) to the warehouse count
- Confirm green-coffee purchases received from the importer are recorded (DR Inventory / CR AP or Cash)
- Recognize cost of goods sold for units sold this month: DR Cost of Goods Sold (5000) / CR Inventory — Green Coffee (1400)
- Watch for inventory building faster than sales — a green-coffee pre-buy ties up cash even while the P&L looks healthy

### 3. Accounts Receivable Review (wholesale)
- Confirm all wholesale invoices for the month have been issued
- Review AR aging for overdue balances; flag any single customer that is a large, aging share of total AR
- Net terms are 30 days; investigate accounts slipping past 60 days

### 4. Deferred Subscription Revenue
- DTC subscribers prepay; cash collected is recorded as Deferred Subscription Revenue (2300), a liability
- Recognize the month's earned subscription revenue: DR Deferred Subscription Revenue (2300) / CR Subscription Revenue — DTC (4000)
- Confirm the remaining deferred balance equals subscriptions billed but not yet delivered

### 5. Accounts Payable Review
- Verify all vendor bills (importer, 3PL fulfillment, rent, marketing) are entered
- Review accrued liabilities for completeness

### 6. Depreciation
Post depreciation per fixed-asset schedules:
- Roasting & Packaging Equipment: straight-line, 7-year life, $1,071.43/month
- Automated Packaging Line: straight-line, 5-year life, $400.00/month
- Entry: DR Depreciation Expense (7000) / CR Accumulated Depreciation (1550)

### 7. Prepaid Expense Amortization
Post amortization per individual prepaid schedules:
- Business Insurance: $1,000.00/month (annual policy, $12,000 — renews annually)
- Annual Platform Prepay: $500.00/month (annual platform fee, $6,000)
- Entry: DR respective expense / CR Prepaid account

### 8. Review & Close
- Verify the trial balance (debits = credits) and that the balance sheet balances
- Compare cash flow to net income — a profitable month can still burn cash when inventory and receivables build
- Generate the income statement and balance sheet, then close the fiscal period
"""

INVENTORY_POLICY = """# Inventory & Cost of Goods Sold Policy — Driftline Coffee Roasters

## Method
Inventory is carried at cost (FIFO). Green coffee is purchased in bulk from the importer ahead of demand, roasted, and sold through DTC and wholesale channels.

## Lifecycle
1. **Purchase** — green coffee received from the importer: DR Inventory — Green Coffee (1400) / CR Cash or Accounts Payable (2000). Cash leaves the business when the coffee is bought, not when it is sold.
2. **Sale / consumption** — when finished coffee is sold, recognize cost of goods sold: DR Cost of Goods Sold (5000) / CR Inventory — Green Coffee (1400).

## Watch-out: the cash trap
Buying green coffee ahead of a wholesale launch or the holiday season builds the Inventory asset and drains cash *before* the corresponding revenue and COGS are recognized. A month can show strong gross profit while operating cash falls — the difference is sitting in inventory. During close, compare the change in inventory to the change in cash.

## Gross Margin
Target blended gross margin ≈ 55%. Investigate months where COGS as a share of revenue moves materially off trend.
"""

REVENUE_POLICY = """# Revenue Recognition Policy — Driftline Coffee Roasters

## Standard
Revenue is recognized under ASC 606 as control of the coffee transfers to the customer.

## Channels

| Channel | Account | Recognition Timing | Cash Timing |
|---|---|---|---|
| DTC Subscriptions | Subscription Revenue — DTC (4000) | Ratably as each month's coffee ships | **Prepaid** — billed a quarter ahead into Deferred Subscription Revenue (2300) |
| DTC Single Orders | Single-Order Revenue — DTC (4200) | On shipment | Immediate (card at checkout) |
| Wholesale | Wholesale Revenue (4100) | On delivery to the grocer/café | **Net 30** — builds Accounts Receivable (1100) |

## Deferred Subscription Revenue
Subscriber prepayments are a liability until the coffee ships. Each month:
- DR Deferred Subscription Revenue (2300) / CR Subscription Revenue — DTC (4000) for the month earned.
- The deferred balance is cash already collected for future shipments — a source of working capital.

## Wholesale Accounts Receivable
Wholesale sells on net-30 terms. Receivables that age past 60 days, or that concentrate in a single large account, are a working-capital and credit risk — review the AR aging by customer every close.
"""

DEPRECIATION_POLICY = """# Depreciation & Prepaid Policy — Driftline Coffee Roasters

## Depreciation
Straight-line, no salvage value unless specified. Entry: DR Depreciation Expense (7000) / CR Accumulated Depreciation (1550).

| Asset | Cost | Useful Life | Monthly |
|---|---|---|---|
| Roasting & Packaging Equipment | $90,000.00 | 84 months | $1,071.43 |
| Automated Packaging Line | $24,000.00 | 60 months | $400.00 |

## Prepaid Expense Amortization
Purchases with a term greater than one month are capitalized and amortized straight-line. Entry: DR expense / CR prepaid account.

| Item | Cost | Term | Monthly | Debit | Credit |
|---|---|---|---|---|---|
| Business Insurance | $12,000.00 | 12 months | $1,000.00 | Insurance (6500) | Prepaid Insurance (1200) |
| Annual Platform Prepay | $6,000.00 | 12 months | $500.00 | Software & Subscriptions (6400) | Prepaid Software (1210) |

During each close, review prepaid and fixed-asset accounts for new purchases that lack a schedule, and create one.
"""

# Document definitions for upload
DOCUMENTS = [
  {
    "title": "Month-End Close Procedures",
    "content": CLOSE_PROCEDURES,
    "folder": "policies",
    "tags": ["close", "procedures", "monthly"],
  },
  {
    "title": "Inventory and COGS Policy",
    "content": INVENTORY_POLICY,
    "folder": "policies",
    "tags": ["inventory", "cogs", "green-coffee"],
  },
  {
    "title": "Revenue Recognition Policy",
    "content": REVENUE_POLICY,
    "folder": "policies",
    "tags": ["revenue", "recognition", "asc606", "deferred-revenue"],
  },
  {
    "title": "Depreciation and Prepaid Policy",
    "content": DEPRECIATION_POLICY,
    "folder": "policies",
    "tags": ["depreciation", "prepaids", "amortization"],
  },
]
