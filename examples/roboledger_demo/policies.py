"""Accounting policy documents for Cascade Advisory Group LLC.

These get uploaded to the document store (PostgreSQL + OpenSearch) so the AI
can search and cite them during the month-end close workflow.
"""

CLOSE_PROCEDURES = """# Month-End Close Procedures — Cascade Advisory Group LLC

## Overview

Cascade Advisory Group LLC is a boutique management consulting firm. The books are maintained on a modified accrual basis. Month-end close converts cash-basis transactions to proper accrual accounting through scheduled entries.

Close the books by the 10th business day of the following month.

## Close Checklist

### 1. Bank Reconciliation
- Reconcile Operating Checking account to the bank statement
- Investigate and resolve any reconciliation discrepancies
- Verify all deposits and payments are recorded

### 2. Accounts Receivable Review
- Confirm all consulting invoices for the month have been sent
- Review AR aging for overdue balances
- Accrue unbilled revenue if services were performed but not yet invoiced

### 3. Accounts Payable Review
- Verify all vendor bills received during the month are entered
- Confirm payroll tax deposits are current
- Review accrued liabilities for completeness

### 4. Depreciation
Post depreciation entries per fixed asset schedules:
- Computer Equipment: straight-line, 3-year useful life, $133.33/month
- Office Furniture: straight-line, 5-year useful life, $25.00/month
- Total monthly depreciation: $158.33
- Entry: DR Depreciation Expense / CR Accumulated Depreciation

### 5. Prepaid Expense Amortization
Post amortization entries per individual prepaid schedules:
- Business Insurance: $100.00/month (annual policy, $1,200 — renews March)
- Software Subscriptions: $25.00/month (annual license, $300 — renews June)
- Cloud Hosting (AWS Savings Plan): $50.00/month (annual commitment, $600 — renews September)
- Total monthly prepaid amortization: $175.00
- Entry: DR respective expense account / CR Prepaid account
- Note: Each prepaid has a different renewal date — check which schedules are active for the current period

### 6. Review Trial Balance
- Verify debits equal credits
- Review each account balance for reasonableness
- Compare to prior month — investigate significant changes

### 7. Generate Financial Statements
- Generate income statement for the month
- Generate balance sheet as of month-end
- Verify balance sheet balances (Total Assets = Total L&E)
- Review account rollups to confirm CoA → GAAP mappings are correct

### 8. Period Close
After all entries are posted and reviewed, close the fiscal period.
"""

DEPRECIATION_POLICY = """# Depreciation Policy — Cascade Advisory Group LLC

## Method
Straight-line depreciation for all fixed assets. No salvage value unless specified.

## Asset Classes

| Asset Class | Useful Life | Monthly Amount |
|---|---|---|
| Computer Equipment | 3 years (36 months) | Cost / 36 |
| Office Furniture | 5 years (60 months) | Cost / 60 |

## Current Fixed Assets

| Asset | Purchase Date | Cost | Useful Life | Monthly Depreciation |
|---|---|---|---|---|
| Computer Equipment | Jan 2025 | $4,800.00 | 36 months | $133.33 |
| Office Furniture | Mar 2025 | $1,500.00 | 60 months | $25.00 |

## Accounting Entry
- Debit: Depreciation Expense (7000)
- Credit: Accumulated Depreciation (1350)

## Notes
- Assets are placed in service on the acquisition date
- Fully depreciated assets remain on the books at $0 net book value until disposed
- Combined monthly depreciation: $158.33
"""

PREPAID_POLICY = """# Prepaid Expense Policy — Cascade Advisory Group LLC

## Recognition
Purchases with a term greater than one month that provide future economic benefit are capitalized as prepaid expenses and amortized straight-line over the service period.

## Current Prepaid Items

| Item | Annual Cost | Monthly Amortization | Renewal Month | Debit Account | Credit Account |
|---|---|---|---|---|---|
| Business Insurance | $1,200.00 | $100.00 | March | Business Insurance (6400) | Prepaid Insurance (1200) |
| Software Subscriptions | $300.00 | $25.00 | June | Software Subscriptions (6100) | Prepaid Software (1210) |
| Cloud Hosting (AWS) | $600.00 | $50.00 | September | Cloud Hosting (6200) | Prepaid Cloud Hosting (1220) |

## Monthly Total
Combined prepaid amortization: $175.00/month (when all three are active)

Note: Each prepaid has a different renewal month. Check the schedule for which items are active in any given period.

## New Prepaid Identification
During each month-end close, review prepaid accounts for new debit entries that don't have a corresponding schedule. For each new purchase:
1. Determine the service term from the vendor receipt
2. Calculate monthly amortization (cost / term in months)
3. Create a new amortization schedule
4. Include the first month's amortization in the current close
"""

REVENUE_POLICY = """# Revenue Recognition Policy — Cascade Advisory Group LLC

## Standard
Revenue is recognized in accordance with ASC 606, simplified for professional services.

## Service Lines

| Service Line | Account | Recognition Timing |
|---|---|---|
| Consulting | Consulting Revenue (4000) | When services are rendered and invoiced |
| Strategy Advisory | Strategy Advisory Revenue (4100) | When advisory deliverables are completed |
| Implementation Services | Implementation Services Revenue (4200) | Percentage of completion or upon delivery |

## Invoicing
- Consulting and advisory services are invoiced monthly based on hours/deliverables
- Implementation projects may be invoiced on milestone completion
- Payment terms: Net 15

## Accrual
If services have been performed but not yet invoiced at month-end:
- Accrue: DR Accounts Receivable (1100) / CR appropriate Revenue account
- Reverse the accrual on the first day of the following month
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
    "title": "Depreciation Policy",
    "content": DEPRECIATION_POLICY,
    "folder": "policies",
    "tags": ["depreciation", "fixed-assets", "pp&e"],
  },
  {
    "title": "Prepaid Expense Policy",
    "content": PREPAID_POLICY,
    "folder": "policies",
    "tags": ["prepaids", "amortization", "subscriptions"],
  },
  {
    "title": "Revenue Recognition Policy",
    "content": REVENUE_POLICY,
    "folder": "policies",
    "tags": ["revenue", "recognition", "asc606"],
  },
]
