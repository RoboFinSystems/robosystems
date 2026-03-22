---
title: Chart of Accounts Mapping Guide
tags: chart-of-accounts, classification, mapping, taxonomy
folder: accounting-policies
---

# Chart of Accounts Mapping Guide

This guide defines how Acme Consulting LLC classifies transactions to the correct general ledger accounts. It serves as the authoritative reference for consistent account coding across all bookkeepers, accountants, and automated classification systems.

## Account Numbering Convention

| Range | Classification | Normal Balance | Financial Statement |
|---|---|---|---|
| 1000–1999 | Assets | Debit | Balance Sheet |
| 2000–2999 | Liabilities | Credit | Balance Sheet |
| 3000–3999 | Equity | Credit | Balance Sheet |
| 4000–4999 | Revenue | Credit | Income Statement |
| 5000–5999 | Expenses | Debit | Income Statement |

## Asset Accounts (1000–1999)

### Cash — 1000

All liquid funds including operating bank accounts and petty cash.

**Post here:**
- Bank deposits and withdrawals
- Wire transfers (incoming and outgoing)
- ACH payments and receipts
- Petty cash transactions

**Do not post here:**
- Restricted cash or security deposits (future account if needed)
- Client trust funds (never commingle)

### Accounts Receivable — 1100

Amounts owed by clients for services already delivered.

**Post here:**
- Invoices issued for consulting services (paired with Revenue 4000 or 4100)
- Unbilled revenue accruals at month-end (services performed, invoice not yet sent)

**Do not post here:**
- Employee advances or loans (not a client receivable)
- Deposits paid to vendors (use Prepaid Expenses 1200)

### Prepaid Expenses — 1200

Payments made in advance for future services or coverage.

**Post here:**
- Annual insurance premiums paid upfront
- Prepaid software licenses
- Prepaid rent (security deposits or last-month rent)

**Amortize to:** Insurance (5300), Professional Fees (5600), or Rent (5100) as the coverage period expires.

### Equipment — 1500

Tangible assets with useful life exceeding one year and cost of $2,500 or more.

**Post here:**
- Computers and servers ($2,500+)
- Office furniture ($2,500+)
- Leasehold improvements

**Do not post here:**
- Items under $2,500 (expense to Office Supplies 5500)
- Software subscriptions (expense to Professional Fees 5600)

### Accumulated Depreciation — 1600

Contra-asset tracking cumulative depreciation of Equipment (1500).

**Post here:**
- Monthly depreciation entries (credit this account, debit the corresponding expense)

## Liability Accounts (2000–2999)

### Accounts Payable — 2000

Amounts owed to vendors for goods and services received.

**Post here:**
- Vendor invoices for services received (rent, utilities, supplies, professional services)
- Approved expense reports awaiting reimbursement (if using AP workflow)

### Accrued Expenses — 2100

Expenses incurred but not yet invoiced by the vendor.

**Post here:**
- Payroll tax accruals
- Utility estimates when invoice is not yet received
- Bonus accruals
- Pending employee reimbursements

**Key difference from AP:** Accrued Expenses have no vendor invoice yet. Once the invoice arrives, reclassify to Accounts Payable (2000).

### Deferred Revenue — 2200

Cash received from clients before services are delivered.

**Post here:**
- Retainer payments received in advance
- Prepaid training fees
- Milestone payments on fixed-fee engagements before work begins

**Release to Revenue:** As services are delivered, debit Deferred Revenue (2200) and credit the appropriate Revenue account (4000 or 4100).

### Loan Payable — 2500

Outstanding principal on business loans.

**Post here:**
- New loan proceeds (credit)
- Principal portion of loan payments (debit)

**Do not post here:**
- Interest portion of loan payments (expense to a future Interest Expense account)

## Equity Accounts (3000–3999)

### Common Stock — 3000

Owner's initial and additional capital contributions.

**Rarely changes.** Only post here for new equity investments or capital contributions.

### Retained Earnings — 3100

Cumulative net income less distributions.

**Do not post directly.** This account is updated only during the annual closing process when net income is transferred from the income statement.

## Revenue Accounts (4000–4999)

### Consulting Revenue — 4000

All consulting engagement income.

**Post here:**
- Time-and-materials billing
- Fixed-fee engagement revenue (recognized per completion percentage)
- Retainer revenue released from Deferred Revenue (2200)

### Training Revenue — 4100

All training and education delivery income.

**Post here:**
- In-person and virtual training session fees
- Self-paced training package sales
- Cancellation fees (non-refundable portion)

## Expense Accounts (5000–5999)

### Salaries — 5000

All employee compensation.

**Post here:** Base salaries, bonuses, commissions, contractor payments (W-2 and 1099).

### Rent — 5100

Office lease payments.

**Post here:** Monthly rent, common area maintenance (CAM) charges.

### Utilities — 5200

**Post here:** Electricity, internet, phone, water.

### Insurance — 5300

**Post here:** General liability, professional liability (E&O), property insurance, workers' comp.

### Marketing — 5400

**Post here:** Advertising, events, client meals, sponsorships, content creation, website costs.

### Office Supplies — 5500

**Post here:** Consumables, equipment under $2,500, postage, printing.

### Professional Fees — 5600

**Post here:** Legal fees, audit fees, tax preparation, outside consulting, software subscriptions.

## Common Classification Mistakes

| Transaction | Wrong Account | Correct Account | Why |
|---|---|---|---|
| Annual software license ($6K) | Equipment (1500) | Prepaid (1200) → Professional Fees (5600) | Software subscriptions are not tangible assets |
| Client dinner | Office Supplies (5500) | Marketing (5400) | Client-facing meals are marketing |
| Laptop ($1,800) | Equipment (1500) | Office Supplies (5500) | Under $2,500 capitalization threshold |
| Security deposit | Rent (5100) | Prepaid Expenses (1200) | Deposit is recoverable, not an expense |
| Bonus accrual | Cash (1000) | Accrued Expenses (2100) | Cash hasn't been paid yet |
