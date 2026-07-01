"""Accounting policy documents for Cadence Labs, Inc.

Tailored to a B2B SaaS startup: subscription revenue recognition (ASC 606) and
the deferred-revenue rollforward, expense classification (R&D / S&M / G&A), and
a close that watches operating burn and runway — the heart of the reveal.
"""

CLOSE_PROCEDURES = """# Month-End Close Procedures — Cadence Labs, Inc.

## Overview

Cadence Labs is a seed-funded B2B SaaS company. Customers sign annual contracts and pay up front; revenue is recognized ratably over the contract term. The company is pre-profitability and managed to a monthly operating burn and a cash runway. Close the books by the 5th business day of the following month.

## Close Checklist

### 1. Bank Reconciliation
- Reconcile Cash to the bank statement
- Confirm all subscription collections (annual prepayments) and vendor payments are recorded

### 2. Subscription Revenue & Deferred Revenue
- Each new or renewing annual contract is collected up front: DR Cash / CR Deferred Revenue (2300) — the full annual amount is a liability, not revenue
- Recognize the month's earned subscription revenue ratably: DR Deferred Revenue (2300) / CR Subscription Revenue (4000)
- Roll forward Deferred Revenue: opening + new bookings collected − revenue recognized = closing. Confirm it ties.
- **Deferred revenue is cash already collected for service you still owe.** It is *not* free cash — track it separately when assessing runway.

### 3. Professional Services & Accounts Receivable
- Onboarding/services are billed net-30: DR AR (1100) / CR Professional Services (4100)
- Review AR aging; most revenue is prepaid so AR should stay small

### 4. Cost of Revenue
- Recognize hosting (cloud infrastructure) and customer-support costs: DR Cost of Revenue (5000) / CR Cash or AP
- Gross margin should hold near ~78%

### 5. Operating Expense Classification
Classify spend by function — this drives the income statement and the burn analysis:
- **Research & Development (6000)** — engineering salaries and product development
- **Sales & Marketing (6100)** — sales team, commissions, and advertising
- **General & Administrative (6200/6300/6400)** — admin, finance, legal, rent, software tools

### 6. Accruals, Depreciation & Prepaids
- Accrue unpaid expenses incurred in the month
- Depreciate equipment and the office build-out; amortize annual prepaids (tooling, insurance)

### 7. Burn & Runway Review
- Compute the monthly operating burn (operating loss adjusted for non-cash items)
- Compute the cash runway = cash on hand ÷ net monthly burn
- **Net deferred revenue out of cash before judging runway**: the bank balance is inflated by prepayments you owe as future service. The annual-prepayment "float" can mask the burn while bookings grow, then disappears when growth slows.

### 8. Review & Close
- Verify the trial balance and that the balance sheet balances
- Generate the income statement and balance sheet, then close the fiscal period
"""

REVENUE_POLICY = """# Revenue Recognition Policy — Cadence Labs, Inc.

## Standard
Revenue is recognized under ASC 606 as the performance obligation is satisfied.

## Subscriptions (annual, prepaid)
- Customers sign annual contracts and pay the full year up front.
- Cash collected is recorded as **Deferred Revenue (2300)**, a current liability.
- Revenue is recognized **ratably** over the 12-month term: DR Deferred Revenue / CR Subscription Revenue (4000) each month.
- The deferred-revenue balance equals contracts billed but not yet delivered. It is a source of working-capital "float" — cash in hand that finances operations but is owed as future service.

## Professional Services
- Onboarding/implementation is billed net-30 and recognized as delivered: DR AR (1100) / CR Professional Services (4100).

## Why it matters for runway
Because subscriptions are prepaid, cash collected can exceed revenue recognized while the business grows — softening the cash burn. That float is a liability, not equity. When assessing how long the company can operate, subtract deferred revenue from cash before dividing by the burn.
"""

EXPENSE_POLICY = """# Operating Expense & Burn Policy — Cadence Labs, Inc.

## Expense classification (by function)
| Function | Account | Includes |
|---|---|---|
| Cost of Revenue | 5000 | Cloud hosting, customer support |
| Research & Development | 6000 | Engineering salaries, product development |
| Sales & Marketing | 6100 | Sales team, commissions, advertising |
| General & Administrative | 6200 / 6300 / 6400 | Admin, finance, legal, rent, software tools |

R&D is expensed as incurred (no internal-use software capitalization in this policy).

## Depreciation & Prepaids
Straight-line. Equipment over 36 months; the office build-out over 60 months (DR Depreciation Expense 7000 / CR Accumulated Depreciation 1350). Annual tooling and insurance are capitalized as prepaids and amortized over 12 months.

## Burn & runway
- **Monthly operating burn** ≈ operating loss + non-cash addbacks (depreciation/amortization), adjusted for working-capital movements.
- **Runway** = cash ÷ net monthly burn. Report it net of deferred revenue: the prepayment float inflates the cash balance with obligations owed as service.
"""

# Document definitions for upload
DOCUMENTS = [
  {
    "title": "Month-End Close Procedures",
    "content": CLOSE_PROCEDURES,
    "folder": "policies",
    "tags": ["close", "procedures", "monthly", "runway"],
  },
  {
    "title": "Revenue Recognition Policy",
    "content": REVENUE_POLICY,
    "folder": "policies",
    "tags": ["revenue", "recognition", "asc606", "deferred-revenue", "subscriptions"],
  },
  {
    "title": "Operating Expense and Burn Policy",
    "content": EXPENSE_POLICY,
    "folder": "policies",
    "tags": ["expenses", "r&d", "s&m", "burn", "runway"],
  },
]
