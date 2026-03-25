---
title: Bank Reconciliation Procedures
tags: reconciliation, bank, cash, controls
folder: accounting-policies
---

# Bank Reconciliation Procedures

This document describes the bank reconciliation process for Acme Consulting LLC. Bank reconciliation is performed monthly as part of the month-end close (Day 3 of the close checklist).

## Purpose

Bank reconciliation ensures the Cash account (Account 1000) in the general ledger agrees with the bank statement balance. Discrepancies are identified, investigated, and resolved before the period is closed.

## Reconciliation Format

### Book-to-Bank Method

Start with the general ledger Cash (1000) balance and reconcile to the bank statement balance:

```
General Ledger Cash Balance (Account 1000)    $XX,XXX.XX
  Add: Deposits in transit                    + $X,XXX.XX
  Less: Outstanding checks                   - $X,XXX.XX
  Add/Less: Bank errors                      ± $X,XXX.XX
                                              ──────────
Adjusted Book Balance                         $XX,XXX.XX

Bank Statement Balance                        $XX,XXX.XX
  Add/Less: Book errors                       ± $X,XXX.XX
  Add: Bank interest earned                   + $XX.XX
  Less: Bank fees not yet recorded            - $XX.XX
                                              ──────────
Adjusted Bank Balance                         $XX,XXX.XX

Difference (must equal zero)                  $0.00
```

## Step-by-Step Process

### Step 1 — Gather Documents

- Download bank statement for the month (PDF and CSV)
- Export Cash (Account 1000) ledger detail for the month
- Retrieve prior month's reconciliation for outstanding items

### Step 2 — Match Transactions

- Match each bank transaction to its corresponding ledger entry
- Use amount, date, and description to identify matches
- Mark matched items as cleared in the ledger
- Flag any transactions appearing in only one source

### Step 3 — Identify Reconciling Items

#### Deposits in Transit

Deposits recorded in the ledger but not yet appearing on the bank statement. Common causes:

- Deposits made on the last business day (bank processes next day)
- Wire transfers initiated but not yet credited
- Mobile check deposits pending clearance

**Action:** Verify these deposits clear in the first few days of the following month. If a deposit remains in transit for more than 5 business days, investigate immediately.

#### Outstanding Checks

Checks issued and recorded in the ledger but not yet cashed by the payee. Review criteria:

| Age | Action |
|---|---|
| 0–30 days | Normal — no action needed |
| 31–60 days | Monitor — contact payee if significant amount |
| 61–90 days | Follow up — confirm payee received the check |
| 90+ days | Investigate — consider stop payment and reissue or void |

**Stale checks (90+ days):** If the payee cannot be reached and the check is confirmed uncashed, void the check. Debit Cash (1000), Credit the original expense account (reversal). Document the reason for voiding.

#### Bank Fees and Interest

- Bank fees: Debit Professional Fees (Account 5600), Credit Cash (Account 1000)
- Interest earned: Debit Cash (Account 1000), Credit a future Interest Income account (currently not in CoA — record as reduction to Professional Fees (5600) until volume justifies a new account)

### Step 4 — Record Adjustments

Post journal entries for any items appearing on the bank statement but not in the ledger:

- Bank service charges
- Interest income
- Returned checks (NSF)
- Wire transfer fees
- Automatic debits not yet recorded

### Step 5 — Verify and Document

- Confirm adjusted book balance equals adjusted bank balance (difference = $0.00)
- Sign and date the reconciliation
- Attach bank statement, ledger detail, and list of outstanding items
- File in the monthly close binder

## Controls

### Segregation of Duties

- The person preparing the reconciliation should not have authority to sign checks or initiate wire transfers
- The controller reviews and approves all completed reconciliations
- Any adjustments exceeding $500 require controller approval before posting

### Timeliness

- Reconciliation must be completed by Day 3 of the close
- If the bank statement is not available by Day 2, notify the controller and document the delay
- Late reconciliations (after Day 5) require a memo explaining the cause

### Red Flags

Investigate immediately if any of the following occur:

- Unexplained debits on the bank statement
- Checks clearing for amounts different than issued
- Deposits appearing on the bank statement with no ledger entry
- Bank balance significantly lower than expected
- Round-dollar debits with no corresponding invoice or approval

## Reconciliation History

Maintain a rolling 12-month history of reconciliations showing:

- Month-end bank balance
- Month-end book balance
- Number of outstanding checks
- Total outstanding check amount
- Number of deposits in transit
- Any unresolved reconciling items carried forward
