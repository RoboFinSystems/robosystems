---
title: Reconcile your accounts
description: Tie the ledger each month to your QuickBooks books, your schedules or a statement, sign the result off, and have the close wait until it ties.
order: 23
section: Keep the books right
---

A reconciliation checks the ledger against something outside it at a month end and shows where the two disagree. RoboLedger keeps each one as a record: what was compared, when, what it found, and who signed it off. The close can wait until it ties.

## What a reconciliation compares

- **Against the synced books.** Every account in the ledger against QuickBooks' own trial balance at the month end, in one check for the whole ledger. Balance-sheet accounts are compared to the month end, income-statement accounts from the start of the fiscal year. A difference here is often activity that hasn't synced yet, so sync before you dig.
- **Against its schedules.** Each account a schedule carries a balance on, such as a prepaid, accumulated depreciation or the asset's cost, against what its schedules say it should hold. The ledger side includes the month's drafts and any schedule entries not yet drafted, so it shows the balance the close will leave. See [Schedules for recurring entries](schedules.md).
- **Against its statement.** A bank account, a card or a loan against the ending balance on its statement, once you record one.

## Run them

Open **Ledger → Closing Book → Reconciliations**, pick the month, and choose **Run reconciliations**. That runs the check against the synced books and the check for each scheduled account, and records the result of each. Open a row to see what was compared and which accounts differ.

Each row shows its status:

- **Not run** — nothing has been compared for this month.
- **Does not tie** — the two sides differ by more than the materiality.
- **Out of date** — the books changed after it was compared. Run reconciliations again.
- **Reconciled** — the two sides agree within the materiality.
- **Reviewed** — reconciled and signed off.

Materiality is zero unless you set one, so the two sides have to agree to the cent. Ask your assistant to set a materiality on a reconciliation, and a difference up to that amount still counts as tied. It applies from the next comparison.

## Record a statement balance

Choose **Record statement**, pick the account, and enter the **Statement ending date** and the **Ending balance** as the statement shows it. An amount owed on a loan or a card is a positive number. Only balance-sheet accounts can be reconciled to a statement.

A statement that ends mid-month is compared with the ledger at its own date. Recording the same account and date again replaces the earlier balance. Your assistant can also keep the statement itself with the balance as evidence.

## When the close waits on one

A reconciliation with **Holds the close** ticked must be **Reconciled** before its month can close, or **Reviewed** if it needs a review. One that hasn't been run for the month holds the close too. Untick the box to release it.

The first time you run reconciliations, the check against the synced books starts holding the close. A scheduled account's check holds it only if it ties on that first run; one that doesn't is reported without holding the close, so you can look into it first. A statement check holds the close only once you tick the box, which makes it a statement you owe every month.

Once you've run reconciliations, each QuickBooks sync runs the next month's again, so the close never waits on a comparison nobody remembered to run. A sync refreshes the checks you have and doesn't add new ones.

A month's comparison only goes out of date while the month is open. Once it closes, the comparison stands as the record the close was made on.

## Sign off

On a reconciled row, choose **Sign off**. Only someone added to the graph itself as a member or admin can sign off. Access that comes only from an organization role isn't enough.

A sign-off is for the figures as they stood. If a balance at that month end changes, the sign-off lapses, and the reconciliation needs running and signing again. A run that finds the same balances keeps it.

Two settings, which your assistant can change for you, make the review part of the close:

- *Review required.* The close waits for a sign-off, not only for the two sides to tie.
- *Separate reviewer.* The person who signs off must be someone other than the one who ran the comparison or recorded the statement balance. It needs at least two members of the graph who can make changes.

If a reconciliation can't be cleared before the close, the close can still run without it: the app offers **Close without reconciling** when it's the only thing in the way, and the override is kept in the close's audit note. See [Close the month with your AI assistant](month-end-close.md).

## Try asking

- "Run August's reconciliations and tell me what doesn't tie."
- "Compare the ledger with QuickBooks at August 31 without recording anything."
- "The checking account statement ends August 31 at 18,250.75. Record it and reconcile."
- "Set a $25 materiality on the synced-books reconciliation."
- "Which reconciliations are holding the September close, and why?"
- "Require a separate reviewer on the bank reconciliations."

## Go deeper

- [Period close](https://robosystems.ai/docs/technical/period-close#reconciliations): how reconciliations are recorded, what holds the close, and the operations behind them.
