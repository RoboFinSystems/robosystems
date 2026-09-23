---
title: When QuickBooks changes after a sync
description: Someone edits an old invoice or bill in QuickBooks after RoboLedger has recorded it. How RoboLedger flags the change, and the three ways to settle it.
order: 22
section: Keep the books right
---

People edit QuickBooks. An invoice from two months ago gets a new amount, a bill moves to a different account, a payment is deleted and entered again. That's normal bookkeeping, not an error. It does mean the transaction in QuickBooks no longer matches what RoboLedger recorded when it synced.

RoboLedger doesn't quietly overwrite its records when that happens, because the old figures may already sit in a closed month or in a report you've shared. It flags the transaction and asks you to decide.

## How a change shows up

After a sync, your AI assistant tells you how many transactions changed at the source. They also show up as a blocker when you ask what's in the way of a close, and on the period close view in **Ledger → Closing Book**. A close won't run over changes nobody has looked at.

For each one, your assistant can show you what RoboLedger posted, what QuickBooks says now, and the difference for each account.

A regular sync looks back 60 days. If someone edited something older than that, ask your assistant to sync from a date that covers it, or choose a full sync. Otherwise the change is missed.

## Three ways to settle it

**Restate.** RoboLedger rebuilds the entries to match QuickBooks, in the month the transaction belongs to. That month's figures change. This is the usual answer inside the current year, while the month is still open. If the month is closed, reopen it first.

**Catch up.** The original month stays as it was, and the difference posts as one entry in an open month. Use this when the earlier months are locked: you've sent those statements to a lender, an investor or the board, and they should stand. The entry stays in RoboLedger and isn't written to QuickBooks, because QuickBooks already has the edit.

**Mark it as handled.** You already booked the difference yourself. Nothing posts. Your assistant records a note saying why, and which entry covered it.

Settling a change is what makes it stick. Until you do, the flag comes back on every sync.

## Which one to choose

| Situation | Choose |
|---|---|
| The month is open, or you're happy to reopen it | Restate |
| The month is closed and its statements have gone out | Catch up |
| You already posted an entry for the difference | Mark it as handled |

If you aren't sure, ask your assistant what each choice would do to the figures before you pick. Looking at a change doesn't post anything.

## Try asking

- "Did anything change in QuickBooks after it synced? Show me each one."
- "For the changed invoice, show me what we posted, what QuickBooks says now, and the difference."
- "Restate the first two. Catch up the March one in the current month."
- "Someone edited transactions from last spring. Sync from April 1 and tell me what changed."

## Go deeper

- [QuickBooks sync and write policy](https://robosystems.ai/docs/technical/quickbooks-sync-and-write-policy): how a sync detects an edited transaction and what each way of settling it writes.
