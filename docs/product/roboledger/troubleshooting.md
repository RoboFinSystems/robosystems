---
title: Troubleshooting
description: A stale sync, an expired QuickBooks connection, a close that won't run, empty statements, credits and rate limits. What each message means and what to do.
order: 30
section: Reference
---

What your AI assistant or RoboLedger tells you when something is in the way, and how to clear it. You can also paste any of these messages to your assistant and ask what to do.

## "Cannot close period…" with a stale sync

QuickBooks hasn't synced since the month ended. Ask your assistant to sync, or press **Sync Now** on the QuickBooks card, then try the close again. If the change you're waiting for is older than 60 days, choose a full sync.

## "QuickBooks rejected the credential refresh… Reconnect the account"

The connection to QuickBooks has expired or was revoked in Intuit. Open **Entity → Connections**, disconnect QuickBooks and choose **Disconnect**, not *Sever and go native*, which can't be undone. Then connect the same QuickBooks company again. It picks up the existing connection, so your history stays attached.

## "Sync already in progress"

A sync is still running. Wait for it to finish, then ask again.

## Statements come back empty

Your chart of accounts isn't mapped, so RoboLedger doesn't know where your accounts belong on the statements. Ask your assistant which accounts are unmapped, or review them in **Ledger → Chart of Accounts**. See [What RoboLedger needs to work well](what-it-needs.md).

## "Ledger not initialized. Connect a data source first."

The graph has no books yet. Connect QuickBooks. See [Connect your books to your AI assistant](connect-your-books.md).

## "roboledger is not provisioned for this graph"

Your assistant is connected to a graph without RoboLedger, such as the SEC filings graph. Each connection is one graph, chosen when you sign in. Add a connection and choose your RoboLedger graph.

## "Scheduled entries for this period are still pending"

A schedule has an entry due for the month and it hasn't been drafted yet. Ask your assistant to draft the month's schedule entries, or use **Draft entry** in **Ledger → Closing Book**. If the blocker names a month long before the one you're closing, a schedule was set up to start before your first open month. Ask your assistant to fix that schedule's start. See [Schedules for recurring entries](schedules.md).

## A close refuses to run

- **Out of sequence.** Months close in order. Close the earliest open month first.
- **The month isn't over.** A month can't close until it has ended.
- **Transactions changed in QuickBooks after they synced.** Ask your assistant to show you each one and decide how to handle it. See [Close the month with your AI assistant](month-end-close.md).
- **"Cannot write to closed period… Reopen the period first."** That month is already closed. Reopen it if the entry belongs there, or post the entry in an open month.

## A mapping change is refused

The account has activity in a month that's already closed, and that month's saved statements were built with the old mapping. The message names the earliest month affected. Reopen back to that month, change the mapping, and close forward again. See [Map your chart of accounts](map-your-chart-of-accounts.md).

## The Plan page has no history, or only yearly columns

The Plan page shows a column for every closed month. If your history came over from QuickBooks and you haven't closed month by month in RoboLedger, there are no monthly statements behind it yet. Ask your assistant to fill in the plan history. See [Plan and forecast with your AI assistant](plan-and-forecast.md).

## A forecast stops partway

A month failed its checks, and RoboLedger doesn't calculate later months from one that's wrong. Ask your assistant which month stopped, which check failed and which lines are involved. The months before it are still there to look at.

## A forecast line didn't follow my assumption

Each line has one owner. If you set a number on a line, a driver or growth rate for the same line steps aside for those months, and your assistant lists it as skipped with the reason. A driver also only acts in months you gave it a value for. Other months carry the line forward.

## A forecast has nothing to calculate

Every month of the scenario is now closed, so there's nothing left ahead of it. Ask your assistant to make the scenario longer and run it again.

## A one-time item repeats every month in a forecast

Lines you don't mention carry forward from the last actual month, including a one-off. Ask your assistant to set that line to zero, or to its usual amount.

## QuickBooks rejected an entry during a close

The close reports which entries QuickBooks refused. Fix the cause in QuickBooks or the entry, then close again. Entries that were already written aren't sent a second time.

## The sync fails for a company with other currencies

RoboLedger reads companies that keep their books in US dollars. A QuickBooks company with transactions in other currencies can't sync yet. Get in touch if you need it.

## "Insufficient credits for…"

AI mapping needs more credits than your graph has left this month. Reading, reporting, forecasting and closing don't use credits. Check your balance on the **Usage** page at [robosystems.ai](https://robosystems.ai).

## "Rate limit exceeded"

Your plan allows a set number of MCP calls per minute. Ask your assistant to combine its questions into fewer calls, or wait a minute.
