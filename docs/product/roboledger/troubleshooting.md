---
title: Troubleshooting
description: A stale sync, an expired QuickBooks connection, a close that won't run, empty statements, credits and rate limits. What each message means and what to do.
order: 7
---

What Claude or RoboLedger tells you when something is in the way, and how to clear it. You can also paste any of these messages to Claude and ask what to do.

## "Cannot close period…" with a stale sync

QuickBooks hasn't synced since the month ended. Ask Claude to sync, or press **Sync Now** on the QuickBooks card, then try the close again. If the change you're waiting for is older than 60 days, choose a full sync.

## "QuickBooks rejected the credential refresh… Reconnect the account"

The connection to QuickBooks has expired or was revoked in Intuit. Open **Entity → Connections**, disconnect QuickBooks and choose **Disconnect**, not *Sever and go native*, which can't be undone. Then connect the same QuickBooks company again. It picks up the existing connection, so your history stays attached.

## "Sync already in progress"

A sync is still running. Wait for it to finish, then ask again.

## Statements come back empty

Your chart of accounts isn't mapped, so RoboLedger doesn't know where your accounts belong on the statements. Ask Claude which accounts are unmapped, or review them in **Ledger → Chart of Accounts**. See [What RoboLedger needs to work well](what-it-needs.md).

## "Ledger not initialized. Connect a data source first."

The graph has no books yet. Connect QuickBooks. See [Connect your books to Claude](connect-your-books.md).

## "roboledger is not provisioned for this graph"

Claude is connected to a graph without RoboLedger, such as the SEC filings graph. Each connection is one graph, chosen when you sign in. Add a connection and choose your RoboLedger graph.

## A close refuses to run

- **Out of sequence.** Months close in order. Close the earliest open month first.
- **The month isn't over.** A month can't close until it has ended.
- **Transactions changed in QuickBooks after they synced.** Ask Claude to show you each one and decide how to handle it. See [Close the month with Claude](month-end-close.md).
- **"Cannot write to closed period… Reopen the period first."** That month is already closed. Reopen it if the entry belongs there, or post the entry in an open month.

## QuickBooks rejected an entry during a close

The close reports which entries QuickBooks refused. Fix the cause in QuickBooks or the entry, then close again. Entries that were already written aren't sent a second time.

## The sync fails for a company with other currencies

RoboLedger reads companies that keep their books in US dollars. A QuickBooks company with transactions in other currencies can't sync yet. Get in touch if you need it.

## "Insufficient credits for…"

AI mapping needs more credits than your graph has left this month. Reading, reporting, forecasting and closing don't use credits. Check your balance on the **Usage** page at [robosystems.ai](https://robosystems.ai).

## "Rate limit exceeded"

Your plan allows a set number of MCP calls per minute. Ask Claude to combine its questions into fewer calls, or wait a minute.
