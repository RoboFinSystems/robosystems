---
title: Review events in the Inbox
description: Events that still need a person wait in the Inbox. Approve or reject them, and know which ones the close will post on its own.
order: 11
section: Work with your books
---

**Ledger → Inbox** holds business events that haven't fully posted to the ledger. Open one, check its journal entry, and approve or reject it.

Most of what you'll find there comes from two places:

- **QuickBooks transactions that couldn't post on sync.** Transactions from QuickBooks are already booked there, so they normally post straight to the journal. One lands here only if it can't, for example because an account it uses isn't in RoboLedger's chart of accounts yet, or its month is closed. Fix the cause and approve it, which posts it at once, or let the next sync that brings it try again.
- **Entries drafted in RoboLedger**, by your AI assistant or by a schedule. These are drafts until the close posts them.

The close posts every draft in the month unless its event has been rejected. A **Classified** event whose draft entry is already written posts at the next close even if nobody approves it, so reject anything that shouldn't post.

Approving writes to the RoboLedger ledger only. A transaction that came from QuickBooks is never sent back. An entry drafted in RoboLedger can be written to QuickBooks when it posts, which for most people is the close. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

## Open an event

Each row shows the date, the event (an invoice issued, a bill paid, a check written and so on), the customer or vendor, where it came from, its status and the amount.

Open a row to see the event and, for a QuickBooks transaction, its journal lines. **Preview** checks whether it can post, flagging a closed month or an entry that doesn't balance, and summarizes each entry's debit and credit. **Approve** records it in the ledger. **Reject** asks you to confirm, then voids it, and a voided event never posts.

## Statuses

The Inbox opens on **Captured**. Change the status filter to see the rest.

- **Captured** — waiting for you. Nothing is in the ledger yet.
- **Classified** — its accounts are decided. When its draft entry is already in the ledger, as it is for entries your assistant or a schedule drafted, the next close posts it without an approval; reject it if it shouldn't post. One classified without an entry waits until it's approved.
- **Committed** — approved. Its draft posts at the next close.
- **Voided** — rejected, or a schedule month the ledger retired.

Under **All statuses** you will also see **Fulfilled**, where most transactions synced from QuickBooks end up; **Pending**, a schedule's entries for months still to come; and occasionally **Superseded**.

## Filter and follow a counterparty

Filter by event type, status, source (QuickBooks, manual, schedule, system) or the customer or vendor. The Inbox loads up to 200 events at a time, and the search box looks only through those, matching the description, the QuickBooks number or the customer or vendor name, so filter first. From **Agents**, open a customer or vendor and choose **View all events in inbox** to see theirs.

## Try asking

- "What is sitting in the Inbox that hasn't posted?"
- "Why is this invoice still captured? What needs fixing before it posts?"
- "Capture this month's software accrual as an event for me to review before it posts."

## Go deeper

- [The event lifecycle](https://robosystems.ai/docs/technical/event-driven-ledger#the-event-lifecycle): every event status and which moves between them are allowed.
