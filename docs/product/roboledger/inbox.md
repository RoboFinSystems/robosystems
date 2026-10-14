---
title: Review events in the Inbox
description: Events that still need a person wait in the Inbox. Approve or reject them, sort bank lines into accounts, and know which ones the close will post on its own.
order: 11
section: Work with your books
---

**Ledger → Inbox** holds business events that haven't fully posted to the ledger. Open one, check its journal entry, and approve or reject it.

Most of what you'll find there comes from three places:

- **QuickBooks transactions that couldn't post on sync.** Transactions from QuickBooks are already booked there, so they normally post straight to the journal. One lands here only if it can't, for example because an account it uses isn't in RoboLedger's chart of accounts yet, or its month is closed. Fix the cause and approve it, which posts it at once, or let the next sync that brings it try again.
- **Lines from a bank feed.** Every transaction the bank has posted arrives here with a suggested account and nothing in the ledger. Nothing posts until someone sorts it. See [Lines from a bank feed](#lines-from-a-bank-feed).
- **Entries drafted in RoboLedger**, by your AI assistant or by a schedule. These are drafts until the close posts them.

An event that's **Captured** or **Classified** with no entry yet holds the close of the month it's dated in, because once that month closes it could never post there. Approve or reject each one before you close.

The close posts every draft in the month unless its event has been rejected. A **Classified** event whose draft entry is already written posts at the next close even if nobody approves it, so reject anything that shouldn't post.

Approving writes to the RoboLedger ledger only. A transaction that came from QuickBooks is never sent back. An entry drafted in RoboLedger can be written to QuickBooks when it posts, which for most people is the close. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

## Open an event

Each row shows the date, the event (an invoice issued, a bill paid, a check written and so on), the customer or vendor, where it came from, its status and the amount.

Open a row to see the event and, for a QuickBooks transaction, its journal lines. **Preview** checks whether it can post, flagging a closed month or an entry that doesn't balance, and summarizes each entry's debit and credit. **Approve** records it in the ledger. **Reject** asks you to confirm, then voids it, and a voided event never posts.

## Lines from a bank feed

A bank feed brings in every transaction the bank has posted, each as a **Captured** event with nothing in the ledger. Most arrive with a suggested account, and the badge on the row says where the suggestion came from:

- **Learned** — the counterparty's own default account, learned from the lines you approved before. The event says how often it held, such as "classified to Payroll on 7 of 8 past lines". This is the one to trust.
- **Bank category** — only the bank's category for the transaction, matched to your chart. Check it.

A line with no badge carries no suggestion and needs a choice. A transfer between two of your own accounts arrives as one event with both sides known, and posts as it is.

Open a line. **Post to account** holds the suggestion; change it if it's wrong. **Remember for** the counterparty makes the account you chose its default, so its next lines are suggested there. It's ticked already when the counterparty has no default yet, and left to you when you're overriding one. **Classify** records the account without posting. **Approve** posts the line as a draft entry against the bank account, and the close posts the draft. A line your assistant has split across accounts posts the split as recorded.

A line that is a financing or investing flow, such as a loan drawn, an owner's contribution or distribution, or equipment bought, also takes a **Cash flow row**: the row of the cash flow statement it belongs on, in place of the one its account defaults to. You can set it on a line that has already posted, and the statement re-reads it in place.

Every approval teaches. Under **Agents**, a customer or vendor shows its **Default account**, how often it held, and **Always ask** for one whose lines go to different accounts, which keeps the default but stops suggesting it.

### Approve many at once

The **Suggestion** filter narrows the list to lines by where their suggestion came from, or to lines with none. Tick the lines to take, press **Select learned** for every line whose suggestion is learned, or tick the box in the header for every line that has a suggestion. **Approve** then takes each line's suggestion and posts it, oldest first, through the same checks as approving one at a time, so a line that can't post is skipped rather than forced. The summary says how many posted and names each line skipped with the reason: already handled, no suggestion to take, changed by the feed since it was captured, in a closed month, or busy. A line with no suggestion, or whose suggestion names an account that isn't on your chart, is left for you to open.

Your assistant can do the same from the Inbox's lines: ask it to take the learned suggestions on the month's bank lines and tell you what it skipped. From the Console, `/do` can sort lines into accounts for you, and a line it sorts posts only when someone approves it.

## Attach the document behind an event

An event can keep the document it rests on: the bill, the receipt or the invoice. Open the event and, under **Document**, attach a PDF or a photo (PNG or JPEG) up to 25 MB. Once it's attached, **Download** gets it back and **Detach** unlinks it; the document itself stays stored.

You can attach one to any event that hasn't been rejected or replaced, so a bill that's already posted or paid can still get its receipt. A stored document can't be deleted while an event still rests on it, and it isn't searchable.

## Statuses

The Inbox opens on **Captured**. Change the status filter to see the rest.

- **Captured** — waiting for you. Nothing is in the ledger yet.
- **Classified** — its accounts are decided. When its draft entry is already in the ledger, as it is for entries your assistant or a schedule drafted, the next close posts it without an approval; reject it if it shouldn't post. One classified without an entry waits until it's approved, and holds the close of its month until you approve or reject it.
- **Committed** — approved. Its draft posts at the next close.
- **Voided** — rejected, or a schedule month the ledger retired.

Under **All statuses** you will also see **Fulfilled**, where most transactions synced from QuickBooks end up; **Pending**, a schedule's entries for months still to come; and occasionally **Superseded**.

## Filter and follow a counterparty

Filter by event type, status, source (QuickBooks, Plaid, Mercury, manual, schedule, system), suggestion or the customer or vendor. The Inbox loads up to 200 events at a time, and the search box looks only through those, matching the description, the QuickBooks number or the customer or vendor name, so filter first. From **Agents**, open a customer or vendor and choose **View all events in inbox** to see theirs.

## Try asking

- "What is sitting in the Inbox that hasn't posted?"
- "Why is this invoice still captured? What needs fixing before it posts?"
- "Take the learned suggestions on October's bank lines and post them. Tell me what you skipped and why."
- "Capture this month's software accrual as an event for me to review before it posts."

## Go deeper

- [The event lifecycle](https://robosystems.ai/docs/technical/event-driven-ledger#the-event-lifecycle): every event status and which moves between them are allowed.
- [Capture, classify, commit](https://robosystems.ai/docs/technical/event-driven-ledger#capture-classify-commit): how a captured line records its account and then posts, and what each step refuses.
