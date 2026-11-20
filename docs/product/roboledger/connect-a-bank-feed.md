---
title: Connect a bank feed
description: Bring a bank's posted transactions into the Inbox for a company whose books RoboLedger keeps. What a feed needs, how to connect it, and how to disconnect.
order: 8
section: Get started
---

A bank feed brings every transaction the bank has posted into RoboLedger. Each one arrives in **Ledger → Inbox** with a suggested account, and none of them is in the ledger yet: a line posts only once someone sorts it into an account and approves it. The bank never becomes the ledger, and your chart of accounts stays your own.

A feed only reads. It never moves money, and a transaction the bank still shows as pending is left out until it posts. Banks connect through Plaid; **Add Connection** lists the ones you can connect.

## Which companies can take a feed

A bank feed is for a company whose books are kept in RoboLedger. A company whose books QuickBooks or another ledger keeps can't take one: its bank activity is already in those books, and the feed would book it a second time. QuickBooks keeps only the group parent's books, so while QuickBooks is connected, a bank connects for a subsidiary. The group's consolidation entity takes no feed either, because its books hold the consolidating entries. See [Keep your books where they are](keep-your-books-where-they-are.md).

The company also needs a chart of accounts first, because each bank account is linked to an account on it. A company with none shows **Start from a template** on **Ledger → Chart of Accounts**: pick a template and choose **Create chart of accounts**.

Connecting a bank needs a member or admin role on the graph. Disconnecting one needs an admin.

## Connect a bank

1. Open **Entity → Connections** and choose **Add Connection**. The list shows what you can connect; choose **Connect** beside **Bank account (Plaid)**.
2. Under **Company**, pick the company whose books the accounts go into. A group with one company has no choice to make, so the field doesn't appear. While QuickBooks keeps the group parent's books, the parent is listed but can't be picked.
3. Set **Backfill from**: the first sync pulls every posted transaction from this date, at most two years back. It's filled in with January 1 of last year.
4. Choose **Connect a bank**, sign in to your bank in the window that opens, and choose the accounts to share. Each bank login is its own connection, so connect a second bank, or a second login at the same bank, the same way. A bank already connected to the graph is refused, because its accounts are on the existing connection.


If you close the bank's window before finishing, the connection waits on the Connections page with **Continue sign-in**, so you can pick up where you left off, or disconnect it.

## What the first sync does

The first sync starts on its own once the bank sign-in is done:

- **Each bank or card account is linked to a chart account** with the same name, or one is added to the chart for it.
- **Every posted transaction from the backfill date** arrives in the Inbox with a suggested account. Some banks send their older history over the next few syncs rather than all at once.
- **The company's fiscal calendar is set up** if it doesn't have one yet, with the books opening in the month of the earliest line. Until the company's first close you can still move that start. See [What RoboLedger needs to work well](what-it-needs.md#a-fiscal-calendar).
- **Counterparties are added** under **Agents** as they appear in the bank's lines.

The connection's card shows the bank, the companies its accounts book to, and when it last synced. After the first sync, RoboLedger syncs each connection on its own once a day. For anything more recent, press **Sync Now** on the card, or ask your assistant to sync.

A transaction dated in a month that's already closed doesn't land in that month. It's held out, and the next close waits until you decide on it: reopen the month to bring it back to the Inbox, or close over it. See [Troubleshooting](troubleshooting.md).

Sorting the lines is the Inbox's job: taking a suggestion, choosing another account, splitting a line, settling a bill it pays and approving many at once. See [Lines from a bank feed](inbox.md#lines-from-a-bank-feed).

## The Bank Accounts page

**Ledger → Bank Accounts** lists the group's bank and card accounts: each one's company, its source (the bank and the provider), its status, when it last synced, and whether it's a bank or a card account.

- **Move** changes the chart account a feed's account books to. Pick the company, then either **Create a new account in** that company's chart or **Link to an existing account**. An account another feed already books to is refused. Lines still in the Inbox move with it: across companies, their suggestions are matched again on the new chart, and any already sorted to the old chart go back to the Inbox. Posted entries stay where they were posted. Moving an account to another company is also how a feed's accounts are split across a group.
- **Add account** takes you to Connections to connect a bank.
- **Kept by hand** is the source shown for a bank or card account that no feed or synced ledger writes to. Its lines come from entries you or your assistant make.

The status column says **Auto-synced** for a working feed, **Awaiting sign-in** for one whose sign-in wasn't finished, and **Reconnect** for one whose bank login stopped working.

## When the bank asks you to sign in again

A bank can end the access a feed was given: a changed password, an expired consent, a locked login. The connection's card then shows **Reconnect** in place of **Sync Now**, and the daily sync leaves it alone. Choose **Reconnect** and sign in to the bank again. The connection keeps its history and carries on from where it stopped.

## When the bank changes a line

Banks sometimes change a transaction after it posts, or take it back.

- **A line still in the Inbox** is updated to match, or removed if the bank took it back. A line you've already sorted keeps its account, and a split that no longer adds up is refused when you approve it.
- **A line you've already approved** is never changed. It's flagged as a changed transaction, with what the bank now says, and the close waits until you settle it. See [When QuickBooks changes after a sync](changes-after-sync.md), which covers bank lines too.

## Disconnect a bank

**Disconnect** on the connection's card stops the syncing and gives up RoboLedger's access to the bank. The dialog says what happens to the books:

- Lines still waiting in the Inbox are deleted, along with the counterparties only they used.
- Entries you already posted stay in your books, without the bank's transaction details.
- Your chart of accounts stays. Its accounts stop being linked to the bank, and the history on them is unchanged.

You can connect the bank again at any time. A reconnected bank starts from a fresh sync: it links your existing chart accounts by name again, and doesn't bring back the lines already posted.

## Try asking

- "Which bank accounts are connected, and which chart account does each book to?"
- "Sync the bank feed, then tell me how many lines are waiting in the Inbox."
- "Move the savings account to the Austin subsidiary's chart."
- "How many of October's bank lines needed my choice, and how many took the suggestion?"

## Go deeper

- [Connections and bank feeds](https://robosystems.ai/docs/technical/connections-and-bank-feeds): connecting a feed, routing its accounts and working the Inbox over the API.
