---
title: Nothing writes to QuickBooks until you post
description: Reading, reporting, planning and syncing never write to QuickBooks. Entries RoboLedger posts do. What gets written back, and when.
order: 24
section: Keep the books right
---

Connecting QuickBooks, syncing, asking questions, building reports and running forecasts never write anything to QuickBooks. Syncing only reads. You can use everything in [What your AI assistant can do with your books](what-claude-can-do.md) short of the close without changing a single QuickBooks record.

## What does write to QuickBooks

Only entries RoboLedger posts:

1. **Closing a month.** The entries RoboLedger drafted for the month are written to QuickBooks when the month closes. That means schedule entries like depreciation, and one-off adjustments you asked your AI assistant to draft. Transactions that came from QuickBooks are never sent back.
2. **Posting a single entry.** Your assistant can post one entry you've agreed on, outside a close, and it's written to QuickBooks when it posts.

Before a close, the list of drafts shows which entries will be written to QuickBooks and which stay in RoboLedger. An entry can also be marked to stay in RoboLedger only.

One kind of entry always stays in RoboLedger: a catch-up entry for a transaction that was edited in QuickBooks after it synced. QuickBooks already has that edit, so writing the entry back would apply it twice. See [When QuickBooks changes after a sync](changes-after-sync.md).

If QuickBooks rejects an entry during a close, the close reports it. Entries that were already written aren't sent again when you retry.

## QuickBooks stays your book of record

A connected QuickBooks company is the book of record. RoboLedger mirrors it on every sync, and the entries RoboLedger posts are written back to it, so the two ledgers never drift apart. That's the **QuickBooks authoritative, write back on close** setting on the QuickBooks card under **Entity → Connections**, and it's the default.

## Disconnecting QuickBooks

Open the QuickBooks card, disconnect, and choose **Disconnect**. It revokes access and stops syncing, and your books stay in the graph. Connect the same QuickBooks company again later and it picks up where it left off.

## Try asking

- "Which of this month's draft entries will be written to QuickBooks at the close, and which stay in RoboLedger?"
- "Post this accrual now instead of waiting for the close, and tell me whether it goes to QuickBooks."

## Go deeper

- [QuickBooks sync and write policy](https://robosystems.ai/docs/technical/quickbooks-sync-and-write-policy): the write policies a connection can carry, and exactly what reaches QuickBooks under each.
