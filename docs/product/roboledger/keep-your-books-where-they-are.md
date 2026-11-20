---
title: Keep your books where they are
description: Run RoboLedger beside the ledger or ERP you already close in. It reads your books into a knowledge graph for your AI, and writes nothing back.
order: 27
section: Keep the books right
---

You don't have to move your books to use RoboLedger. You can keep closing in the ledger or ERP you use today and run RoboLedger beside it: your books are read into a knowledge graph your AI assistant works from, with reports, plans and comparisons on top, and nothing is written back. Or you can keep your books in RoboLedger itself. Either way, your AI works from a knowledge graph of them.

## Three ways to keep your books

| Your books are kept in | How they reach RoboLedger | What RoboLedger writes there |
|---|---|---|
| QuickBooks Online | Connect it and it syncs daily | Only entries you post, or nothing at all with the **Shadow** setting |
| Another ledger or an ERP | An integration sends its journal through the API | Nothing, ever |
| RoboLedger | They are already here | Not applicable |

Each company in a group makes this choice on its own, so one company's books can stay in an ERP while another's are kept in RoboLedger.

## QuickBooks stays your ledger

A connected QuickBooks company is the book of record. By default the entries RoboLedger posts are written back to it; with **Shadow** on the QuickBooks card, nothing is. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

## Another ledger or an ERP stays your ledger

There is no connector to sign in to for these. An integration, built by your developers or an integrator from the open-source template, sends the ledger's journal entries to RoboLedger through the API and registers the ledger as the keeper of a company's books. From then on:

- **The ledger's entries are the books.** They arrive already posted. RoboLedger does not post entries of its own to those books, and an assistant's draft there stays a draft.
- **Nothing is written back.** RoboLedger has no way to write to that ledger.
- **You don't send your whole history.** Record an opening balance as the starting point and send entries from that date.
- **Corrections follow the ledger.** When an entry changes or is removed in your ledger, the integration sends the change and RoboLedger's copy follows.
- **The close is a stamp.** Closing a month in RoboLedger posts nothing. It marks the month closed for reporting, fixes that month's statements, and lists anything it found, such as books that haven't been sent recently.
- **A group is read combined.** When every company in a group keeps its books elsewhere, your ledger has already consolidated them, so RoboLedger doesn't draft consolidating entries for that group.

One source keeps one company's books, so a group with five companies in one ERP registers five sources.

## What you get either way

Everything that reads the books works the same: asking questions and opening a number down to its entries, statements and reports, plans and forecasts, and comparisons with public companies. See [What your AI assistant can do with your books](what-claude-can-do.md).

What needs books kept in RoboLedger, or QuickBooks with write-back on: entries RoboLedger drafts and posts, such as schedule entries at the close and consolidating entries for a group.

## Try asking

- "Which ledger keeps each company's books, and when was each last updated?"
- "Close October and tell me what the close found."

## Go deeper

- [Build a ledger integration](https://robosystems.ai/docs/technical/build-a-ledger-integration): registering a ledger as the keeper of a company's books, sending corrections, and reporting each run.
