---
title: How RoboLedger works
description: The path a number takes, from a QuickBooks transaction to a report you can send. Events, entries, the ledger, live statements, blocks and closed months.
order: 4
section: Get started
---

RoboLedger turns what happened in your business into statements you can check and send. Knowing the path a number takes helps you ask better questions, and tells you which answer you're looking at when two of them disagree.

## From an event to a report

1. **Everything starts as an event.** An invoice issued, a bill received, a payment, a check written, a month of depreciation. Each one is recorded as an event, with its date, its amount, the customer or vendor, and where it came from.
2. **RoboLedger works out the entries.** Each kind of event has a rule for its debits and credits. A transaction from QuickBooks brings the entry QuickBooks already booked, and posts to the ledger when it syncs. An event recorded in RoboLedger, such as a schedule's depreciation or an adjustment your assistant drafts, becomes a draft entry, which usually posts when the month closes. Every entry has to balance.
3. **The ledger** is every entry, line by line, against your chart of accounts. It's what **Journal** and **Trial Balance** show. Events that still need a person wait in the [Inbox](inbox.md).
4. **Statements are built live.** When you ask for a balance sheet, RoboLedger adds up the ledger through your mapping: each account to a reporting concept, and each concept into the lines above it. Nothing is saved, so the numbers move with every sync and every posted entry. A draft counts once it posts. That's why the [mapping](map-your-chart-of-accounts.md) matters.
5. **Closing a month saves them.** The close posts the month's draft entries, locks the month, and saves its balance sheet, income statement and cash flow. From then on, that month reads the same whatever happens to the live ledger. See [Close the month with your AI assistant](month-end-close.md).
6. **A report is a snapshot** you create for a period, to download or share. It stays as it was until you regenerate it. See [Reports and sharing](reports-and-sharing.md).

The arithmetic along the way is calculated, not generated. Entries, statements, their checks and forecasts come out the same every time from the same books. RoboLedger uses AI to map your chart of accounts and to answer questions asked inside the app, not to calculate statements. Your own AI assistant reads the calculated figures and does its reasoning on top of them.

## Information blocks

Every statement, note, schedule, set of metrics and forecast in RoboLedger is an **information block**. A block is more than a table. It carries:

- **the numbers for a period,** each one a fact with its concept and dates
- **the accounts and reporting concepts** those numbers come from
- **how the lines add up:** which lines are subtotals, and what rolls into each one
- **the checks the numbers must pass,** such as assets equaling liabilities plus equity, and the result of each

A block keeps its shape from month to month and gains a set of numbers for each period. A statement gets one when a month closes, a metrics block when it's computed, a schedule for each month it runs, and a forecast for each month ahead.

That's why [Explorer](explorer.md) can open any block that holds numbers as a series over time, and switch the same block between the table, a chart, the facts, the elements, the validation and the rules. They're all views of one thing.

It's also why a report carries its own meaning. The statements go out with the concept behind each number and how the lines add up, so whoever opens a download in the free viewer at [xbrlkit.com](https://xbrlkit.com), or a shared copy in their own graph, reads the statement as you built it, without access to your ledger.

## Live, closed and reported

The same line can come from the live ledger, a closed month or a report. Live figures move with every sync; a closed month changes only if it's reopened; a report changes only if you regenerate it. When two answers disagree, this is usually why. The table is in [Ask about your books](ask-about-your-books.md#live-closed-and-reported).

## Try asking

- "Walk me through how this invoice got into the income statement."
- "Is this figure from the live ledger, a closed month or a report?"
- "Which information blocks does this graph have, and what does each one hold?"
- "Show me the checks behind last month's balance sheet."

## Go deeper

- [Information blocks](https://robosystems.ai/docs/technical/information-blocks#atomic-vs-molecular): why a block carries its own context, and what's inside one.
- [Event-driven ledger](https://robosystems.ai/docs/technical/event-driven-ledger#the-thesis-events-before-entries): how events become entries, and the three levels of the ledger.
