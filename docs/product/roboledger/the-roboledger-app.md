---
title: Find your way around the app
description: A tour of the RoboLedger app, page by page. Where to see your ledger, statements, the close, reports, plans and the concepts your accounts map to.
order: 5
section: Get started
---

Most of the work in RoboLedger happens in a conversation with your AI assistant: Claude, ChatGPT or any MCP client. The app at [roboledger.ai](https://roboledger.ai) is where you connect your books, see what your assistant is working from, and review what it did. Everything it reads and writes is visible here.

The screens below show Cadence Labs, a made-up company used for demos.

![The RoboLedger home page, with transaction and account counts, recent transactions and the latest report](images/home.png)

The menu at the top right picks the company you're looking at, if you have more than one. **Home** shows recent transactions, your latest reports and shortcuts.

## Entity

- **Entity Info** holds your company's details.
- **All Entities** lists every company across your graphs.
- **Connections** is where you connect QuickBooks, press **Sync Now**, choose sync options, and disconnect. See [Connect your books to your AI assistant](connect-your-books.md).

## Agents

The people and companies you do business with: customers, vendors and employees. They come over from QuickBooks, and each one opens to its details and activity.

## Ledger

**Chart of Accounts** lists your accounts. **Show mappings** adds the reporting concept each account is mapped to, and the bar at the top shows how much of the chart is covered. See [Map your chart of accounts](map-your-chart-of-accounts.md).

![The chart of accounts with each account's reporting concept and a coverage bar reading 20 of 20](images/chart-of-accounts.png)

**Inbox** holds events that have been captured and haven't posted to the ledger yet, so you can review and approve them first. See [Review events in the Inbox](inbox.md).

**Journal** is every journal entry with its lines, in posting order, and a second tab for transactions. You can create a manual entry here.

**Trial Balance** shows each account's debits and credits, and whether they agree.

**Statements** shows the balance sheet, income statement, cash flow and statement of equity straight from the current ledger, for any period. Nothing is saved, and no close is needed.

![Live statements showing a balance sheet with current and prior columns](images/statements.png)

**Closing Book** is where the month-end close lives. The period close view shows the last closed month, the month you're working towards, what's blocking it, and each schedule's entry for the month. Beneath it are your account mapping, your schedules and the trial balance. See [Close the month with your AI assistant](month-end-close.md) and [Schedules for recurring entries](schedules.md).

![The Closing Book period close view, showing the last closed month, a blocker, and four schedule entries pending for August](images/closing-book.png)

## Reports

- **View Reports** lists the reports you've created. Open one to read its statements and notes, download it, or share it.
- **Create Report** builds a report for a period: this month or last, this quarter or last, monthly year to date, a full year by month, year over year, or dates you choose.
- **Publish Lists** are the sets of graphs you send reports to.
- **Blocked Senders** are graphs you don't accept reports from.

See [Reports and sharing](reports-and-sharing.md).

## Explorer

Explorer opens any statement, note, schedule, set of ratios or scenario as a series over time. Switch between the rendered table, a chart, the facts behind it, the accounts and concepts it's built from, the checks it passed and the rules behind them. Export its table as CSV or JSON. See [Explore statements and metrics over time](explorer.md).

![Explorer showing key financial metrics as a table, one column a month](images/explorer.png)

## Plan

Your statements and a scenario's assumptions in one monthly grid, with actual months on the left and forecast months on the right. See [Plan and forecast with your AI assistant](plan-and-forecast.md).

## Library

The reporting concepts your accounts map to, with their definitions and how they roll up into each statement. Look here when you're deciding where an account belongs. See [The reporting library](the-library.md).

## Console

Ask questions about your books inside the app. The Console runs on RoboLedger's own AI, so questions you ask it use credits from your plan's monthly allowance. Claude, ChatGPT and other clients you connect yourself don't use credits.

## Search

Finds text across the documents in your graph, such as accounting policies and close procedures.

## Your account

Your password, passkeys, API keys, usage and billing are in your RoboSystems account at [robosystems.ai](https://robosystems.ai). The user menu at the top right takes you there.
