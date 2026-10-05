---
title: What your AI assistant can do with your books
description: Analyze your numbers, build reports, plan scenarios, compare with public companies and close the month, all in a conversation with RoboLedger.
order: 2
section: Get started
---

Once your books are connected, your AI assistant works from the ledger itself, not from an export or a screenshot: every account and transaction, as of your last sync. Most people start with questions, then reports and plans, and close the month last, once they trust what they're seeing.

## Analyze

Ask about your books in plain language. It reads your current statements, trial balance, journal entries, customers and vendors, and any documents you've added, and it can line up periods side by side.

- "Why did gross margin drop in August?"
- "Which vendors grew fastest this year, and what did we spend with each?"
- "Compare operating expenses for the last six months, by account."

Reading your books uses no credits. See [Ask about your books](ask-about-your-books.md).

## Report

Your assistant can build a report from your ledger for a period: the balance sheet, income statement, cash flow and statement of equity, published in RoboLedger. From there you can download it as a holon (the default), a Tavi file or an XBRL 2.1 package, or share it with another RoboLedger or RoboInvestor graph. See [Reports and sharing](reports-and-sharing.md).

- "Create a report for September."
- "What changed between the August and September reports?"

## Plan

Build forecast scenarios from assumptions, like hiring, pricing or a new contract, and your assistant projects them forward month by month. The **Plan** page shows actuals and forecast in one grid, with a link you can share for each scenario. A forecast is calculated, not generated, so it uses no credits, and running it again replaces the old values.

A plan starts from closed months. If you haven't closed any in RoboLedger yet, your assistant can fill in the recent history first, over the general address or your graph's own address rather than RoboLedger's own ([Which address to use](connect-your-books.md#which-address-to-use)). See [Plan and forecast with your AI assistant](plan-and-forecast.md).

- "Build a scenario where we hire two engineers in October."
- "Re-run the forecasts now that September is closed."

## Compare with public companies

Add the SEC filings graph as a second connection beside your books, with its own address, `https://api.robosystems.ai/v1/graphs/sec/mcp`. You can also add RoboSystems SEC from Claude's connector directory, or the RoboSystems plugin in ChatGPT. With both connected, your assistant can put your margins, growth and expense ratios next to public companies in your industry. The SEC graph is a separate RoboSystems subscription. See [Compare with public companies](compare-with-public-companies.md).

- "Compare our gross margin with three small public companies in our industry."
- "How does our revenue growth stack up against those filers over the last eight quarters?"

## Keep the books right

The answers are only as good as the books. Your assistant can [map your chart of accounts](map-your-chart-of-accounts.md) to reporting concepts, set up [schedules](schedules.md) for depreciation, amortization and prepaid expenses, and walk you through [transactions that changed in QuickBooks](changes-after-sync.md) after they synced.

- "Which accounts aren't mapped yet? Suggest where each one belongs."
- "Which of last year's closing entries should be schedules?"

## Reconcile

Your assistant can tie your balances to something outside the ledger: QuickBooks, a bank or card statement balance you give it, or a schedule's own figures. It shows which accounts don't tie and by how much, and records your sign-off once they do. You decide which reconciliations hold the close. See [Reconcile your accounts](reconcile-your-accounts.md).

- "Reconcile September and tell me what doesn't tie."
- "The bank statement for checking ended September at $48,210.55. Does the ledger agree?"

## Close the month

When you're ready, your assistant runs the month-end close. It clears what's blocking, drafts the adjusting entries, and shows you what will be written to QuickBooks before anything posts. See [Close the month with your AI assistant](month-end-close.md).
