---
title: Compare with public companies
description: Put your margins, growth and ratios next to public companies. Add the SEC filings graph as a second connection beside your books and ask your AI to compare.
order: 15
section: Work with your books
---

Public companies report their financials to the SEC in a structured format, tagged with the same kind of reporting concepts your chart of accounts is mapped to. RoboSystems keeps those filings in a graph of their own. Connect it beside your books, and your AI assistant can put your gross margin, revenue growth or expense ratios next to companies in your industry, in one conversation.

## Set it up

1. **Subscribe to the SEC filings graph.** It's a separate RoboSystems subscription from your RoboLedger graph. Plans and how to subscribe are in the [SEC filings guide](https://robosystems.ai/docs/guides/sec-filings).
2. **Add it as a second connection.** One connection is one graph, so the filings sit beside your books as a connection of their own. Add a connector with the SEC graph's address:

```text
https://api.robosystems.ai/v1/graphs/sec/mcp
```

You can also add the address you used for your books a second time and choose **SEC EDGAR Filings** when you sign in. Give the two connectors names you'll recognize, like "Our books" and "SEC filings".

Your ledger stays in your graph. The SEC graph is shared and read-only, and nothing from your books is copied into it. Your assistant reads from both and does the comparison in the conversation.

## What you can compare

- **Margins:** gross, operating and net, against one company or a group.
- **Growth:** your revenue growth against a filer's, over the same quarters or years.
- **Cost structure:** what share of revenue goes to cost of revenue, sales and marketing, research, and general and administrative expense.
- **Balance sheet ratios:** current ratio, debt to equity, days sales outstanding.
- **What they say about it:** Your assistant can search the text of filings, so after the numbers you can ask how a company explains its margin or describes a risk.

The SEC graph covers annual and quarterly reports from more than 8,000 companies. What it holds and how to ask about it is in the [SEC filings guide](https://robosystems.ai/docs/guides/sec-filings).

## Make the comparison fair

- **Compare ratios, not dollars.** A company two hundred times your size is still a useful benchmark for gross margin. It isn't one for headcount cost.
- **Pick companies with your business model,** not just your industry. A software company that sells through resellers has different margins from one that sells direct.
- **Match the periods.** Public companies report by quarter and by fiscal year, and their fiscal years don't all end in December. Ask your assistant to line your months up to their quarters.
- **Check your mapping first.** If your cost of revenue is mapped as an operating expense, your gross margin compares against nothing. See [Map your chart of accounts](map-your-chart-of-accounts.md).
- **Use closed months** for anything you'll show someone else.

## Try asking

- "Find five small public companies with a business like ours, and tell me why you picked them."
- "Compare our gross margin for the last four quarters with theirs."
- "What share of revenue do they spend on sales and marketing? What do we spend?"
- "Our days sales outstanding is 52. Where does that put us in this group?"
- "How do those companies explain their margin changes this year?"
