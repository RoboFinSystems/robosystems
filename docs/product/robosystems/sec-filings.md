---
title: Analyze SEC filings with Claude or ChatGPT
description: "Connect an AI client to the SEC filings graph: financial statements, cross-company comparisons, disclosures and filing text for 8,000+ companies."
order: 4
section: Working with graphs
---

The SEC filings graph holds the XBRL financial filings of more than 8,000 public companies: annual and quarterly reports (10-K, 10-Q, 20-F and 40-F) filed since January 2024, updated daily. Every reported number is there with its concept, period and breakdown, and the text of each filing is searchable. Connect it to Claude, ChatGPT or another MCP client and ask about public companies in plain language.

The graph is read-only and shared: everyone who subscribes reads the same filings.

![RoboSystems SEC filing analysis in ChatGPT](https://youtu.be/Qb4b29Za8LA)

## Get access

Access is a subscription. In the app, open **Repositories**, browse to **SEC EDGAR Filings** and choose a plan. There are two plans, **Starter** and **Advanced**; Advanced has higher usage limits. Current plans and prices are on the [pricing page](https://robosystems.ai/pricing).

After you subscribe, the repository's getting-started page shows how to connect. Without a subscription, a connected client gets an access-denied error.

RoboSystems is open source, so the free route is to run it yourself and load the filings you need. See [Local Development](https://robosystems.ai/docs/technical/local-development).

To read one filing at a time, [xbrlkit](https://xbrlkit.com), an open-source RoboSystems project, is free and needs no account. The site opens a listed company's 10-K, 10-Q, 20-F or 40-F in your browser, and its [local MCP server](https://xbrlkit.com/mcp) lets Claude, Cursor or any MCP client read filings on your own computer. It holds only the filings you load, with no index across companies. For questions that span the market, such as a screen of every filer, a cohort or a long time series, use the graph.

## Connect

- **ChatGPT, no setup:** install the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) from ChatGPT's plugin directory and sign in.
- **Any MCP client:** add `https://api.robosystems.ai/v1/mcp`, and on the consent screen choose **SEC EDGAR Filings** under shared repositories.
- **Beside your own books:** add a second connection with the SEC graph's own address, so one conversation can use both.

```text
https://api.robosystems.ai/v1/graphs/sec/mcp
```

Step-by-step setup for each client is in [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md).

## What you can ask

- **Financial statements.** A company's income statement, balance sheet, cash flow or equity statement, by ticker, for its latest filing or a period you name.
- **Comparisons.** Chosen figures across periods and across companies: revenue growth over eight quarters, margins across a peer group, leverage against competitors.
- **Concepts in your words.** "Operating lease liability" or "deferred revenue" is matched to the tags companies actually report.
- **Disclosures, section by section.** A filing's notes and schedules, one at a time and whole: the rows, the breakdowns and the text, with the arithmetic checked.
- **The narrative.** Search risk factors, MD&A, business descriptions and disclosure text across filers, then read the passage in context.
- **Anything else.** The client can query the graph directly when no ready-made view answers the question.

Reading the SEC graph through an MCP client uses no RoboSystems credits. The reasoning happens in your AI client; RoboSystems returns the data.

## Getting the numbers right

These rules decide whether a number is right. The model is given them, and they're worth knowing when you check an answer:

- **Totals or breakdowns.** A company reports a consolidated total and often breakdowns by segment, product or geography. Adding the breakdowns to the total double-counts. Ask for "consolidated" when that's what you mean.
- **The same concept, different tags.** Companies tag revenue, for example, in several different ways. Comparisons across companies use the normalized concept, not one tag.
- **Flows and balances.** Revenue and cash flow cover a period; assets and liabilities are a point in time. Say "annual" or "quarterly" for flows.
- **Fiscal years differ.** A company's fiscal 2025 may end in a month other than December. Compare by fiscal period, and name the dates when it matters.
- **Numbers from facts, explanations from text.** Figures come from the reported facts; the why comes from the filing's narrative.

## Try asking

- "Show me the income statement for [ticker] for the last three fiscal years."
- "Compare gross margin for [ticker] and [ticker] over the last eight quarters."
- "Which companies mention goodwill impairment in their latest 10-K?"
- "Read the revenue recognition note from [ticker]'s latest annual report."
- "What does [ticker] say about customer concentration in its risk factors?"

## Go deeper

- [SEC data model and query rules](https://robosystems.ai/docs/technical/sec-data-model-and-query-rules): how filings are modeled in the graph, and the rules behind a right number.
