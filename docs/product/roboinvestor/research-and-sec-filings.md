---
title: Public companies and SEC filings
description: Read research and filings in the app, subscribe to the SEC graph, and connect it to your AI assistant.
order: 3
section: Get started
---

RoboInvestor has two ways into public companies, and they are not the same graph.

## Research in the app

**Research** (`/companies` once you are signed in, and [roboinvestor.ai/research](https://roboinvestor.ai/research) on the public site) reads a catalog of filings and research briefs. It does not use your investment graph. You can browse a company, open a filing, and read a brief where one has been published.

Reading filings and briefs doesn't touch the SEC knowledge graph. The **Ask about this filing** box on a company page does: it needs the SEC subscription below, and it uses credits.

## The SEC filings graph

The structured filings of 8,000+ public companies live in a shared RoboSystems repository. Access is a subscription. In the app, open **Repositories**, browse to **SEC EDGAR Filings** and choose a plan. Plans and prices are on the [pricing page](https://robosystems.ai/pricing).

After you subscribe, the repository's getting-started page shows how to connect. The graph is read-only and shared: everyone who subscribes reads the same filings.

Connect it beside your portfolio as a second MCP connection, with the SEC graph's address:

```text
https://api.robosystems.ai/v1/graphs/sec/mcp
```

Give the two connectors names you'll recognize. Setup for each client is in [Connect Claude, ChatGPT or any MCP client](https://robosystems.ai/docs/guides/connect-an-mcp-client).

What you can ask of the filings, and the rules that decide whether a number is right, are in [Analyze SEC filings](https://robosystems.ai/docs/guides/sec-filings).

## Console and Search

With a graph selected — your portfolio graph or the SEC filings — **Console** asks questions inside the app. It runs on RoboSystems' own AI and uses credits. **Search** finds text in the SEC filings, or, on your portfolio graph, in the documents saved there.

Claude, ChatGPT and other clients you connect yourself don't use RoboSystems credits. See [Graphs, tiers and credits](https://robosystems.ai/docs/guides/graphs-tiers-and-credits).

## Try asking

Once the SEC graph is connected:

- "Show me the income statement for [ticker] for the last three fiscal years."
- "Compare gross margin for [ticker] and [ticker] over the last eight quarters."
- "Which companies mention goodwill impairment in their latest 10-K?"
