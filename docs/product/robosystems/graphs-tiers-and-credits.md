---
title: Graphs, tiers and credits
description: What a RoboSystems graph is, how tiers differ, what uses credits and what doesn't, and what happens when a graph runs out.
order: 4
section: Working with graphs
---

Everything in RoboSystems lives in a graph, and every graph runs on its own tier with its own monthly credits. This page explains what you're choosing when you create one, and what you're spending when you use it.

## Graphs

A **graph** is one company's own database: its books, reports, plans, documents and memory. Each graph runs on a dedicated instance, so no other customer's data or workload shares it.

Graphs belong to an **organization**. Organization owners and admins create them with **Create Graph** in the app, and creating one starts a subscription on the organization's payment method. Members work on the graphs they've been given access to. An organization has a limit on how many graphs it can create; if you reach it, the app lets you ask for more.

**Create Graph** first asks for the kind of graph. An **entity graph** is built around a company, and you choose which products it runs, either, both or neither:

- **RoboLedger** for accounting: QuickBooks sync, statements, forecasts, the month-end close. See the [RoboLedger docs](https://roboledger.ai/docs).
- **RoboInvestor** for private-company portfolios, securities and positions, and for reports a RoboLedger company shares with you. See the [RoboInvestor docs](https://roboinvestor.ai/docs). RoboInvestor is in beta.
- **Neither**, for a company graph without either product yet.

A **generic graph** is for data you model and load yourself, from files you upload. It is created on the Standard tier.

**Shared repositories** are graphs you subscribe to rather than create. The SEC filings are one: read-only, the same for every subscriber. See [Analyze SEC filings](sec-filings.md).

## Tiers

Every graph you create has a tier. The tiers are named for what you get, a dedicated instance of a given size:

| Tier | For |
|---|---|
| **Standard** | A single company's books and reporting |
| **Large** | Heavier use, more history, more experiments alongside |
| **XLarge** | The largest graphs and workloads |

Larger tiers come with more memory, more storage, more subgraphs, longer backup retention and more monthly credits. The current figures and prices are on the [pricing page](https://robosystems.ai/pricing). The same offering is published for programs at `https://api.robosystems.ai/v1/offering`, which needs no sign-in.

## Subgraphs

A **subgraph** is a separate workspace inside a graph, for trying a different model or a what-if without touching the main graph. It has its own data, runs on its parent's instance, shares its parent's permissions and credits, and connects to an AI client with its own address. Each tier allows a set number of subgraphs.

## Credits

Credits pay for AI that runs **inside** RoboSystems. Most work doesn't use any.

**Uses credits:**

- AI analysis run on a graph from within RoboSystems.
- AI mapping of a chart of accounts to reporting concepts, which RoboLedger runs on a company's first QuickBooks sync.

**Uses no credits:**

- Everything an AI client does through MCP: every tool call, on your graphs and on the SEC filings. The model runs in Claude or ChatGPT; RoboSystems answers with data.
- Queries, reports, statements, searches and documents.
- Syncs, uploads, imports, backups and exports.

Each graph gets its tier's credits every month, on the 1st (UTC). The balance resets rather than accumulates, so unused credits don't carry over. Subgraphs draw on their parent's credits. An SEC subscription carries its own monthly credits for AI analysis on the SEC graph.

**If a graph runs out**, AI analysis is refused with an insufficient-credits message until the next monthly refill. Everything else, MCP tools included, keeps working.

**Watching usage:** the app's **Usage** page shows each graph's balance, recent transactions and rate limits. The same is available over the API at `GET /v1/graphs/{graph_id}/credits`.

## Try asking

- "Which subgraphs does this graph have?"
- "Create a subgraph so we can try a different model without touching the main graph."
