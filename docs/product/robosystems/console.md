---
title: Ask questions in the Console
description: The Console is RoboSystems' own AI on a graph. It uses credits from your plan. Claude and ChatGPT do not.
order: 6
---

The **Console** is an AI assistant that runs inside RoboSystems, on the graph you have selected. You type a question in plain language. It reads that graph and answers.

It is the same idea as connecting Claude or ChatGPT, with one difference that matters for billing: **the Console uses credits** from the graph's monthly allowance. A client you connect yourself does not. The reasoning in Claude or ChatGPT happens in that product; RoboSystems only returns data.

If a graph runs out of credits, the Console is refused until the next monthly refill. MCP tools keep working. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

## What it can see

The Console is graph-aware. Examples and tools follow the graph you picked:

- a **RoboLedger** graph: books, statements, the close
- a **RoboInvestor** graph: portfolios, positions, received reports
- the **SEC filings** repository: public-company statements and filing text
- a graph you modeled yourself: schema and Cypher

It can also show the query it ran, so you can check the answer.

## Connect a client from here

Type `/mcp` in the Console to copy the connector for this graph. The full setup for Claude, ChatGPT, Cursor and API keys is in [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md).

## Try asking

- "Which graph am I on, and what's in it?"
- "Show me the schema, then a few example queries."
- On a ledger graph: "What's blocking the next close?"
- On the SEC graph: "Income statement for NVDA for the last three fiscal years."
