---
title: Ask questions in the Console
description: The Console is RoboSystems' own AI on a graph. Questions use credits from your plan. Claude and ChatGPT do not.
order: 7
section: In the app
---

The **Console** is an AI assistant that runs inside RoboSystems, on the graph you have selected. You type a question in plain language. It reads that graph and answers.

It sits in a drawer at the bottom of every page. Click the **Console** bar, or press Ctrl and the backtick key to open it; drag its top edge to resize it, or maximize it. The page above stays where it is, so you can ask about what you're looking at.

It is the same idea as connecting Claude or ChatGPT, with one difference that matters for billing: **questions in the Console use credits**, from the graph's monthly allowance or, on the SEC filings, from your repository subscription. A client you connect yourself does not. The reasoning in Claude or ChatGPT happens in that product; RoboSystems only returns data.

When a graph's credits can't cover a question, the Console refuses it until the next monthly refill. MCP tools keep working. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

## What it can see

The Console follows the graph you picked. Its examples and the tools it uses change with it:

- a **RoboLedger** graph: books, statements and close status
- a **RoboInvestor** graph: portfolios, positions, received reports
- the **SEC filings** repository: public-company statements and filing text
- a generic graph you modeled yourself: schema and Cypher

When it queries the graph, it shows the **Generated Cypher** so you can check the answer, and **Run** it again. Results can be downloaded as CSV or copied as JSON.

## Asking it to make a change

A plain question only reads. To have the Console change something, start the request with `/do`, for example "/do add a gross margin metric block". It makes additive changes: metric and forecast blocks, counterparties, and memories. When it finishes, it lists each change it made. In RoboLedger, Plan and Explorer refresh to show them. Changes use credits like questions do, and need write access to the graph.

Some work stays out of the Console on purpose: posting and editing journal entries, closing or reopening a period, and deleting anything. Do those in the RoboLedger app or through your own AI assistant.

Graph admins can see every change made to a graph, from the Console, a connected AI assistant or the API, on the **Activity** page.

## Commands

Commands start with a slash. Apart from `/do`, they don't use credits.

- `/do` makes a change, as above.
- `/query` runs a Cypher query you write yourself.
- `/search` searches the graph's documents.
- `/recall` searches the graph's memories, on graphs that have them.
- `/examples` shows example queries for this graph.
- `/mcp` shows this graph's connector address and the setup for common clients. `/mcp key` makes an API key scoped to this graph, for clients that take a key instead of a sign-in.
- `/help` lists them all.

The full setup for Claude, ChatGPT, Cursor and API keys is in [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md).

## Try asking

- "Which graph am I on, and what's in it?"
- "Show me the schema, then a few example queries."
- On a ledger graph: "What's blocking the next close?"
- On the SEC graph: "Income statement for NVDA for the last three fiscal years."

## Go deeper

- [The operator endpoint](https://robosystems.ai/docs/technical/ai-operators-and-mcp#the-operator-endpoint): the endpoint the Console's questions run through, with its modes and limits.
