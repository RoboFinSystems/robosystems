---
title: Find your way around the app
description: "A tour of robosystems.ai: graphs, MCP, console, documents, memory, the data lake, subgraphs, backups and billing."
order: 6
section: In the app
---

Most of the work in RoboSystems happens in a conversation with your AI assistant. The app at [robosystems.ai](https://robosystems.ai) is where you create graphs, connect a client, watch usage, and keep documents and files on a graph. Documents and memories your assistant saves show up here too. Ledger and portfolio work shows in the RoboLedger and RoboInvestor apps.

## Graphs

**Home** lists the graphs you can use. Organization owners and admins start a new one with **Create Graph**, on Home or in the graph selector. It asks for the kind of graph. An entity graph then asks for the company, the products to turn on (RoboLedger, RoboInvestor, both or neither) and the tier. A generic graph, for data you model and load yourself, asks for its name and schema and is created on the Standard tier. Creating it starts a subscription on the organization's payment method, and asks for one first if there isn't one. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

Pick a graph in the selector at the top. The rest of the menu follows that graph.

**Dashboard** summarizes the selected graph: its size, its details and your role, with shortcuts to the other pages. The graph ID is here, which is what someone needs to share a report with your RoboLedger or RoboInvestor graph. Graph admins also get a **Members** button.

## MCP

**MCP** is the page that connects an AI client to this graph. It shows the general address and the graph's own address, or a subgraph's, and makes a key scoped to that graph for scripts. See [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md) and [Sign-in and graph access](oauth-and-graph-scope.md).

## Console, Search, documents and memory

**Console** asks questions about the selected graph inside the app. Questions run on RoboSystems' own AI, so they use credits. See [Ask questions in the Console](console.md).

**Search** finds text across the documents in the graph, or across the filings on the SEC repository.

**Knowledge Base** is those documents: policies, notes, procedures. You can create and edit your own here. **Memory** is a store on each graph that your assistant can recall later. See [Documents and memory](documents-and-memory.md).

## Data lake, schema, subgraphs and backups

**Data Lake** holds the graph's staging tables: Parquet files you upload to a generic graph, or, on a RoboLedger or RoboInvestor graph, tables built from its accounting or portfolio records. **Schema** lists the node and relationship types in the graph. **Subgraphs** are separate workspaces on the same instance. **Backups** creates and downloads a copy of the graph. See [Data lake, subgraphs and backups](data-lake-and-subgraphs.md).

On a shared repository such as SEC filings, Knowledge Base, Memory, Data Lake, Schema and Subgraphs are hidden, and Backups is download-only.

## Repositories and usage

**Repositories** is where you subscribe to shared graphs such as SEC filings. See [Analyze SEC filings](sec-filings.md).

**Usage** shows the graph's credit balance and recent credit transactions, its storage, and its limits, including rate limits.

## Your account

Your organization's name at the top of the sidebar opens its page. Members are listed there. Owners and admins also invite people and see the organization's graphs and billing.

The user menu at the top right reaches **User Settings** (your profile, password, passkeys, API keys and connected apps), these docs, and sign-out. The grid icon beside it switches to RoboLedger or RoboInvestor. The icons at the bottom of the sidebar contact support and report an issue.
