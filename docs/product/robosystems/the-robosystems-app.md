---
title: Find your way around the app
description: "A tour of robosystems.ai: graphs, MCP, console, documents, memory, the data lake, subgraphs, backups and billing."
order: 5
---

Most of the work in RoboSystems happens in a conversation with your AI assistant. The app at [robosystems.ai](https://robosystems.ai) is where you create graphs, connect a client, watch usage, and keep documents and files on a graph. Everything your assistant reads and writes is visible here.

Your password, passkeys, API keys and connected apps are under **Settings**. Organization members, graphs and billing are under **Organization**.

## Graphs

**Home** lists the graphs you can use. **Create Graph** starts a subscription on the organization's payment method and lets you turn on RoboLedger, RoboInvestor, or neither. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

Pick a graph in the selector at the top. The rest of the menu follows that graph.

**Dashboard** is a summary of the selected graph: its name, members and recent activity.

## MCP

**MCP** is the page that connects an AI client to this graph. It shows the general address, the graph's own address, and a key scoped to that graph for scripts. See [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md) and [Sign-in and graph access](oauth-and-graph-scope.md).

## Console, Search, documents and memory

**Console** asks questions about the selected graph inside the app. It runs on RoboSystems' own AI, so it uses credits. See [Ask questions in the Console](console.md).

**Search** finds text across the documents in the graph.

**Knowledge Base** is those documents: policies, notes, procedures. You can create and edit them here. **Memory** is a per-graph store your assistant can recall later, on graphs that have it enabled. See [Documents and memory](documents-and-memory.md).

## Data lake, schema, subgraphs and backups

**Data Lake** is the staging tables for files you upload, and for a RoboLedger or RoboInvestor graph the live tables the graph is built from. **Schema** lists the node and relationship types in the graph. **Subgraphs** are separate workspaces on the same instance. **Backups** creates and downloads a copy of the graph. See [Data lake, subgraphs and backups](data-lake-and-subgraphs.md).

## Repositories and usage

**Repositories** is where you subscribe to shared graphs such as SEC filings. See [Analyze SEC filings](sec-filings.md).

**Usage** shows this graph's credit balance, recent AI calls and rate limits.

## Your account

Creating a graph, inviting members and paying for the organization happen on **Organization**. Stripe checkout returns here. The user menu at the top right also reaches Settings and the other RoboSystems apps.
