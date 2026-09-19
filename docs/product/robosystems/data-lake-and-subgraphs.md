---
title: Data lake, subgraphs and backups
description: Stage and query tables, create subgraphs for experiments, and download backups. All of it lives on the graph's instance.
order: 8
section: In the app
---

These pages are for working on the graph itself: the tables it is loaded from, a workspace that doesn't touch production, and a copy you can take away. They are in the app at [robosystems.ai](https://robosystems.ai) once a graph is selected. Shared repositories hide the data lake, schema and subgraphs, and their backups are download-only.

## Data Lake

**Data Lake** holds the selected graph's staging tables, which you can query with SQL.

- **On a generic graph**, one you model yourself, upload Parquet files into a staging table you name, new or existing. **Ingest to Graph** loads them into the graph.
- **On a RoboLedger or RoboInvestor graph**, there is nothing to upload. The staging tables are built from the graph's accounting or portfolio records, and **Sync to Graph** brings the graph up to date with them. Tick **Rebuild entire graph** to start it clean.

Uploading, querying, ingesting and syncing use no credits.

## Schema

**Schema** lists the node labels in the graph with their properties, and the relationship types with the nodes they connect. The **Export Schema** tab shows it as JSON you can copy. On a generic graph this is how you see what you loaded. On a RoboLedger or RoboInvestor graph the schema is the platform's.

## Subgraphs

A **subgraph** is a separate workspace inside a graph, for a what-if or a test without touching the main graph. It has its own data, runs on the parent's instance, and shares the parent's permissions and credits. Each tier allows a set number. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

Open **Subgraphs** to list them and create one. A subgraph created here starts empty. Each one has **Connect**, which opens the **MCP** page with that subgraph's own address, so you can connect an AI client to it the same way you connect the parent. You can also back up or delete a subgraph from its row.

## Backups

Every graph and subgraph is backed up automatically each night. **Backups** lists those copies and lets you make your own: create one, choose how many days to keep it (up to your tier's limit, 7 days on Standard), wait for it to finish, and download it. Downloads are limited per month on every graph, and on a repository you can download the system-generated backups only.

There is no restore button in the app. A RoboLedger or RoboInvestor graph rebuilds from its own records with **Sync to Graph**. A generic graph rebuilds from the files still in its data lake: run **Ingest to Graph** with **Rebuild entire graph**. For anything else, contact support.

## Try asking

- "Which subgraphs does this graph have?"
- "Create a subgraph so we can try a different model without touching the main graph."
- "Create a backup of this graph."
