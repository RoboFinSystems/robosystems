---
title: Data lake, subgraphs and backups
description: Stage files, query them, create subgraphs for experiments, and download backups. All of it lives on the graph's instance.
order: 8
---

These pages are for working on the graph itself: files you load, a workspace that doesn't touch production, and a copy you can take away. They are in the app at [robosystems.ai](https://robosystems.ai) once a graph is selected. Shared repositories hide some of them; backups on a repository are download-only.

## Data Lake

**Data Lake** is the staging tables for the selected graph. Files you upload land here first, as tables you can query with SQL, and from here they can be materialized into the graph.

On a RoboLedger or RoboInvestor graph the lake also holds the live tables the graph is built from. **Sync to Graph** rebuilds the analytical graph from those tables.

Uploading, querying and materializing use no credits.

## Schema

**Schema** lists the node labels, relationship types, constraints and indexes in the graph. On a graph you modeled yourself this is how you see what you loaded. On a RoboLedger or SEC graph the schema is the platform's.

## Subgraphs

A **subgraph** is a separate workspace inside a graph, for a what-if or a test without touching the main graph. It has its own data, runs on the parent's instance, and shares the parent's permissions and credits. Each tier allows a set number. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

Open **Subgraphs** to list them and create one. Connect an AI client to a subgraph with its own address, the same way you connect the parent — the id is in the URL. The **MCP** page copies it for you.

## Backups

**Backups** creates a downloadable copy of the graph. Create one, wait for it to finish, and download it. Repositories offer download of the system-generated backups only.

There is no restore button in the app. To rebuild a graph from files you still have, load them again through the data lake.

## Try asking

- "Which subgraphs does this graph have?"
- "Create a subgraph so we can try a different model without touching the main graph."
- "What tables are in the data lake?"
