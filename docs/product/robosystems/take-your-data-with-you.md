---
title: Take your data with you
description: Every way to get your data out of RoboSystems, from report files to the graph's database, and what happens when you disconnect, cancel or leave.
order: 11
section: Your account and data
---

Your data is yours, and so is the software: RoboSystems is open source under Apache 2.0. This page lists every way to take a copy out, and what happens to your data when you disconnect QuickBooks, cancel a graph or close your account.

## Take a copy at any time

| What | Where | Format |
|---|---|---|
| **Financial reports** | RoboLedger, **Reports → View Reports**, on a published report | XBRL 2.1 package, Tavi (JSON), holon (JSON-LD) |
| **A series or a plan** | RoboLedger, **Explorer** and **Plan** | CSV or JSON |
| **The graph's database** | robosystems.ai, **Backups** (graph admins) | A LadybugDB database file, zipped |
| **Your records, row by row** | The API, or ask your AI assistant | JSON |

**Reports** open in the free viewer at [xbrlkit.com](https://xbrlkit.com), with no account, and in any tool that reads XBRL. See [Reports and sharing](https://roboledger.ai/docs/reports-and-sharing).

**Backups** are made automatically every night, and you can make your own. The download is the graph's database file, which the open-source [LadybugDB](https://github.com/LadybugDB/ladybug) engine opens: on a RoboLedger graph, the ledger, facts and plans as of the graph's last refresh from its records. Documents and report files are stored separately and aren't in it, so download reports on their own. Downloads are limited each month by tier. See [Data lake, subgraphs and backups](data-lake-and-subgraphs.md).

**Your records.** There is no one-click export of the general ledger in the app. Accounts, transactions and journal entries can all be read over the API (GraphQL at `/extensions/{graph_id}/graphql`), and the graph can be queried with Cypher or its tables with SQL. The simplest way is often to ask your AI assistant: "List every journal entry in September with its lines."

## Disconnect QuickBooks

A graph admin disconnects in RoboLedger under **Entity → Connections**. RoboSystems' access to your QuickBooks company is revoked and syncing stops. **Everything already imported stays in the graph**: your history, statements, reports and plans. Reconnect the same company later and syncing picks up again. See [When QuickBooks changes after a sync](https://roboledger.ai/docs/changes-after-sync).

## Cancel or delete a graph

The organization owner cancels a graph on the organization's page: open **Subscriptions** and choose **Cancel Subscription** on the graph. You choose when:

- **At the end of the billing period**, while the subscription is active. The graph keeps working until then. Afterwards it is suspended, and removed seven days later.
- **Now.** The graph is deleted within about ten minutes. You type the graph ID to confirm.

Either way, RoboSystems tries to take a **final backup** first and keeps it for 90 days; the organization's owners and admins can download it over the API, or ask support. Don't rely on it: the graph is removed even if that backup fails, and once it is, the graph no longer appears in the app. Its database, ledger records, documents, search index and report files are gone for good.

**Download what you want to keep before you cancel.**

## Close your account

Accounts aren't deleted from the app. Cancel your graphs first, then ask support, using the icon at the bottom of the sidebar at [robosystems.ai](https://robosystems.ai). Billing records are kept as the law requires. The [privacy policy](https://robosystems.ai/pages/privacy) covers your rights over personal data.

## Run it yourself

The platform that runs your graph is public at [github.com/RoboFinSystems/robosystems](https://github.com/RoboFinSystems/robosystems). The [technical docs](https://robosystems.ai/docs/technical) cover [local development](https://robosystems.ai/docs/technical/local-development) and deploying your own copy to AWS with the [bootstrap guide](https://robosystems.ai/docs/technical/bootstrap-guide).

## Try asking

- "Which reports does this graph have, and which periods do they cover?"
- "List the chart of accounts with each account's balance at the end of September."
- "Create a backup of this graph."

## Go deeper

- [What's inside each format](https://robosystems.ai/docs/technical/serialization-and-export#whats-inside-each-format): the contents of the XBRL, holon and Tavi files.
- [Graph operations](https://robosystems.ai/docs/technical/graph-operations): backups over the API, and opening a backup file.
