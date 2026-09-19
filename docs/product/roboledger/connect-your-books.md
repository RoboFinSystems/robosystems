---
title: Connect your books to your AI assistant
description: Connect QuickBooks Online to RoboLedger, then add RoboLedger to Claude, ChatGPT, Cursor or VS Code as an MCP connector. What syncs, and how to add it.
order: 1
section: Get started
---

RoboLedger keeps your QuickBooks books in a knowledge graph your AI assistant can read and work with. Getting there takes three steps: create a graph, connect QuickBooks, and add RoboLedger to the AI client you already use.

## 1. Create an account and a graph

Sign up at [roboledger.ai](https://roboledger.ai/register). One account covers RoboSystems, RoboLedger and RoboInvestor.

Then create a graph with RoboLedger turned on. A graph is your company's own database: its books, reports, plans and documents live in it, and no other customer's data does. If the page says graph creation requires approval, your account can't create one on its own yet. Get in touch and we'll set it up.

## 2. Connect QuickBooks

In RoboLedger, open **Entity → Connections** and connect QuickBooks Online. You sign in to Intuit and approve access. You need admin permissions in the QuickBooks company.

The first sync starts on its own and brings over your full history:

- company information and your chart of accounts
- customers, vendors and employees
- invoices, bills, payments, purchases, deposits, sales receipts and journal entries

Large companies can take several minutes. The first sync also sets up your fiscal calendar and maps your chart of accounts to standard reporting concepts. That mapping is done by AI, and it's the one step here that uses credits. See [What RoboLedger needs to work well](what-it-needs.md).

After the first sync, syncing is on demand. Press **Sync Now** on the QuickBooks card, or ask your assistant to sync. RoboLedger doesn't sync on a schedule, so sync before you ask about recent activity.

A regular sync picks up the last 60 days of changes. If something older changed in QuickBooks, choose a full sync in the sync options, or ask your assistant to sync from a specific date. When a transaction RoboLedger already recorded has been edited, it's flagged for you to settle. See [When QuickBooks changes after a sync](changes-after-sync.md).

RoboLedger reads companies that keep their books in US dollars. A company with transactions in other currencies can't sync yet.

## 3. Add RoboLedger to your AI client

Every client uses the same address:

```text
https://api.robosystems.ai/v1/mcp
```

The first time a tool runs, you sign in to RoboSystems and choose which graph to connect. **One connection is one graph.** To work on another graph, such as the SEC filings for comparing against public companies, add a second connection and choose that graph.

**Claude (claude.ai and Claude Desktop):** Settings → Connectors → Add custom connector, and paste the address.

**Claude Code:** run this, then `/mcp` to sign in.

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

**ChatGPT:** turn on developer mode, then Settings → Connectors → Create, and paste the address. A connector you add this way gets every RoboLedger tool. The RoboSystems plugin in ChatGPT's plugin directory reads SEC filings, so use a custom connector for your books.

**Cursor, VS Code and other MCP clients:** add the address to the client's MCP configuration. The client runs the sign-in for you.

```json
"robosystems": { "url": "https://api.robosystems.ai/v1/mcp" }
```

Nothing is written to QuickBooks by connecting, syncing or asking questions. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

## What to do next

- Check the mapping the first sync made. See [Map your chart of accounts](map-your-chart-of-accounts.md).
- Look around the app. See [Find your way around the app](the-roboledger-app.md).
- Start asking. See [Ask about your books](ask-about-your-books.md).

## Try asking

- "Which graph are you connected to, and when did QuickBooks last sync?"
- "Sync QuickBooks, then tell me what changed since last week."
- "Show me last month's income statement."
