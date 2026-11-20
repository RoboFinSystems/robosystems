---
title: Connect your books to your AI assistant
description: Connect QuickBooks Online to RoboLedger, then add RoboLedger to Claude, ChatGPT, Cursor or VS Code as an MCP connector. What syncs, and how to add it.
order: 1
section: Get started
---

RoboLedger keeps your QuickBooks books in a knowledge graph your AI assistant can read and work with. Getting there takes three steps: create a graph, connect QuickBooks, and add RoboLedger to the AI client you already use.

## 1. Create an account and a graph

Sign up at [roboledger.ai](https://roboledger.ai/register). One account covers RoboSystems, RoboLedger and RoboInvestor.

Then create a graph with RoboLedger turned on. A graph is your company's own database: its books, reports, plans and documents live in it, and no other customer's data does. **Create Graph** in RoboLedger takes you to [robosystems.ai](https://robosystems.ai), where you enter the company's details and choose a size: Standard is usually ready straight away, and Large and XLarge may be set up on request. When a size is marked **Provisioned on request**, choose **Request access** and we'll set it up. Only your organization's owners and admins can create a graph. If the page says graph creation requires approval, your account can't create one on its own yet. Get in touch and we'll set it up.

## 2. Connect QuickBooks

In RoboLedger, open **Entity → Connections** and connect QuickBooks Online. You sign in to Intuit and approve access. You need admin permissions in the QuickBooks company.

A graph connects to one QuickBooks company, and it keeps the books of the company the graph was created for. To bring in another QuickBooks company, create another graph for it. See [Work across several companies](working-across-companies.md).

The first sync starts on its own and brings over your full history:

- company information and your chart of accounts
- customers, vendors and employees
- invoices, bills, payments, purchases, deposits, sales receipts and journal entries

Large companies can take several minutes. The first sync also sets up your fiscal calendar and maps your chart of accounts to standard reporting concepts. That mapping is done by AI, and it's the one step here that uses credits. See [What RoboLedger needs to work well](what-it-needs.md).

After the first sync, RoboLedger syncs each connection on its own once a day. For anything more recent, press **Sync Now** on the QuickBooks card, or ask your assistant to sync. The card shows when the last sync ran.

**Sync Now** asks how far back to go. **Last 60 days** is the regular sync: it re-reads the last 60 days and also picks up older transactions that were edited or deleted since the last sync. **From a specific date** pulls again from that date, and **Full rebuild** pulls your whole history again, for a company that hasn't synced in more than a month or when you want to be sure. Your assistant can sync from a date too. When a transaction RoboLedger already recorded has been edited, it's flagged for you to settle. See [When QuickBooks changes after a sync](changes-after-sync.md).

RoboLedger reads companies that keep their books in US dollars. A company with transactions in other currencies can't sync yet.

## 3. Add RoboLedger to your AI client

Every client uses the same address:

```text
https://api.robosystems.ai/v1/mcp
```

The first time a tool runs, you sign in to RoboSystems and choose which graph to connect. **One connection is one graph.** To work on another graph, such as the SEC filings for comparing against public companies, add a second connection for it. See [Which address to use](#which-address-to-use).

**Claude (claude.ai and Claude Desktop):** Customize → Connectors → Add → Add custom connector, and paste the address.

**Claude Code:** run this, then `/mcp` to sign in.

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

**ChatGPT:** open **Plugins**, then **Add → Create custom MCP server**. Paste the address under Connection, leave Authentication on OAuth, accept the warning about custom servers and choose **Create as a plugin**. Custom MCP servers aren't available on every ChatGPT plan; if the menu item is missing, turn on developer mode in ChatGPT's settings. A connection you add this way gets every RoboLedger tool. The RoboSystems plugin in ChatGPT's plugin directory reads SEC filings only, so add a custom server for your books.

**Cursor, VS Code and other MCP clients:** add the address to the client's MCP configuration. The client runs the sign-in for you.

```json
"robosystems": { "url": "https://api.robosystems.ai/v1/mcp" }
```

### Which address to use

RoboSystems answers on three addresses:

- **`https://api.robosystems.ai/v1/mcp`** is the general address used above. At sign-in you choose any of your graphs, or the SEC filings if you subscribe to them.
- **`https://api.robosystems.ai/v1/graphs/{graph_id}/mcp`** is one graph's own address, with that graph already chosen. The **MCP** page at robosystems.ai gives you the address for any graph. Use it to keep your books and the SEC filings connected side by side. The SEC filings are `https://api.robosystems.ai/v1/graphs/sec/mcp`.
- **`https://api.robosystems.ai/v1/mcp/roboledger`** is RoboLedger's own address. At sign-in it shows only your ledgers, and it leaves out a few tools: maintenance and administration ones such as filling in plan history, rebuilding a schedule, backups and choosing whether entries write back to QuickBooks. Use one of the other two when you need those.

If you're not sure, use the general address.

Nothing is written to QuickBooks by connecting, syncing or asking questions. See [Nothing writes to QuickBooks until you post](quickbooks-write-back.md).

## What to do next

- Check the mapping the first sync made. See [Map your chart of accounts](map-your-chart-of-accounts.md).
- Look around the app. See [Find your way around the app](the-roboledger-app.md).
- Start asking. See [Ask about your books](ask-about-your-books.md).

## Try asking

- "Which graph are you connected to, and when did QuickBooks last sync?"
- "Sync QuickBooks, then tell me what changed since last week."
- "Show me last month's income statement."

## Go deeper

- [Build a ledger integration](https://robosystems.ai/docs/technical/build-a-ledger-integration): bringing books from a source other than QuickBooks into a RoboLedger graph.
