---
title: Work across several companies
description: For fractional CFOs, bookkeepers and advisors. One graph per company, a connection for each, and moving between clients in the app and your AI assistant.
order: 6
section: Get started
---

If you keep the books for several companies, each one is its own graph. Set it up that way from the start, and you and your AI assistant always know which company you're working on.

## One graph per company

A graph holds one company's books: its own QuickBooks connection, chart of accounts and mapping, schedules, closed months, reports and plans. A graph connects to one QuickBooks company, so each client gets a graph of its own. See [Connect your books to your AI assistant](connect-your-books.md).

Each graph is its own subscription on your organization's payment method, with its own tier and its own monthly credits. Nothing crosses from one graph to another unless you share a report.

## Who can reach each graph

Graphs belong to an organization. Only people in the organization that owns a graph can be given access to it, and each account belongs to one organization. So the client graphs you work on need to live in your organization.

- **Organization owners and admins** can reach every graph the organization owns.
- **Everyone else** sees only the graphs they've been given. To give someone access to one company, open that graph's **Dashboard** at [robosystems.ai](https://robosystems.ai), choose **Members**, and add them as a viewer (read only), a member (read and write) or an admin.

Owners and admins change members' roles and remove members on the organization's page at robosystems.ai.

## Move between companies in the app

The menu at the top right of RoboLedger lists one company for each of your RoboLedger graphs. Pick one and every page switches to that company's graph.

**Entity → All Entities** lists every company across your graphs in one table, with its graph's name and ID, and marks the one you're working on. The graph ID is what a client's investor or lender needs if you share reports with them.

## Connect your AI assistant to each company

**One connection is one graph.** You can connect with the general address and choose a company each time you sign in, but switching then means disconnecting and signing in again.

With several clients, give each company a connection of its own:

1. In the app at [robosystems.ai](https://robosystems.ai), select the company's graph and open **MCP**.
2. Copy the graph's own address. It has the graph in it, so that connection can only ever reach that company.
3. Add it to your AI client as a separate connector, named after the company, like "Cadence Labs books".

In Claude Code, the name is the first thing you type:

```bash
claude mcp add --transport http cadence-labs https://api.robosystems.ai/v1/graphs/{graph_id}/mcp
```

Named connectors are how you and your assistant tell clients apart. In a conversation about one client, start by asking which graph it's connected to. Setup for each client is in [Connect Claude, ChatGPT or any MCP client](https://robosystems.ai/docs/guides/connect-an-mcp-client).

## Share a client's reports

When a client's investor, lender or board member has a RoboLedger or RoboInvestor graph of their own, add their graph ID to a publish list in the client's graph and share the report from there. They get the statements, never the ledger. For someone without a graph, download the report's holon or Tavi file and send it; it opens in the free viewer at [xbrlkit.com](https://xbrlkit.com). See [Reports and sharing](reports-and-sharing.md).

## Try asking

- "Which company is this connection on, and when did its QuickBooks last sync?"
- "What's blocking the close for this client?"
- "Using the connections for [client] and [client], compare gross margin for the last quarter."
- "Create the September report for this client and tell me anything that looks off."

## Go deeper

- [Graph membership](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-membership): how organization roles and graph roles combine.
- [Who can reach a graph](https://robosystems.ai/docs/technical/graphs-and-multi-tenancy#who-can-reach-a-graph-orgs-roles-and-subscriptions): organizations, roles and why access never crosses organizations.
