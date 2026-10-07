---
title: Work across several companies
description: For fractional CFOs, controllers and advisors. One graph per client, a group of related companies inside a graph, and moving between them.
order: 6
section: Get started
---

If you're the finance lead for several companies, as a fractional CFO, controller or advisor, each unrelated client is its own graph. Companies that belong together, a holding company and the LLCs under it, share one graph as a reporting group. Set it up that way from the start, and you and your AI assistant always know which company you're working on.

## One graph per client

A graph holds one reporting group's books: its QuickBooks connection, each company's chart of accounts and mapping, schedules, closed months, reports and plans. Everyone with access to a graph sees everything in it, so two clients never share one. A graph connects to one QuickBooks company, the group parent's, so each client gets a graph of its own. See [Connect your books to your AI assistant](connect-your-books.md).

Each graph is its own subscription on your organization's payment method, with its own tier and its own monthly credits. Nothing crosses from one graph to another unless you share a report.

## Several companies in one graph

A graph is one **reporting group**: the group parent, the company the graph was created for, and the subsidiaries under it. Each company in the group keeps its own books, its own chart of accounts and mapping, its own fiscal calendar and its own close, on the group's fiscal year. Counterparties and the reporting library are shared across the group. A subsidiary's books don't come from QuickBooks; your assistant, the API and file-based integrations write them.

Ask your assistant to add a subsidiary: its name, its legal form and the share the parent holds. It gets a chart of accounts from a template and a calendar on the group's cadence, and from then on every question or action names the company it's about. When you don't name one, your assistant works on the group parent. In the app, **Entity → All Entities** shows the group and **New Entity** adds a company: its name, legal name, legal form, ticker, the company it's held under and the parent's share. There is no limit on the companies in a group; the graph's tier decides its capacity.

The group's combined statements are the sum of every company's own, line by line at the reporting concepts. They are combined, not consolidated: nothing is eliminated between the companies, so a balance one owes another is still in. Ask for the combined balance sheet on the group parent, or for any one company's own. In the app, **Ledger → Statements** on the group parent has a Scope control, _This entity_ or _Combined_, and the combined view says how many companies it summed.

A reporting group is companies under common control. It is not a way to put several clients in one graph: everyone with access to the graph sees every company in it.

## Who can reach each graph

Graphs belong to an organization. Only people in the organization that owns a graph can be given access to it, and each account belongs to one organization. So the client graphs you work on need to live in your organization.

- **Organization owners and admins** can reach every graph the organization owns, through their organization role.
- **Everyone else** sees only the graphs they've been given. To give someone access to one company, a graph admin opens that graph's **Dashboard** at [robosystems.ai](https://robosystems.ai), chooses **Members**, and adds them as a viewer (read only), a member (read and write) or an admin. The same dialog changes someone's role on that graph or removes it.

Two kinds of role are at work. A graph role, set in **Members**, covers one company. An organization role, set on the organization's page at robosystems.ai, covers the whole organization: owners and admins invite people there, change their organization role and remove them. In **Members**, people who reach the graph through their organization role are marked **Via org role**, and their access is changed on the organization's page.

## Move between companies in the app

The menu at the top right of RoboLedger lists every company in every RoboLedger graph you can reach, grouped by graph, with each group parent first and its subsidiaries under it. Type to narrow the list by name or ticker. Pick a company and every page shows its books; a company in another graph switches the graph first. The company you're on is remembered between visits.

**Entity → All Entities** is the selected graph's reporting group: each company's legal form, the share the parent holds, its status and the month it's closed through, with **Select** to move to one and **New Entity** to add one. **Entity → Entity Info** holds the selected company's details and, for a subsidiary, names its parent and the share held. The graph's ID, which a client's investor or lender needs if you share reports with them, is on the graph's **Dashboard** at [robosystems.ai](https://robosystems.ai).

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
- "Add Maple Court LLC as a wholly owned subsidiary and set up its chart of accounts."
- "Close September for Maple Court LLC."
- "Show me the group's combined balance sheet, then Maple Court on its own."
- "What's blocking the close for this client?"
- "Using the connections for [client] and [client], compare gross margin for the last quarter."
- "Create the September report for this client and tell me anything that looks off."

## Go deeper

- [Graph membership](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-membership): how organization roles and graph roles combine.
- [Who can reach a graph](https://robosystems.ai/docs/technical/graphs-and-multi-tenancy#who-can-reach-a-graph-orgs-roles-and-subscriptions): organizations, roles and why access never crosses organizations.
