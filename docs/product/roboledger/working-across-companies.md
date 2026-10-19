---
title: Work across several companies
description: For fractional CFOs, controllers and advisors. One graph per client, a group of related companies consolidated inside a graph, and moving between them.
order: 6
section: Get started
---

If you're the finance lead for several companies, as a fractional CFO, controller or advisor, each unrelated client is its own graph. Companies that belong together, a holding company and the LLCs under it, share one graph as a reporting group. Set it up that way from the start, and you and your AI assistant always know which company you're working on.

## One graph per client

A graph holds one reporting group's books: its QuickBooks connection, each company's chart of accounts and mapping, schedules, closed months, reports and plans. Everyone with access to a graph sees everything in it, so two clients never share one. A graph connects to one QuickBooks company, the group parent's, so each client gets a graph of its own. See [Connect your books to your AI assistant](connect-your-books.md).

Each graph is its own subscription on your organization's payment method, with its own tier and its own monthly credits. Nothing crosses from one graph to another unless you share a report.

## Several companies in one graph

A graph is one **reporting group**: the group parent, the company the graph was created for, and the subsidiaries under it. Each company in the group keeps its own books, its own chart of accounts and mapping, its own fiscal calendar and its own close, on the group's fiscal year. Counterparties and the reporting library are shared across the group. QuickBooks connects the group parent and only the parent; a subsidiary's books come from its own bank accounts, your assistant, the API and file-based integrations.

Ask your assistant to add a subsidiary: its name, its legal form and the share the parent holds. It gets a chart of accounts from a template and a calendar on the group's cadence, and from then on every question or action names the company it's about. When you don't name one, your assistant works on the group parent. In the app, **Entity → All Entities** shows the group and **New Entity** adds a company: its name, legal name, legal form, ticker, the company it's held under and the parent's share. There is no limit on the companies in a group; the graph's tier decides its capacity.

A reporting group is companies under common control. It is not a way to put several clients in one graph: everyone with access to the graph sees every company in it.

## Consolidate the group

The group's **combined** statements are the sum of every company's own, line by line at the reporting concepts, with nothing eliminated. The **consolidated** statements take one more company in the group, the **consolidation entity**, which holds only the consolidating entries. Add it as a company, mark it the group's consolidation entity, and start its chart from the _Consolidation adjustments_ template. The consolidated statements read its entries, the combined statements leave them out, and no operating company's statements ever include them.

The share the parent holds decides how the group treats a company: half or more is consolidated in full; a fifth to a half takes the equity method and is left out of the sum — the group carries the investment and its share of the associate's result instead, and a distribution it receives reduces the investment and shows in investing; less is an investment; and you can set the treatment yourself where control is not the percentage. A company that joins or leaves the group is summed for the months it was a member; the rest of its history stays readable on its own.

**Your assistant drafts the consolidating entries from the books.** For any month it works out what the group needs: the parent's investment eliminated against each subsidiary's capital, with an outside owner's share of that capital as noncontrolling interest; the balances the companies owe each other; intercompany income against the matching expense; the outside owners' share of the month's result; and the parent's share of an equity-method company's result and distributions. A transaction is intercompany when it names another company in the group as its counterparty, so every company in the group is also one of the group's counterparties. Ask to see the entries first, then have them written as drafts in the consolidation entity; the group close posts them. Each entry brings its balance to what the month needs, less what the consolidation entity already holds, so an entry you post yourself for one of those adjustments counts toward it and drafting a month again replaces only the drafts written before. Where an outside owner's share of a month's result isn't their percentage, as with a preferred return or a waterfall, tell your assistant the amount and why, and the drafts use it.

**What the books don't explain is left for you, never plugged.** When the parent's investment in a subsidiary doesn't match its share of the subsidiary's capital, that elimination is held back and the difference named — unless the parent booked the subsidiary's distributions as income, in which case the draft reverses that income against the investment. When two companies disagree on what one owes the other, the lower amount is eliminated and the difference listed. Correct the books, post the explaining entry yourself, or close over the difference knowingly.

The consolidated balance sheet shows the investment and the intercompany balances at zero and noncontrolling interest beside the parent's equity, the income statement shows the share of income attributable to it, and the statement of changes in equity rolls the noncontrolling interest forward beside the parent's equity: its share of income, the outside owners' contributions, and the distributions paid to them. The consolidation entity's close is the group close, and it saves the month's consolidated statements; see [Close a group of companies](month-end-close.md#close-a-group-of-companies).

In the app, **Ledger → Statements** on the group parent has a Scope control: _This entity_, _Combined_ (the companies summed, nothing eliminated), _Consolidated_ (with the consolidation entity's adjustments) or _Worksheet_ (each company in its own column, then the combined sum, the adjustments and the consolidated total, with an equity-method company beside the sum). The menu at the top right leaves the consolidation entity out; select it under **Entity → All Entities**, and its Closing Book shows the month's **Consolidating entries**, with **Draft entries** to write them and anything held back listed beneath.

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
- "Add a consolidation entity for the group and draft August's consolidating entries."
- "Show me August as a consolidation worksheet, one column per company."
- "Where does the noncontrolling interest on the consolidated balance sheet come from?"
- "What's holding the group's September close?"
- "What's blocking the close for this client?"
- "Using the connections for [client] and [client], compare gross margin for the last quarter."
- "Create the September report for this client and tell me anything that looks off."

## Go deeper

- [Graph membership](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-membership): how organization roles and graph roles combine.
- [Who can reach a graph](https://robosystems.ai/docs/technical/graphs-and-multi-tenancy#who-can-reach-a-graph-orgs-roles-and-subscriptions): organizations, roles and why access never crosses organizations.
