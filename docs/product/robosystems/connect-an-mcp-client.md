---
title: Connect Claude, ChatGPT or any MCP client
description: Add RoboSystems to Claude, ChatGPT, Claude Code, Cursor or VS Code with one address. Sign in, choose a graph, and start asking.
order: 1
section: Start here
---

RoboSystems is a remote MCP server. There is nothing to install: you give your AI client one address, sign in to RoboSystems, and choose the graph the client may work on. From then on the client can read that graph, and act on it within your role.

```text
https://api.robosystems.ai/v1/mcp
```

The first time a tool runs, the client opens RoboSystems in your browser. You sign in, choose a graph and approve. What that screen asks, and how to change the graph later, is in [Sign-in and graph access](oauth-and-graph-scope.md).

You need a RoboSystems account and a graph to connect: your own, a RoboLedger graph, or the SEC filings. See [Graphs, tiers and credits](graphs-tiers-and-credits.md).

## Claude

**claude.ai and Claude Desktop:** Settings → Connectors → Add custom connector, and paste the address. Claude detects the sign-in on its own, so leave the OAuth client fields blank.

**Claude Code:** add the server, then run `/mcp`, pick `robosystems` and sign in.

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

Claude Code can also install the RoboSystems plugin, which adds the same server plus three skills that teach Claude how to explore a graph, analyze SEC filings and close the month:

```bash
claude plugin marketplace add RoboFinSystems/robosystems-plugin
claude plugin install robosystems@robosystems
```

## ChatGPT

Turn on developer mode, then Settings → Connectors → Create, and paste the address. A connector added this way serves every tool of the graph you choose, RoboLedger included.

To read SEC filings without any setup, install the [RoboSystems plugin](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) from ChatGPT's plugin directory instead.

## Cursor, VS Code and other clients

Add the address to the client's MCP configuration. The editor opens the sign-in the first time it connects.

```json
"robosystems": { "url": "https://api.robosystems.ai/v1/mcp" }
```

Any client that supports remote MCP servers over HTTP with OAuth sign-in works the same way. For a client that only runs local servers, use the [stdio bridge](https://github.com/RoboFinSystems/robosystems-mcp-client), which forwards to the same server with an API key.

## More than one graph

**One connection is one graph.** The address above lets you choose the graph when you sign in, so to switch, disconnect in your client and sign in again.

To keep several graphs connected at once, give each its own connection with the graph's own address. In the app, open **MCP**, choose the graph, and copy its connector. The address has the graph in it:

```text
https://api.robosystems.ai/v1/graphs/{graph_id}/mcp
```

When you sign in through a graph's own address, that graph is already selected. Subgraphs connect the same way, with the subgraph's id in the address. The public SEC filings are `sec`:

```text
https://api.robosystems.ai/v1/graphs/sec/mcp
```

## Scripts, CI and clients that can't sign in

A graph's own address also accepts an API key in the `X-API-Key` header, so a script or CI job can connect without a browser. The app's **MCP** page generates a key scoped to that graph. The general address above takes a sign-in only and rejects keys.

```bash
claude mcp add --transport http robosystems-kg123 \
  https://api.robosystems.ai/v1/graphs/kg123/mcp \
  --header "X-API-Key: <your key>"
```

```json
"robosystems-kg123": {
  "url": "https://api.robosystems.ai/v1/graphs/kg123/mcp",
  "headers": { "X-API-Key": "<your key>" }
}
```

A key never goes in the address. For how keys and their scopes work, see [Authentication and API keys](https://robosystems.ai/docs/technical/authentication-and-api-keys).

## Try asking

Once connected, start by letting the client orient itself:

- "Which graph are you connected to, and what's in it?"
- "What can you do on this graph?"
- "Show me the schema, then a few example queries."
