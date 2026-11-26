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

There are two ways in. Almost everyone wants the first.

| | For | How |
|---|---|---|
| **Connect** | Anyone using an AI assistant | Paste the address above and sign in. This page |
| **Build** | Developers working across graphs, or from scripts | A local bridge or an API key. [For developers](#for-developers), below |

## Add the address to your client

**Claude (claude.ai and Claude Desktop):** Customize → Connectors → Add → Add custom connector, and paste the address. Claude detects the sign-in on its own, so leave the OAuth client fields blank. On a Team or Enterprise plan, an organization owner adds the connector first, under the organization's connector settings. Each member then connects it and signs in to RoboSystems with their own account.

**ChatGPT:** open **Plugins**, then **Add → Create custom MCP server**. Paste the address under Connection, leave Authentication on OAuth, accept the warning about custom servers and choose **Create as a plugin**. Custom MCP servers aren't available on every ChatGPT plan; if the menu item is missing, turn on developer mode in ChatGPT's settings, and on a business workspace an admin may need to allow it.

**Claude Code:** add the server, then run `/mcp`, pick `robosystems` and sign in.

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

**Cursor:** the entry goes under `mcpServers`. The editor opens the sign-in the first time it connects.

```json
"robosystems": { "url": "https://api.robosystems.ai/v1/mcp" }
```

**VS Code:** `mcp.json` lists servers under `servers` and needs the transport type.

```json
{
  "servers": {
    "robosystems": { "type": "http", "url": "https://api.robosystems.ai/v1/mcp" }
  }
}
```

Any other client that supports remote MCP servers over HTTP with OAuth sign-in works the same way. A connection added like this serves every tool of the graph you choose, RoboLedger included.

### Shortcut: SEC filings from the directory

To read SEC filings there is nothing to paste. Install the listing from your assistant's own directory:

- **Claude:** [RoboSystems SEC](https://claude.ai/directory/robosystems-sec) in the connector directory
- **ChatGPT:** [RoboSystems](https://chatgpt.com/plugins/plugin_asdk_app_6a8f6d7d50d081918787990d4cab45ca) in the plugin directory

Both reach the SEC filings only. For your own books or any other graph, use the address above. You still need a RoboSystems account with an SEC filings subscription; see [Analyze SEC filings](sec-filings.md).

### Skills for Claude Code

Claude Code can also install the RoboSystems plugin, which adds the same server plus five skills: finding its way around a graph, analyzing SEC filings, closing the month on RoboLedger, building a board pack from a closed period, and loading a portfolio company's report from a file onto a RoboInvestor graph. It is not the ChatGPT listing above. See [Skills and the RoboSystems plugin](skills-and-the-plugin.md).

```bash
claude plugin marketplace add RoboFinSystems/robosystems-plugin
claude plugin install robosystems@robosystems
```

## More than one graph

**One connection is one graph.** The address above lets you choose the graph when you sign in, so to switch, disconnect in your client and sign in again.

To keep several graphs connected at once, give each its own connection with the graph's own address. In the app, open **MCP**, choose the graph, and copy its address. It has the graph in it:

```text
https://api.robosystems.ai/v1/graphs/{graph_id}/mcp
```

When you sign in through a graph's own address, that graph is already selected. Subgraphs connect the same way, with the subgraph's id in the address. The public SEC filings are `sec`.

If you have a RoboLedger graph, the app's **MCP** page also shows a RoboLedger address, `https://api.robosystems.ai/v1/mcp/roboledger`. It offers only your own graphs running RoboLedger and carries the ledger and close tools without the subgraph, backup or other administration tools.

## For developers

Two routes use an API key instead of a sign-in. Create keys in the app; how they and their scopes work is in [Authentication and API keys](https://robosystems.ai/docs/technical/authentication-and-api-keys).

### One connection for every graph

The RoboSystems bridge is a small program your MCP client runs locally. Give it a key and it reaches every graph that key covers through one connection: the client lists your graphs, looks up any graph's tools and calls them, naming the graph each time. A subgraph is usable the moment it is created, with nothing to add or reconnect.

```json
{
  "mcpServers": {
    "robosystems-bridge": {
      "command": "npx",
      "args": ["-y", "@robosystems/mcp@latest"],
      "env": { "ROBOSYSTEMS_API_KEY": "<your key>" }
    }
  }
}
```

It can be limited to named graphs, and a graph or the whole connection can be made read-only. It needs Node.js 22 or later and a client that runs local servers, such as Claude Code, Claude Desktop, Cursor or VS Code; it does not work in claude.ai or ChatGPT. Setup and options are in [The MCP Bridge](https://robosystems.ai/docs/technical/the-mcp-bridge).

### Scripts and CI

A graph's own address also accepts a key in the `X-API-Key` header, so a script or CI job can connect without a browser. The app's **MCP** page generates a key scoped to that graph. A key never goes in the address, and the general address at the top of this page takes a sign-in only.

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

## Which one do I use?

| You want to | Use |
|---|---|
| Ask your assistant about your books or any graph | The address at the top of this page. When in doubt, this one |
| Read SEC filings in Claude or ChatGPT with nothing to paste | The directory listing |
| Keep two graphs connected in one chat client | Each graph's own address, one connection each |
| Work across many graphs, or create and use subgraphs as you go | The bridge |
| Connect from a script or CI job | A graph's own address with an API key |

## Try asking

Once connected, start by letting the client orient itself:

- "Which graph are you connected to, and what's in it?"
- "What can you do on this graph?"
- "Show me the schema, then a few example queries."

## Go deeper

- [Connecting from MCP clients](https://robosystems.ai/docs/technical/ai-operators-and-mcp#connecting-from-mcp-clients): the server's sign-in, transport and per-graph addresses in detail.
- [The MCP Bridge](https://robosystems.ai/docs/technical/the-mcp-bridge): one local connection for every graph a key covers.
- [API key lifecycle](https://robosystems.ai/docs/technical/authentication-and-api-keys#api-key-lifecycle): creating, scoping and revoking the keys scripts connect with.
