# The MCP Bridge

`@robosystems/mcp` is a small program an MCP client runs locally. With an API key it gives the client one connection to every graph the key covers: your own graphs, their subgraphs and the shared repositories you subscribe to. The model names the graph on each call, so it can work across graphs in one conversation and use a subgraph the moment it is created.

It is the developer route. To ask an assistant about one graph, connect the hosted server by address and sign in instead; that needs no key, no install and works in claude.ai and ChatGPT. See [Connecting from MCP Clients](AI-Operators-and-MCP.md#connecting-from-mcp-clients).

| | Connect by address | The bridge |
| --- | --- | --- |
| Runs | On our servers | On your machine, started by the client |
| Signs in with | OAuth in the browser | An API key in the client's configuration |
| Reaches | One graph per connection | Every graph the key covers |
| A new subgraph | A new connection | Usable on the next call |
| Works in | Any MCP client with HTTP transport, including claude.ai and ChatGPT | Clients that run local servers: Claude Code, Claude Desktop, Cursor, VS Code |
| Tools the model sees | The graph's own tools | Four fixed tools that look up and call any graph's tools |

---

## Install

There is no install step. The client runs the package through `npx`; it needs Node.js 22 or later.

```json
{
  "mcpServers": {
    "robosystems-bridge": {
      "command": "npx",
      "args": ["-y", "@robosystems/mcp@latest"],
      "env": {
        "ROBOSYSTEMS_API_KEY": "<your key>"
      }
    }
  }
}
```

This is the shape of `claude_desktop_config.json` and of other clients that take stdio `command` entries. In Claude Code:

```bash
claude mcp add robosystems-bridge -e ROBOSYSTEMS_API_KEY=<your key> -- npx -y @robosystems/mcp@latest
```

That adds the bridge to the current project only; add `--scope user` to have it in every project. The command suits the simple case of a key alone. Anything more ([choosing graphs](#choosing-graphs), aliases, [read-only](#read-only)) is another `-e` flag each, and is easier to read and change in the configuration file.

Keep `@latest` in the arguments: a global `npm install -g @robosystems/mcp` can shadow `npx` and pin an old version. Creating and scoping keys is covered in [Authentication & API Keys](Authentication-and-API-Keys.md#api-key-lifecycle).

---

## How a Session Works

The bridge serves four tools, the same on every connection:

| Tool | Does |
| --- | --- |
| `list-graphs` | Lists the graphs the connection reaches, with subgraphs, and the id or alias to pass as `graph` |
| `get-tools` | For one graph: the index of its tools and its instructions; with `names`, the full description and input schema of those |
| `call-read-tool` | Runs one of a graph's read tools |
| `call-write-tool` | Runs one of a graph's write tools |

A graph's tools are looked up, not listed. A ledger graph, the `sec` repository and a subgraph each return their own tools and instructions from `get-tools`, so the bridge's own tool list stays four small definitions whatever the graphs behind it serve. The tools themselves are the ones in the [tool catalog](AI-Operators-and-MCP.md#tool-catalog).

A typical sequence:

1. `list-graphs` to see what is reachable.
2. `get-tools` with a graph, to read its tool index and its instructions.
3. `get-tools` with that graph and `names`, to read a tool's definition before calling it.
4. `call-read-tool` or `call-write-tool` with the graph, the tool's name and its arguments.

Every call names its graph. There is no current graph and nothing to switch, so conversations that share one bridge process cannot redirect each other. Each call is forwarded to that graph's own endpoint (`/v1/graphs/{graph_id}/mcp`) with your key, where access, write policy and audit apply exactly as they do for a direct connection.

Reads and writes stay separate because clients treat them differently. The split follows the read-only hint the server publishes for each tool, so a client can approve reads without prompting and still show each write by name and arguments.

---

## Subgraphs

A subgraph is a graph with its own, smaller tool list: schema changes, Cypher reads and writes, and graph information. Create one with the parent's `create-subgraph` through `call-write-tool`; the id it returns (`{parent_graph_id}_{name}`) is accepted as `graph` on the next call.

```text
call-write-tool  graph: kg1a2b…          name: create-subgraph   arguments: { "name": "scratch" }
call-write-tool  graph: kg1a2b…_scratch  name: add-node-table    arguments: { … }
call-write-tool  graph: kg1a2b…_scratch  name: write-graph-cypher arguments: { … }
call-read-tool   graph: kg1a2b…_scratch  name: read-graph-cypher  arguments: { … }
```

The result of `create-subgraph` also carries a `connector_url` and advice to add a connector for it. That advice is for a connection to a single graph; through the bridge, pass the id as `graph`. The parent's `list-subgraphs` lists them and `delete-subgraph` removes one. See [Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy.md) for how subgraphs relate to their parent.

---

## Choosing Graphs

By default the connection reaches every graph the key covers. `ROBOSYSTEMS_GRAPH_IDS` limits it to named graphs and their subgraphs, and can give them aliases:

```json
"env": {
  "ROBOSYSTEMS_API_KEY": "<your key>",
  "ROBOSYSTEMS_GRAPH_IDS": "books=kg1a2b3c4d5e6f7a8b9c0d,sec:ro"
}
```

- An entry is a graph id or `alias=id`. The alias is what the model passes as `graph`, and what an approval prompt shows
- Any graph outside the list is refused before a request is made
- A key scoped to one graph cannot list the account, so name its graph here

---

## Read-Only

Text a model reads from one graph can try to steer a write on another. Two settings take writes off the table:

- `ROBOSYSTEMS_READ_ONLY=true` makes the whole connection read-only: `call-write-tool` is not listed and cannot be called
- A `:ro` suffix in `ROBOSYSTEMS_GRAPH_IDS` does the same for one graph and its subgraphs

A read-only graph's write tools are left out of `get-tools`. These are settings of the connection, on top of what the server allows: every call is still checked against the role of the key's owner on that graph.

---

## Configuration

| Variable | Purpose | Default |
| --- | --- | --- |
| `ROBOSYSTEMS_API_KEY` | API key, sent as `X-API-Key`; account-wide or scoped to one graph | none |
| `ROBOSYSTEMS_GRAPH_IDS` | The graphs to reach, as ids or `alias=id` pairs separated by commas; `:ro` marks one read-only | every graph the key reaches |
| `ROBOSYSTEMS_READ_ONLY` | `true` withholds every write tool | `false` |
| `ROBOSYSTEMS_API_URL` | Base URL of the API, for a stack you run | `https://api.robosystems.ai` |
| `ROBOSYSTEMS_GRAPH_ID` | Selects proxy mode for this one graph (below) | none |
| `ROBOSYSTEMS_MCP_URL` | Selects proxy mode for a full endpoint URL | none |

For a stack you run, set `ROBOSYSTEMS_API_URL` to its address: `http://localhost:8000` in [Local Development](Local-Development.md), or your server's name in [Self-Hosting on a Server](Self-Hosting-on-a-Server.md). The read and write split needs a server that publishes the per-tool hints; against one that does not, every tool runs through `call-write-tool`.

---

## Proxy Mode

Setting `ROBOSYSTEMS_GRAPH_ID` (or `ROBOSYSTEMS_MCP_URL`) turns the bridge into a transparent pipe to one graph's endpoint. Every message is forwarded unchanged with the key attached, so the client sees that graph's own tools and instructions, exactly as it would connecting the address directly.

```json
"env": {
  "ROBOSYSTEMS_API_KEY": "<your key>",
  "ROBOSYSTEMS_GRAPH_ID": "sec"
}
```

This is for a client that can only run local servers and needs one graph's tools listed natively. A client with HTTP transport should connect that graph's address directly.

---

## Limits

- **Local clients only.** claude.ai, ChatGPT and other hosted chat clients cannot run it; they connect by address
- **API key only.** The bridge does not use the OAuth routes
- **One more step per tool.** The model reads a tool's definition before its first call, where a direct connection lists every definition up front
- **Client permissions are per bridge tool.** A client can allow all reads or approve each write, but cannot allow one named platform tool
- **Protocol.** The bridge answers MCP revision `2025-06-18`, the revision the server implements

---

## Related Documentation

- **[AI Operators & MCP](AI-Operators-and-MCP.md)** - The MCP surface, the tool catalog and connecting by address
- **[Authentication & API Keys](Authentication-and-API-Keys.md)** - Keys, scopes and revocation
- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy.md)** - Graphs, subgraphs and what a key reaches
- **[Custom Graph Schema](Custom-Graph-Schema.md)** - Node and relationship tables on a subgraph
- **[Source and releases](https://github.com/RoboFinSystems/robosystems-mcp-bridge)** - The package's repository

---

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems-mcp-bridge/issues)
- [Technical docs](Home.md)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
