---
title: Sign-in and graph access
description: What the RoboSystems consent screen asks, why one connection reaches one graph, how to switch graphs, and how to revoke an app's access.
order: 2
section: Start here
---

When an AI client connects to RoboSystems, you sign in and approve it on a consent screen. Approving gives that client access to **one graph**, with the permissions your role on that graph already gives you. The client never sees your password, and you can revoke it at any time.

## The consent screen

The screen names the app asking and what it will be able to do: use the MCP tools on one graph, with the access your role gives you. Then you choose the graph.

- **Connected with the general address** (`/v1/mcp`): you pick from your graphs and the shared repositories you subscribe to, such as SEC filings.
- **Connected with a graph's own address** (`/v1/graphs/{graph_id}/mcp`): that graph is already selected, and it's the only one the connection can use.

Choose **Allow access** to connect, or **Cancel** to send the client away with nothing. The request expires after ten minutes; if it does, start the connection again from your client.

**Check who is asking.** The screen tells you where you'll be sent after approving. Apps RoboSystems recognises, such as Claude, ChatGPT and VS Code, are shown as they are. Any other app shows **This app isn't verified by RoboSystems**, and if its name looks like a well-known app while the destination is somewhere else, the screen says so. Only continue if you started the connection yourself, from an app you trust.

## One connection, one graph

A connection is bound to the graph you chose. A client connected to your books can't reach another company's graph, or the SEC filings, through that connection. That's the point: you decide per graph which apps can work on it.

**To switch graphs**, disconnect in your client and connect again, choosing the other graph.

**To use several graphs side by side**, for example your books and the SEC filings for comparisons, add a separate connection for each, using each graph's own address. The app's **MCP** page gives you the address for any graph. See [Connect Claude, ChatGPT or any MCP client](connect-an-mcp-client.md).

## What the client can do

The client acts as you, within your role on the graph:

- **Anyone with access** can read: ask questions, run reports and queries, search documents.
- **Writing**, such as posting entries, changing mappings or creating a subgraph, needs a role that allows it. A read-only member who asks for a write gets an error saying the role is read-only.
- **Shared repositories** such as SEC filings are read-only for everyone.

Access is checked on every request, not only at sign-in. If you're removed from a graph, the connection stops working on it within ten minutes.

## Staying connected

You sign in once. The client renews its access on its own, so a connection you use keeps working. A connection left unused for 90 days needs you to sign in again.

## Revoking access

Open **Settings → Connected apps** in the app. It lists every client you've authorized, with the graph it reaches and when it was last used. **Revoke** ends that connection immediately: the client's next request fails, and it will ask you to authorize again.

Changing your password signs every connected app out at once.

## API keys instead of sign-in

Scripts, CI jobs and clients that can't open a browser connect with an API key rather than a sign-in, using a graph's own address. A key created from the app's **MCP** page reaches only that graph and its subgraphs. Revoke a key under **Settings → API keys**. See [Authentication and API keys](https://robosystems.ai/docs/technical/authentication-and-api-keys).

## Try asking

- "Which graph are you connected to?"
- "What can you do on this graph?"

## Go deeper

- [Graph-scoped access](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-scoped-access): how every request is checked against the graph it names.
- [Graph membership](https://robosystems.ai/docs/technical/authentication-and-api-keys#graph-membership): organization roles, graph roles, and how they combine.
