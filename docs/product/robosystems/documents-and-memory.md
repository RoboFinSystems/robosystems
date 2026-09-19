---
title: Documents and memory
description: Keep policies and notes on a graph, search them, and store memories your AI assistant can recall later.
order: 7
---

A graph holds documents as well as numbers. Your AI assistant can search them, quote them, and — if you ask it to — save a policy or a close procedure so it is there the next time.

## Knowledge Base

**Knowledge Base** lists the documents on the selected graph. You can create one, edit it, and delete it. Each document is markdown, with an optional folder and tags.

Shared repositories such as SEC filings don't have a Knowledge Base of your own. Their filing text is searched through **Search** and through an MCP client connected to that graph.

Creating and editing documents uses no credits. Neither does search.

## Search

**Search** finds text across the documents in the graph. On the SEC filings graph it searches filing narratives. You can search by keyword, or by meaning when semantic search is on for that graph.

## Memory

**Memory** is a per-graph store of things your assistant should remember across conversations: a mapping decision, a counterparty you always treat a certain way, a note about a close. You can browse, edit and delete memories in the app.

Memory is on for graphs that have it enabled. It is not available on shared repositories, and a subgraph does not get a store of its own.

Your assistant can also remember and recall through MCP. That uses no credits. The Console uses credits because it is RoboSystems' own AI; see [Ask questions in the Console](console.md).

## Try asking

- "Save this as our capitalization policy."
- "What close procedures do we have documented?"
- "Remember that we treat [vendor] as a contractor, not an employee."
- "What do you remember about how we map owner draws?"
