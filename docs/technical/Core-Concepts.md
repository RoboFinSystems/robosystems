# Core Concepts

RoboSystems is a multi-tenant financial knowledge graph platform, and a handful of recurring terms run through every endpoint, SDK call, and README. This page is an orientation glossary: short, plain-language definitions that build on one another, each linking to its deep-dive page. Read it once to get your bearings, then follow the links when you need detail.

## Table of Contents

- [graph_id and Tenancy](#graph_id-and-tenancy)
- [Graph Tiers](#graph-tiers)
- [Subgraphs](#subgraphs)
- [Element and the Chart of Accounts](#element-and-the-chart-of-accounts)
- [Information Block](#information-block)
- [Taxonomy Block and Event Block](#taxonomy-block-and-event-block)
- [Operator vs REA Agent](#operator-vs-rea-agent)
- [Framework vs Reporting Style](#framework-vs-reporting-style)
- [OLTP vs OLAP](#oltp-vs-olap-operational-vs-analytical-graph)
- [The Three Retrieval Planes](#the-three-retrieval-planes)
- [Where to Go Next](#where-to-go-next)

## graph_id and Tenancy

The platform is multi-tenant by `graph_id`. Each customer entity gets a dedicated, isolated graph database, and the `graph_id` is the handle that scopes authentication, billing, and data. It appears as a URL path segment on nearly every graph-scoped route — `/v1/graphs/{graph_id}/...` and `/extensions/{graph_id}/graphql` — so you almost never operate "across the platform"; you operate inside one graph at a time.

Parent graph IDs match the regex `kg[a-f0-9]{16,}` — `kg` followed by 16 or more hex characters (for example `kg1234567890abcdef`). The identifier is the unit of isolation: a request authorized for one `graph_id` cannot read another's data.

The simplest call needs no `graph_id` at all — the health check:

```bash
curl http://localhost:8000/v1/status
```

Authenticated calls read the API key from `.local/config.json` (created by `just demo-user`) and send it as `X-API-Key`. Listing the graphs your key can reach:

```bash
curl "http://localhost:8000/v1/graphs" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)"
```

**Note:** Use `X-API-Key` for backend and `curl` testing. `Authorization: Bearer` (JWT) is a frontend concern and is not used for API testing.

→ Deep-dive: [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)

## Graph Tiers

A graph's tier is the dedicated AWS instance size it runs on, plus its subgraph allowance. RoboSystems uses **instance-based naming** for transparency — the tier name tells you exactly what infrastructure you get, rather than a marketing label.

| Technical name | Display name |
| --- | --- |
| `ladybug-standard` | Standard |
| `ladybug-large` | Large |
| `ladybug-xlarge` | XLarge |
| `ladybug-shared` | — (platform-managed) |

For the instance size, subgraph allowance and limits behind each tier, see **`GET /v1/offering`** or
**[robosystems.ai/pricing](https://robosystems.ai/pricing)**. Those are computed from
[`.github/configs/graph.yml`](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml),
the same file that drives provisioning — so they cannot fall out of step with what you actually get.

`ladybug-shared` is not a customer-selectable tier. It is the platform's read-only fleet that serves public repositories such as SEC filings, accessible to all users but owned by the platform. Customer graphs run on one of the three dedicated tiers.

For live tier specifications — instance types, memory, subgraph limits, and pricing — see the [Graph Configuration](https://github.com/RoboFinSystems/robosystems/blob/main/.github/configs/graph.yml) in codebase.

→ Deep-dive: [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)

## Subgraphs

A subgraph is an isolated database *inside* a parent graph — a workspace for AI memory, forks and testing, or team collaboration. Subgraphs run on the same instance as their parent (no extra infrastructure cost), share the parent's credit pool and subscription limits, and keep their data fully isolated: queries cannot cross a subgraph boundary.

A subgraph ID is the parent ID plus the subgraph name, joined by an underscore: `{parent_graph_id}_{subgraph_name}` (for example `kg1234567890abcdef_dev`). Subgraph names are alphanumeric, 1–20 characters, with no hyphens or underscores. Subgraphs are created through the graph-lifecycle command surface (`POST /v1/graphs/{graph_id}/operations/create-subgraph`).

→ Deep-dive: [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)

## Element and the Chart of Accounts

In the graph, **everything is an Element**. There is no separate `accounts` table. A chart-of-accounts line, a US-GAAP reporting concept, an abstract grouping node, and a dimension axis are all `Element` nodes — they differ in provenance and role, not in structure. The arrangement that turns a flat set of Elements into a chart of accounts or a reporting taxonomy lives in separate taxonomy, structure, and association records.

The `Element` node is keyed by `identifier` and carries properties such as `qname`, `name`, `period_type`, `type`, `balance`, `is_abstract`, `is_numeric`, and `is_textblock`. Because Elements are unified, mapping a chart-of-accounts line to a reporting concept is just a typed `Association` row — the same machinery that expresses XBRL taxonomy hierarchies — not a bespoke subsystem.

You can see Elements directly with a Cypher read over the analytical graph:

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1234567890abcdef/query" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "MATCH (e:Element) RETURN e.qname, e.balance LIMIT 5"}'
```

Or, using the `just` helper with a literal graph ID:

```bash
just graph-query kg1234567890abcdef "MATCH (e:Element) RETURN e.qname LIMIT 5"
```

→ Deep-dive: [Information Blocks](Information-Blocks)

## Information Block

The Information Block is the platform's load-bearing primitive: a typed, queryable, exchangeable unit of accounting information. It bundles *atoms* (Elements, associations, facts) with *molecular wiring* (classification, rules, mechanics, presentation) into one self-describing object.

What the frontend calls a "Balance Sheet" or a "Depreciation Schedule" the backend, API, and AI Operators all speak of as an `InformationBlock` discriminated by a `block_type`. The registered block types are `schedule`, `rollforward`, `balance_sheet`, `income_statement`, `cash_flow_statement`, `equity_statement`, `comprehensive_income`, and `metric`. The governing rule is: **what crosses a system boundary is a molecule, never a bare atom.** You exchange a whole, self-describing block, not a loose Element or fact.

Information Blocks are read through the GraphQL surface. Note that `graph_id` comes from the URL, not from a query argument:

```bash
curl -X POST "http://localhost:8000/extensions/kg1234567890abcdef/graphql" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ informationBlocks { id blockType } }"}'
```

→ Deep-dive: [Information Blocks](Information-Blocks)

## Taxonomy Block and Event Block

Two sibling envelopes feed the same Information Block machinery, and they are how you contribute data into a graph — you do not write bare Elements or raw ledger rows.

- **Taxonomy Block** — writes *vocabulary*: custom ontology, chart of accounts, and reporting extensions. This is how new Elements and their arrangement enter the graph.
- **Event Block** — writes the *ledger*: it posts REA economic events, from which the general ledger is derived. The GL is a derivation, not something you post directly.

The principle is the same as for Information Blocks: vocabulary changes go through the Taxonomy Block and ledger writes go through the Event Block, so the molecule crosses the boundary and the platform keeps the derived state consistent. There is no `accounts` table to write to and no raw element or event write path.

→ Deep-dive: [Information Blocks](Information-Blocks)

## Operator vs REA Agent

Two distinct concepts share the word "agent," and the platform keeps them separate on purpose.

- **REA Agent** — an economic counterparty in a business event: a customer, vendor, or employee. This is the canonical ontology term, drawn from the Resources-Events-Agents accounting model. An REA Agent is data: a party an event happened *with*. It is distinct from the reporting `Entity`, which is the graph owner.
- **Operator** — the AI executor layer (Claude- and MCP-driven), with concrete classes like `CypherOperator` (natural-language graph queries) and `MappingOperator` (autonomous chart-of-accounts mapping). Operators live at `POST /v1/graphs/{graph_id}/operator` and execute work.

The rename resolved an "agent operates on agent" naming collision: an AI Operator can act on REA Agent data. Marketing copy may still say "AI agent," but in the API and codebase the executor is always an **Operator**.

Invoking an Operator (auto-routing to the best-fit one for the query):

```bash
curl -X POST "http://localhost:8000/v1/graphs/kg1234567890abcdef/operator" \
  -H "X-API-Key: $(jq -r .api_key .local/config.json)" \
  -H "Content-Type: application/json" \
  -d '{"message": "What was total revenue last period?"}'
```

→ Deep-dive: [AI Operators and MCP](AI-Operators-and-MCP)

## Framework vs Reporting Style

These two terms describe different layers of how numbers become a report, and they are easy to conflate.

- **Framework** — a *basis of accounting*: the recognition and measurement rules that determine what the numbers *are*. `rs-gaap` is the framework for US GAAP financial reporting. A different framework changes the numbers themselves.
- **Reporting style** — a *presentation within* a framework: the layout and arrangement of the balance sheet, income statement, and cash flow statement, and the equity form. A different reporting style rearranges the same numbers; it does not change them.

A practical consequence: banking and insurance presentations are **reporting styles within `rs-gaap`**, not separate frameworks, because they re-present the same GAAP measurements. The decision rule is whether the underlying recognition and measurement change (framework) or only the layout changes (reporting style).

→ Deep-dive: [Information Blocks](Information-Blocks)

## OLTP vs OLAP (Operational vs Analytical Graph)

Each tenant has two stores, and knowing which one you are talking to determines which transport you use.

- **Operational graph (OLTP)** — live transactional state, held in the PostgreSQL `extensions` database with a schema per `graph_id`. You read it through **GraphQL** at `POST /extensions/{graph_id}/graphql`.
- **Analytical graph (OLAP)** — the XBRL hypercube of reporting facts, held in LadybugDB's columnar engine. You read it through **Cypher** at `POST /v1/graphs/{graph_id}/query`.

The operational store is the source of truth for transactions; its rows are **materialized** into the analytical graph for reporting and analysis (`POST /v1/graphs/{graph_id}/operations/materialize`). The materialization pipeline runs PostgreSQL OLTP → DuckDB staging → LadybugDB graph.

**Important:** On a main graph, Cypher access is **read-only**. Writes flow through the staging-and-materialize pipeline (for analytical data) or through Event Blocks (on the operational side). Only subgraphs accept Cypher writes.

→ Deep-dive: [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)

## The Three Retrieval Planes

The same operational/analytical/unstructured split is exposed to AI clients through the MCP (Model Context Protocol) surface as three retrieval planes. Each plane pairs a query tool with a schema tool, so a client can introspect before it queries:

| Plane | Query tool | Schema tool | Backs onto |
| --- | --- | --- | --- |
| **Operational** (typed OLTP reads) | `query-graphql` | `get-graphql-schema` | Extensions GraphQL surface |
| **Analytical** (read-only Cypher) | `read-graph-cypher` | `get-graph-schema` | LadybugDB OLAP graph |
| **Unstructured** (document/filing search) | `search-documents` | `get-document-section` | Document / SEC-filing index |

An Operator — or any MCP client — composes across the planes: pull a policy from the unstructured plane, a live balance from the operational plane, and a reporting fact from the analytical plane, then reason over all three.

A few rules worth knowing up front:

- `read-graph-cypher` is read-only; write clauses (`CREATE`, `SET`, `DELETE`, `MERGE`, `DROP`) and `CALL db.` / `CALL apoc.` procedures are blocked. Run `get-graph-schema` first.
- `query-graphql` rejects mutations and subscriptions and takes `graph_id` from context, not as an argument. Run `get-graphql-schema` first.
- `search-documents` returns a `document_id` per hit; pass it to `get-document-section` for the full section text. Semantic (KNN) ranking is opt-in via `semantic=true` (the default is keyword BM25).

A fourth MCP tool, `resolve-element`, maps a natural-language financial concept to its XBRL Element name using vector search — useful for grounding a query before you run it.

→ Deep-dive: [AI Operators and MCP](AI-Operators-and-MCP)

## Where to Go Next

This page is the orientation hub. Each glossary term has a deeper home:

| Topic | Page |
| --- | --- |
| The full system map — services, storage, infrastructure | [Architecture Overview](Architecture-Overview) |
| `graph_id`, tiers, subgraphs, OLTP vs OLAP in depth | [Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy) |
| Information Blocks, Elements, Taxonomy/Event Blocks, frameworks | [Information Blocks](Information-Blocks) |
| Operators, the REA Agent distinction, the MCP retrieval planes | [AI Operators and MCP](AI-Operators-and-MCP) |

For the full, machine-readable endpoint surface, use the live OpenAPI spec at [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (or `http://localhost:8000/docs` against a local stack), rather than re-deriving routes by hand.

## Related Documentation

**Wiki Guides:**
- **[Architecture Overview](Architecture-Overview)** - The higher-altitude system map: services, storage, and infrastructure
- **[Graphs and Multi-Tenancy](Graphs-and-Multi-Tenancy)** - Tenancy, tiers, subgraphs, and the operational/analytical split
- **[Information Blocks](Information-Blocks)** - Elements, Information Blocks, and the Taxonomy/Event contribution envelopes
- **[AI Operators and MCP](AI-Operators-and-MCP)** - The AI executor layer and the three MCP retrieval planes

**Codebase Documentation:**
- **[GraphQL Extensions](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/graphql/README.md)** - The typed operational-read surface
- **[MCP Middleware](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/middleware/mcp/README.md)** - The retrieval-plane tools and their contracts
- **[Operations](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/operations/README.md)** - The business-logic kernel behind every transport

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
