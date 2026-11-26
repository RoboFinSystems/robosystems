# RoboSystems

**The open-source knowledge graph for finance.** RoboSystems is an open-source knowledge graph that gives your AI agents your company's books or portfolio, and public companies' filings to compare them against, as structured, traceable data, on your infrastructure, with the model you choose.

Three kinds of graph on one platform: a graph for your company (its books or its portfolio), shared repositories of public data to compare it against (today, 8,000+ SEC filers), and graphs you define yourself from your own schema, loaded from Parquet.

- **Entity graph** (your company's graph): one reporting entity or group. Its books, kept on RoboLedger or read from the ledger it closes in, or its portfolio on RoboInvestor: positions and the reports portfolio companies send.
- **Shared repository**: built by RoboSystems from public sources, kept current, and the same for every subscriber. Today: the SEC repository, to benchmark against public peers, research a company and compare filers.
- **Generic graph** (your own graphs): your schema, Parquet in, Cypher out.

Your company's graph and the SEC repository share one ontology, labelled in XBRL's standard terms wherever XBRL has one, so your numbers and a public company's compare line for line. A generic graph takes its own schema. All three run on LadybugDB. Ledger reports use rs-gaap, the RoboSystems reporting taxonomy, and leave as XBRL 2.1 or a holon (RDF). Subgraphs give your AI a workspace it extends over MCP, and semantic memory carries its work between sessions.

[RoboLedger](https://roboledger.ai), the general ledger built on it, is in [roboledger-app](https://github.com/RoboFinSystems/roboledger-app): a full accounting system in its own right, every number traced to the entry. Our own companies' books are kept on it. [RoboInvestor](https://roboinvestor.ai) runs on it too: equity research on public companies, your fund's positions with their terms, and the reports your portfolio companies send, shared from RoboLedger or as a PDF or spreadsheet.

Built by an ex-Workiva principal financial knowledge engineer and inactive CPA. Open source (Apache 2.0 here; MIT on the clients, the shared UI and xbrlkit): you keep the code and the data whatever happens to any vendor, including us.

**Run it yourself:** [on one server](https://robosystems.ai/docs/technical/self-hosting-on-a-server) · [in your AWS account](https://robosystems.ai/docs/technical/bootstrap-guide) · [with open-weight models](https://robosystems.ai/docs/technical/self-hosted-ai-models). Or let us run it: [robosystems.ai](https://robosystems.ai).

**Every tenant gets their own graph.** A dedicated graph database on its own instance, with a dedicated OLTP schema behind it, never a row-level slice of a shared table. Your schema, your taxonomies, and your calculation structures live in it as artifacts you can read, export, and take with you.

## Platform

What every graph gets, whichever kind it is:

- **A dedicated graph database**: LadybugDB on its own instance, sized by tier
- **MCP on every graph**: the graph's URL is an MCP server, with OAuth or an API key
- **AI Operators**: model-driven executors over the same MCP tools (analyst, author, mapping), run in the background with progress streamed back
- **Documents and search**: upload, index and search with full-text and semantic search
- **Subgraphs**: isolated workspaces beside a graph for agent scratch data, development and teams
- **Credits for AI only**: Operator calls consume credits by token usage; MCP calls and database operations are free
- **A web app**: graph management, the query console, schema explorer and billing, in [`robosystems-app`](https://github.com/RoboFinSystems/robosystems-app)

The core API is at `/v1`. Reads are REST `GET`; every write is a named operation with `Idempotency-Key` support, an audit record and progress over SSE.

## Extensions

An extension is a product built on the platform: its own schema, tables, API and app. An extension's graph is built around a reporting entity, a company or a fund, and holds its books or its portfolio. Each graph's extension data lives in its own PostgreSQL schema and is materialized to the graph for analysis. The surface is scoped to a graph by URL:

- **Reads**: `POST /extensions/{graph_id}/graphql`
- **Writes and analytical views**: `POST /extensions/{roboledger|roboinvestor}/{graph_id}/operations/{name}`

GraphQL, the operation routes, the MCP tools and the AI Operators all call the same functions, so there is one implementation of each rule. Details: [Extensions Surface Overview](https://robosystems.ai/docs/technical/extensions-surface-overview).

### [RoboLedger](https://roboledger.ai)

The general ledger and its financial reporting. Everything written is a block: the facts together with their structure, their rules and the check that they hold.

- **[Event Blocks](https://robosystems.ai/docs/technical/event-driven-ledger)**: record what happened (a sale, a payment, a disposal) and the ledger derives the debits and credits
- **[Information Blocks](https://robosystems.ai/docs/technical/information-blocks)**: statements, schedules, metrics and disclosures, each with its facts and the arithmetic rules that check them
- **[Taxonomy Blocks](https://robosystems.ai/docs/technical/taxonomy-and-frameworks)**: accounting frameworks as data. Ships `rs-gaap` (about 2,000 curated US GAAP concepts) and `fac`, with the chart of accounts mapped to them, by hand or by the mapping operator
- **Close**: a fiscal calendar and a period close that names whatever is holding it up
- **[Reporting](https://robosystems.ai/docs/technical/reporting-and-rendering)**: multi-period reports through a reporting style, with a draft-to-filed lifecycle, forecasts through the same statement structures, and statements read live from the ledger
- **[Serialization](https://robosystems.ai/docs/technical/serialization-and-export)**: a report is stored as a holon (RDF in named graphs) and downloads as that, as a DataBook (the holon as a Markdown document, every figure linked to its fact), with its ledger lineage, as an Excel workbook, as a compiled JSON model, or as XBRL 2.1. [xbrlkit](https://xbrlkit.com) checks and reads the files with no account
- **Industry classification**: one open vocabulary shared by a ledger's entities and every SEC filer: 543 industries in four levels, 7,200 public companies placed, in [`frameworks/industries`](/frameworks/industries/README.md)
- **Connections**: [QuickBooks](https://quickbooks.intuit.com/partners/affiliates?cid=par_pim_4TcakSEFQs73) as the books of record, and bank feeds through [Plaid](https://plaid.com) and [Mercury](https://mercury.com/partner/robosystems). A bank transaction arrives with a suggested account, and nothing reaches the ledger until someone approves it

App: [`roboledger-app`](https://github.com/RoboFinSystems/roboledger-app). Operations: [RoboLedger Operations](https://robosystems.ai/docs/technical/roboledger-operations).

### [RoboInvestor](https://roboinvestor.ai)

A fund's positions with their terms, linked to the businesses that issued them.

- **Portfolios, positions and securities**, with instrument terms (strike price, liquidation preference, vesting)
- **Cross-graph research**: when a portfolio company on the platform shares a published report, the holding links through to that company's own reported facts
- **Received reports**: load a report a company sent, from a holon, a compiled model, or rows read out of a PDF or workbook

App: [`roboinvestor-app`](https://github.com/RoboFinSystems/roboinvestor-app). Operations: [RoboInvestor Operations](https://robosystems.ai/docs/technical/roboinvestor-operations).

## Quick Start

Needs Docker, 8 GB of RAM, 20 GB of disk, and `uv` and `just`. macOS and Linux; on Windows use [WSL2](https://robosystems.ai/docs/technical/windows-setup-with-wsl2).

```bash
brew install uv just

just start          # the backend: API, Graph API, Dagster, PostgreSQL, Valkey, OpenSearch
just start apps     # the three frontend apps
just upgrade        # after a git pull: refresh images, recreate what changed
```

| Service | URL | App | URL |
| --- | --- | --- | --- |
| Main API | http://localhost:8000 | RoboSystems | http://localhost:3000 |
| Graph API | http://localhost:8001 | RoboLedger | http://localhost:3001 |
| Dagster UI | http://localhost:8002 | RoboInvestor | http://localhost:3002 |

`just start` runs the published images, so the checkout supplies configuration, not code. To run your own changes, comment out the image lines in `.env`: the stack is then built from the checkout, and an edit applies on `just restart` (`just rebuild` after a dependency change). `just init` sets up the Python environment. More: [Local Development](https://robosystems.ai/docs/technical/local-development).

To deploy to your own AWS account, fork the repo and run `just bootstrap`: see the [Bootstrap Guide](https://robosystems.ai/docs/technical/bootstrap-guide).

### Demos

```bash
just demo-sec               # NVIDIA's SEC filings through the pipeline
just demo-roboledger        # a ledger end to end: entries, schedules, a filed FY 2025 report, an AI close
just demo-roboinvestor      # from a private holding to its issuer's filed report
just demo-custom-graph      # your own schema, loaded and queried
just demo-coffee-roaster    # a synthetic manufacturer
just demo-saas-startup      # a synthetic SaaS company
```

### Development

```bash
just test                   # unit tests
just test-all               # tests plus lint, format and type checks
just test-code              # lint, format and type checks only
just logs api               # a service's logs
```

The [justfile](justfile) has the rest: migrations, graph operations, administration.

## Architecture

Built on open-source engines (PostgreSQL, LadybugDB, DuckDB, LanceDB, OpenSearch, Valkey): a transactional core, an analytical graph materialized from it, and vector search, with no proprietary database. The ontology, reporting taxonomies and calculation structures are artifacts you own and can take with you: [semantic sovereignty](https://robosystems.ai/blog/semantic-sovereignty).

**Every isolation primitive keys on `graph_id`, never on an organization.** Two graphs in one organization are separated by the same mechanism as two unrelated customers.

- **One graph database per tenant.** Tiers differ by instance size, not by how many tenants share one
- **A PostgreSQL schema per graph**, with `search_path` set on every request
- **The graph is a projection.** The ledger rows are the record, and the graph is rebuilt from them
- **Shared repositories are read-only**, queryable beside your graph and never writable through it

So one codebase serves the managed platform, a dedicated deployment and a self-hosted install with no fork. Details: [Graphs & Multi-Tenancy](https://robosystems.ai/docs/technical/graphs-and-multi-tenancy) · [Architecture Overview](https://robosystems.ai/docs/technical/architecture-overview).

**Identity.** Programs use `X-API-Key`; the apps use short-lived JWTs. Sign-in is a deployment choice: password, passkey, or an enterprise identity provider. Enterprise SSO (OIDC) and SCIM 2.0 are in the repository with no license gate. Details: [Authentication & API Keys](https://robosystems.ai/docs/technical/authentication-and-api-keys) · [Enterprise SSO & SCIM](https://robosystems.ai/docs/technical/enterprise-sso-and-scim) · [`SECURITY.md`](/SECURITY.md).

**On AWS:** ECS Fargate for the API and Dagster, EC2 for the graph instances, RDS, ElastiCache, OpenSearch, S3 and DynamoDB, deployed from CloudFormation by GitHub Actions over OIDC.

## SEC Shared Repository

A shared repository is a graph the platform maintains from outside sources, the same for everyone who reads it and writable by no one. Today: the SEC repository, US public companies' XBRL filings as one graph, there to benchmark your company against public peers, research a company and compare filers. It is read through MCP, Cypher and the AI Operators, beside your own graph. Every filing is parsed by [xbrlkit](https://xbrlkit.com), then enriched (element mapping, statement classification, disclosure tagging) and indexed for hybrid full-text and vector search.

The whole corpus is published monthly as one LadybugDB file on Hugging Face: [robosystems/sec-xbrl-knowledge-graphs](https://huggingface.co/datasets/robosystems/sec-xbrl-knowledge-graphs) (tens of GiB to download, well over 100 GiB on disk). `just sec-dump` pulls it for local use with no pipeline run. See the [SEC XBRL Pipeline](https://robosystems.ai/docs/technical/sec-xbrl-pipeline) guide and the [SEC adapter](/robosystems/adapters/sec/README.md).

## Connecting

### MCP

Every graph is an MCP server. The URL picks the graph: `sec` for the public repository, your graph id for your own.

**OAuth.** `https://api.robosystems.ai/v1/mcp` takes any OAuth-capable client (claude.ai, Claude Code, ChatGPT, VS Code, Cursor): sign in, choose the graph, and the client holds a revocable token for it. `https://api.robosystems.ai/v1/mcp/roboledger` is the same for RoboLedger graphs, with the accounting tools only.

```bash
claude mcp add --transport http robosystems https://api.robosystems.ai/v1/mcp
```

**API key**, in the `X-API-Key` header and never in the URL:

```bash
claude mcp add --transport http robosystems-sec \
  https://api.robosystems.ai/v1/graphs/sec/mcp \
  --header "X-API-Key: <your key>"
```

The app's MCP page (`/connect`) has the snippet for each client, filled in for the selected graph. More: [AI Operators and MCP](https://robosystems.ai/docs/technical/ai-operators-and-mcp) · the [MCP bridge](https://github.com/RoboFinSystems/robosystems-mcp-bridge) for one local connection to every graph an API key covers.

### Client libraries

```bash
npm install @robosystems/client     # TypeScript / JavaScript
pip install robosystems-client      # Python
```

[TypeScript client](https://github.com/RoboFinSystems/robosystems-typescript-client) · [Python client](https://github.com/RoboFinSystems/robosystems-python-client)

### API

- [API reference](https://robosystems.ai/docs/api): every endpoint, with its parameters, schemas and an example call
- [Interactive console](https://api.robosystems.ai): Swagger UI, to run a call with your own key
- [OpenAPI specification](https://api.robosystems.ai/openapi.json)

## Documentation

The technical documentation is at **[robosystems.ai/docs/technical](https://robosystems.ai/docs/technical)**. Good places to start:

- [Core Concepts](https://robosystems.ai/docs/technical/core-concepts) · [Architecture Overview](https://robosystems.ai/docs/technical/architecture-overview) · [Local Development](https://robosystems.ai/docs/technical/local-development)
- [Information Blocks](https://robosystems.ai/docs/technical/information-blocks) · [Event-Driven Ledger](https://robosystems.ai/docs/technical/event-driven-ledger) · [Taxonomy & Frameworks](https://robosystems.ai/docs/technical/taxonomy-and-frameworks) · [Serialization & Export](https://robosystems.ai/docs/technical/serialization-and-export)
- [Querying the Analytical Graph](https://robosystems.ai/docs/technical/querying-the-analytical-graph) · [Shared Repositories](https://robosystems.ai/docs/technical/shared-repositories) · [AI Operators & MCP](https://robosystems.ai/docs/technical/ai-operators-and-mcp)
- [Self-hosting on one server](https://robosystems.ai/docs/technical/self-hosting-on-a-server) · [Bootstrap Guide](https://robosystems.ai/docs/technical/bootstrap-guide) · [Security & Compliance](https://robosystems.ai/docs/technical/security-and-compliance) · [Trust Center](https://trust.robosystems.ai)

In the code, each package has a README; read it before working there. The main ones: [operations](/robosystems/operations/README.md) · [adapters](/robosystems/adapters/README.md) · [GraphQL](/robosystems/graphql/README.md) · [Graph API](/robosystems/graph_api/README.md) · [schemas](/robosystems/schemas/README.md) · [config](/robosystems/config/README.md) · [Dagster](/robosystems/dagster/README.md) · [auth](/robosystems/middleware/auth/README.md) · [MCP](/robosystems/middleware/mcp/README.md) · [CloudFormation](/cloudformation/README.md) · [examples](/examples/README.md) · [tests](/tests/README.md).

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues) · [Discussions](https://github.com/orgs/RoboFinSystems/discussions) · [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [CONTRIBUTING.md](/.github/CONTRIBUTING.md): branch conventions, coding standards and the pull request process

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

Apache-2.0 © 2026 RFS LLC

## Disclosures

<sub>Mercury is a fintech company, not an FDIC-insured bank. Banking services provided through Choice Financial Group and Column N.A., Members FDIC.</sub>
