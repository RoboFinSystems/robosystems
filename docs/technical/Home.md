# Welcome to the RoboSystems Wiki!

RoboSystems is an open-source, AI-native financial intelligence platform for accounting, financial reporting, and investment management. It unifies structured data, document search, and AI memory over a **knowledge graph** — transactions, facts, reporting elements, and the calculation structures that relate them are all nodes and edges, with the semantics preserved rather than flattened into rows you query around. On top of that graph it gives AI agents and analysts a ledger-grade system of record they can both query and operate — closing the books, producing reports, and analyzing portfolios across your own ledger, your holdings, and SEC public filings queryable alongside them. It powers [RoboLedger](https://roboledger.ai) and [RoboInvestor](https://roboinvestor.ai).

**Every tenant gets their own graph** — not a row-level slice of a shared table, but a dedicated graph database on its own instance with a dedicated OLTP schema behind it. Your ontology, your taxonomies, and your calculation structures live in it as artifacts you can read, export, and take with you.

**The platform is fork-ready.** The [repository](https://github.com/RoboFinSystems/robosystems) ships full GitHub Actions CI/CD that deploys the CloudFormation infrastructure into your own AWS account — see the [Bootstrap Guide](Bootstrap-Guide) to stand up a deployment of your own.

This wiki is the technical documentation for using, operating, and building on the platform. Its examples call the hosted API at `https://api.robosystems.ai`; to run the platform yourself, start with [Local Development](Local-Development). It is organized by what you are doing: starting out, building on the API, connecting AI, querying the SEC repository, working with the RoboLedger and RoboInvestor extensions, the concepts underneath them, running your own stack, and hands-on demos.

**Which docs do you need?** To *use* RoboSystems, RoboLedger or RoboInvestor through Claude, ChatGPT or any MCP client, start with the product guides: [robosystems.ai/docs/guides](https://robosystems.ai/docs/guides), [roboledger.ai/docs](https://roboledger.ai/docs) and [roboinvestor.ai/docs](https://roboinvestor.ai/docs). To *build on* the platform or run it yourself, you are in the right place. For one endpoint's exact request and response, see the [API reference](https://robosystems.ai/docs/api).

## Start Here

Orientation: which docs you need, a first authenticated call, the vocabulary, and the system design.

- **[Quick Start](Quick-Start)** - From a new account to your first authenticated query and a connected AI client
- **[Core Concepts](Core-Concepts)** - The vocabulary: graphs, tiers, blocks, operators, operational vs analytical
- **[Architecture Overview](Architecture-Overview)** - System design and components

## Build on the API

The contracts every integration relies on, and the core graph surface.

- **[Authentication & API Keys](Authentication-and-API-Keys)** - The X-API-Key header for technical access, and the JWT boundary
- **[Operations Contract](Operations-Contract)** - The envelope, sync and async, idempotency, and the progress stream every write shares
- **[Errors & Rate Limits](Errors-and-Rate-Limits)** - The error body, what each status means, rate-limit headers, and insufficient credits
- **[Versioning & Compatibility](Versioning-and-Compatibility)** - What stays stable under /v1, and the two-tier SDK contract
- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy)** - The graph_id model, tiers, and subgraphs
- **[Graph Operations](Graph-Operations)** - Backups, subgraphs, materialize, tier changes
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Ad-hoc Cypher over the analytical (OLAP) graph
- **[File Uploads](File-Uploads)** - Staging tables and bulk loading for generic graphs
- **[Credits & Billing](Credits-and-Billing)** - The credit model: only AI operations consume credits
- **[Building Custom Integrations](Building-Custom-Integrations)** - The three integration lanes, and where each is documented
- **[Build a Ledger Integration](Build-a-Ledger-Integration)** - Tutorial: post events from an outside system into a RoboLedger graph

## Connect AI, Documents & Search

The MCP server, the operators, and the document and memory planes.

- **[AI Operators & MCP](AI-Operators-and-MCP)** - The MCP surface, connecting clients, and the operator endpoint
- **[Search & AI Retrieval](Search-and-AI-Retrieval)** - Index documents and retrieve them, and semantic memory
- **[Document Management](Document-Management)** - The document store on a graph

## Shared Repositories (SEC)

Platform-managed public datasets you subscribe to and query.

- **[Shared Repositories](Shared-Repositories)** - Subscribing, access, and snapshots
- **[SEC Data Model & Query Rules](SEC-Data-Model-and-Query-Rules)** - The SEC graph's model and the four rules that decide whether a number is right

## RoboLedger & RoboInvestor

The per-graph extensions: typed reads, command writes, and the workflows built on them.

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The URL topology, three sub-surfaces, and feature flags
- **[GraphQL Reads](GraphQL-Reads)** - Typed reads over the operational (OLTP) graph
- **[RoboLedger Operations](RoboLedger-Operations)** - The catalog of ledger operations and analytical views
- **[QuickBooks Sync & Write Policy](QuickBooks-Sync-and-Write-Policy)** - Sync windows, the book of record, write-back, and reconciling items
- **[Chart of Accounts Mapping](Chart-of-Accounts-Mapping)** - The mapping model, auto-map vs suggestions, and the closed-period lock
- **[Period Close](Period-Close)** - The fiscal calendar, blockers, the close and its receipt, reopening
- **[Forecasting & Metrics](Forecasting-and-Metrics)** - Forecast scenarios, the four drivers, plan history, and metrics
- **[RoboInvestor Operations](RoboInvestor-Operations)** - Portfolios, securities, positions, and issuer linking

## Concepts in Depth

The financial-content model underneath the products. Each concept page has a reference page behind it.

- **[Information Blocks](Information-Blocks)** - The envelope that ties numbers to their meaning
- **[Information Block Reference](Information-Block-Reference)** - Fields, per-type mechanics, the registry, rule patterns
- **[Event-Driven Ledger](Event-Driven-Ledger)** - Events before entries: REA, the three-level ledger, capture, classify, commit
- **[Event Block Reference](Event-Block-Reference)** - Fields, vocabularies, transitions, handlers, errors
- **[Taxonomy & Frameworks](Taxonomy-and-Frameworks)** - The rs-gaap and fac frameworks, the library, and the Taxonomy Block
- **[Reporting & Rendering](Reporting-and-Rendering)** - The calc-DAG, what the renderer derives, reporting styles, views
- **[Serialization & Export](Serialization-and-Export)** - XBRL, JSON-LD, holon and Tavi exports, and reading them without the platform

## Run It Yourself

Running, deploying, and operating your own stack.

- **[Local Development](Local-Development)** - From `just start` to your first authenticated query, and how local differs from hosted
- **[Windows Setup (WSL2)](Windows-Setup-with-WSL2)** - Set up WSL2 and run the stack on Windows
- **[Self-Hosted AI Models](Self-Hosted-AI-Models)** - Run the operators on open-weight models through any OpenAI-compatible endpoint
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Run real QuickBooks OAuth against a local stack
- **[Bootstrap Guide](Bootstrap-Guide)** - Set up AWS OIDC federation and GitHub Actions to deploy your own
- **[Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM)** - OIDC login and SCIM 2.0 provisioning for dedicated and self-hosted deployments
- **[Security & Compliance](Security-and-Compliance)** - The security posture, built-in controls, and optional compliance stacks
- **[Pipeline Guide](Pipeline-Guide)** - Data pipelines and the Dagster architecture

## Demos

Hands-on walkthroughs against a local stack.

- **[RoboLedger Demos](RoboLedger-Demo-Walkthrough)** - Synthetic-data and Seattle Method XBRL demos
- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query real SEC financial filings
- **[Custom Graph Schema](Custom-Graph-Schema)** - Design, build, and query a custom graph

## Reference

- **[Component READMEs](https://github.com/RoboFinSystems/robosystems?tab=readme-ov-file#documentation)** - Detailed technical docs in the codebase
- **[API reference](https://robosystems.ai/docs/api)** - API reference with machine-readable OpenAPI spec

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
