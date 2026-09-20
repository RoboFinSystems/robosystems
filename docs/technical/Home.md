# Welcome to the RoboSystems Wiki!

RoboSystems is an open-source, AI-native financial intelligence platform for accounting, financial reporting, and investment management. It unifies structured data, document search, and AI memory over a **knowledge graph** — transactions, facts, reporting elements, and the calculation structures that relate them are all nodes and edges, with the semantics preserved rather than flattened into rows you query around. On top of that graph it gives AI agents and analysts a ledger-grade system of record they can both query and operate — closing the books, producing reports, and analyzing portfolios across your own ledger, your holdings, and SEC public filings queryable alongside them. It powers [RoboLedger](https://roboledger.ai) and [RoboInvestor](https://roboinvestor.ai).

**Every tenant gets their own graph** — not a row-level slice of a shared table, but a dedicated graph database on its own instance with a dedicated OLTP schema behind it. Your ontology, your taxonomies, and your calculation structures live in it as artifacts you can read, export, and take with you.

**The platform is fork-ready.** The [repository](https://github.com/RoboFinSystems/robosystems) ships full GitHub Actions CI/CD that deploys the CloudFormation infrastructure into your own AWS account — see the [Bootstrap Guide](Bootstrap-Guide) to stand up a deployment of your own.

This wiki is the technical documentation for using, operating, and building on the platform. Its examples call the hosted API at `https://api.robosystems.ai`; to run the platform yourself, start with [Local Development](Local-Development). It is organized into seven areas — orientation, the operational API, the RoboLedger and RoboInvestor extensions, the financial-content fabric, the document and search layer, self-hosting and development, and hands-on demos.

## Getting Started & Platform

Orientation: get an API key, make your first query, learn the vocabulary, and understand the system design.

- **[Quick Start](Quick-Start)** - From a new account to your first authenticated query and a connected AI client
- **[Core Concepts](Core-Concepts)** - The vocabulary: graphs, tiers, blocks, operators, operational vs analytical
- **[Architecture Overview](Architecture-Overview)** - System design and components
- **[Security & Compliance](Security-and-Compliance)** - The security posture, built-in controls, and optional compliance stacks for forks

## Operations Layer

The core platform API and graph management.

- **[Authentication & API Keys](Authentication-and-API-Keys)** - The X-API-Key header for technical access, and the JWT boundary
- **[Enterprise SSO & SCIM](Enterprise-SSO-and-SCIM)** - OIDC login and SCIM 2.0 provisioning for dedicated and self-hosted deployments
- **[Graphs & Multi-Tenancy](Graphs-and-Multi-Tenancy)** - The graph_id model, tiers, and subgraphs
- **[Shared Repositories](Shared-Repositories)** - Platform-managed public datasets (SEC) you subscribe to and query
- **[Graph Operations](Graph-Operations)** - The CQRS operation surface: backups, subgraphs, materialize
- **[Querying the Analytical Graph](Querying-the-Analytical-Graph)** - Ad-hoc Cypher over the analytical (OLAP) graph
- **[Credits & Billing](Credits-and-Billing)** - The credit model: only AI operations consume credits
- **[AI Operators & MCP](AI-Operators-and-MCP)** - The MCP surface and the three retrieval planes
- **[Pipeline Guide](Pipeline-Guide)** - Data pipelines, Dagster architecture, and custom adapters
- **[Building Custom Integrations](Building-Custom-Integrations)** - The supported customization route

## Extensions Layer

RoboLedger and RoboInvestor — the graph-scoped product surfaces.

- **[Extensions Surface Overview](Extensions-Surface-Overview)** - The URL topology, three sub-surfaces, and feature flags
- **[GraphQL Reads](GraphQL-Reads)** - Ad-hoc typed reads over the operational (OLTP) graph
- **[RoboLedger Operations](RoboLedger-Operations)** - Command writes and analytical views (the fact grid)
- **[RoboInvestor Operations](RoboInvestor-Operations)** - Portfolios, securities, and positions

## Content & Contribution Fabric

The semantic content layer — how taxonomy, ledger, and report content is modeled and contributed.

- **[Information Blocks](Information-Blocks)** - The unifying envelope that ties everything together
- **[Taxonomy & Frameworks](Taxonomy-and-Frameworks)** - The rs-gaap and fac frameworks, the library, and the Taxonomy Block
- **[Event-Driven Ledger](Event-Driven-Ledger)** - REA, the three-level ledger, and the Event Block
- **[Reporting & Rendering](Reporting-and-Rendering)** - The fact grid engine, reporting styles, and views
- **[Serialization & Export](Serialization-and-Export)** - Exporting blocks to XBRL and JSON-LD

## Documents & Search

Unstructured content and retrieval — the institutional-knowledge layer that grounds AI.

- **[Search & AI Retrieval](Search-and-AI-Retrieval)** - Index documents and retrieve them through MCP tools
- **[Document Management](Document-Management)** - The document store over entity graphs
- **[File Uploads](File-Uploads)** - File uploads for generic graphs

## Self-Hosting & Development

RoboSystems is open source: run the whole platform on your machine, contribute to it, or deploy it into your own AWS account.

- **[Local Development](Local-Development)** - From `just start` to your first authenticated query in ten minutes, and how a local stack differs from hosted
- **[Self-Hosted AI Models](Self-Hosted-AI-Models)** - Run the operators on open-weight models through any OpenAI-compatible endpoint
- **[Windows Setup (WSL2)](Windows-Setup-with-WSL2)** - Set up WSL2 and run the stack on Windows
- **[Connecting QuickBooks Locally](Connecting-QuickBooks-Locally)** - Run real QB OAuth against a local stack via ngrok
- **[Bootstrap Guide](Bootstrap-Guide)** - Set up AWS OIDC federation and GitHub Actions to deploy your own

## Demos

Hands-on walkthroughs on a local stack — run one command, explore the result.

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
