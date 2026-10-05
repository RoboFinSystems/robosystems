# Building Custom Integrations

An **integration** is the supported way to connect your own data sources to RoboSystems: a small program in its own repository that calls the public API. It speaks to the platform only through that API, with an API key, and never runs inside the platform — so it survives every platform release, works identically against the managed cloud or a self-hosted deployment, holds its own source credentials, and runs anywhere: a GitHub Actions schedule, a cron job, a container.

This page is the hub for integrators: pick a lane, follow its guide, and use the reference pages for the contract every call shares.

## Table of Contents

- [Start Here](#start-here)
- [The three lanes](#the-three-lanes)
- [The Contract Every Integration Relies On](#the-contract-every-integration-relies-on)
- [How an Integration Is Shaped](#how-an-integration-is-shaped)
- [Why integrations live outside the core](#why-integrations-live-outside-the-core)
- [Self-hosted forks and `custom_*`](#self-hosted-forks-and-custom_)
- [Related Documentation](#related-documentation)
- [Support](#support)

## Start Here

- [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template) — the scaffold. Click *Use this template*, write your extract and transform, wire your lane's emitter, and set the API key as a repository secret (with the graph id and source name as repository variables); the included GitHub Actions workflow runs it on a schedule with zero infrastructure.
- [Build a Ledger Integration](Build-a-Ledger-Integration) — the end-to-end tutorial: posting business events from an external system into a RoboLedger graph, in `curl` and Python.
- [`robosystems-marketing-integration`](https://github.com/RoboFinSystems/robosystems-marketing-integration) — a worked example built from the template: RFS's own marketing and usage metrics (GitHub, npm, PyPI, Docker Hub) modelled as a custom vocabulary and emitted as a monthly metric series.
- SDKs: [`robosystems-client`](https://pypi.org/project/robosystems-client/) (Python) and [`@robosystems/client`](https://www.npmjs.com/package/@robosystems/client) (TypeScript), generated from the live [OpenAPI spec](https://api.robosystems.ai/openapi.json).

## The three lanes

Pick the lane that matches your data's nature (an integration can use more than one):

| Lane | What you send | What the platform enforces | Guide |
|---|---|---|---|
| **1 — Ledger** | Business events via `create-event-block` | Double-entry balance, capture-then-approve, closed-period gate, `(source, external_id)` uniqueness | [Build a Ledger Integration](Build-a-Ledger-Integration) |
| **2 — Semantic facts** | A custom vocabulary (`create-taxonomy-block`) + observed metric series (`assert-metrics`) | Typed concepts, presentation structure, replace-per-period, provenance | [Build a Ledger Integration § Lane 2](Build-a-Ledger-Integration#lane-2-metric-series-instead-of-ledger-events), then [Information Blocks](Information-Blocks) and [Taxonomy & Frameworks](Taxonomy-and-Frameworks) |
| **3 — Raw graph** | Parquet/CSV → staging → `materialize` | Per-graph schema, bulk ingestion pipeline | [File Uploads](File-Uploads) and [Custom Graph Schema](Custom-Graph-Schema) |

- **Lane 1** is for data that *is* accounting: invoices, payments, payroll from a system the platform has no adapter for. Events land in an inbox as `captured`, a person approves them, and the event's handler writes the journal entries. Lane 1 sources are registered as a connection (`provider: "external"`, claiming a `source_name`), so every event traces to a named integration and source names can't collide. The mechanics are in [Event-Driven Ledger](Event-Driven-Ledger).
- **Lane 2** is for data that is *facts but not bookkeeping*: usage numbers, operational KPIs, marketing counts. Author your vocabulary once, then assert observed values each period; the series then renders in fact grids, GraphQL, and MCP with no further work.
- **Lane 3** is for graph-shaped domain data: upload files, stage them, materialize. Combined with the per-graph schema operations you can stand up a complete custom knowledge graph.

The template ships one emitter per lane: `emit/events.py`, `emit/metrics.py`, and `emit/graph.py`.

## The Contract Every Integration Relies On

Three reference pages cover what every call has in common. Read them once before putting an integration on a schedule:

- **[Operations Contract](Operations-Contract)** — the `OperationEnvelope` every write returns, sync versus async, the `Idempotency-Key` header that makes retries safe, and following a long-running operation over Server-Sent Events.
- **[Errors and Rate Limits](Errors-and-Rate-Limits)** — the error body, what each status code means and whether to retry it, the rate-limit headers, and how to quote a `request_id` when something goes wrong.
- **[Versioning and Compatibility](Versioning-and-Compatibility)** — what the API and the SDKs promise to keep stable, which SDK symbols are frozen for a major version, how to pin, and where release notes live.

## How an Integration Is Shaped

The template's layout reflects a deliberate split of responsibility:

- **You own extract and transform** (`collect.py`, `transform.py`) — they are specific to your source.
- **The platform owns validation and load** — the operations behind the API check every record: balance, periods, vocabulary, duplicates.

A few conventions keep an integration dependable:

- **One source name per integration.** It is the identity stamped on everything the integration writes (`source` on lane-1 events, `source_system` on lane-2 assertions).
- **Stable ids from the source.** Lane-1 events carry the source system's own id as `external_id`; the graph accepts one event per `(source, external_id)`, so a re-run cannot double-post.
- **Keep your raw history.** Store your source pulls in your own storage and send the platform the shaped records. A backfill is then one loop over history.
- **Idempotency-Key on every write** you might retry, and backoff on 429 and 5xx — see [Errors and Rate Limits](Errors-and-Rate-Limits#retry-policy).
- **Secrets in the runtime's secret store**, never in the repository: the API key, and your source system's credentials, which the platform never holds.

The template's SDK pin covers exactly what its emitters import. When you call an operation beyond them, read [Versioning and Compatibility](Versioning-and-Compatibility#pinning) for how to pin for it.

## Why integrations live outside the core

Platform-operated deployments run an **unmodified core** — that's what keeps every deployment identical, auditable, and safe to upgrade, and it's why the in-core adapter registry (`robosystems/adapters/`) is maintained exclusively by the platform team. Custom code never enters it. The public API is deliberately rich enough that it doesn't need to: the platform's own QuickBooks adapter writes through the same event envelope an external integration would use.

Platform-built adapters keep expanding — both connection adapters (QuickBooks-class) and shared-repository adapters (SEC-class). If there's a source you'd like the platform to support natively, open a [discussion](https://github.com/orgs/RoboFinSystems/discussions).

## Self-hosted forks and `custom_*`

If you fork the open-source core and operate your **own** deployment in your **own** infrastructure, the `custom_*` adapter namespace remains available as a merge boundary for in-core additions (see the [Adapters README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md)). That pattern applies only to deployments you run yourself — it is not supported on platform-operated deployments, where the integration route above is the way to connect custom sources. For most cases the integration route is the better choice on a self-hosted deployment too: it's release-proof and portable between deployment modes. An integration targets a self-hosted deployment by setting `ROBOSYSTEMS_API_URL` to it.

## Related Documentation

**Wiki Guides:**

- **[Build a Ledger Integration](Build-a-Ledger-Integration)** - Lane 1, end to end
- **[Operations Contract](Operations-Contract)** - Envelope, idempotency, and progress streaming
- **[Errors and Rate Limits](Errors-and-Rate-Limits)** - Status codes, retries, and rate-limit headers
- **[Versioning and Compatibility](Versioning-and-Compatibility)** - The API and SDK stability contract
- **[Event-Driven Ledger](Event-Driven-Ledger)** - The event and handler model behind lane 1
- **[File Uploads](File-Uploads)** - The staging path behind lane 3
- **[Authentication and API Keys](Authentication-and-API-Keys)** - Creating and scoping the key an integration uses

**Repositories:**

- **[robosystems-integration-template](https://github.com/RoboFinSystems/robosystems-integration-template)** - The scaffold
- **[robosystems-marketing-integration](https://github.com/RoboFinSystems/robosystems-marketing-integration)** - A worked lane-2 example

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
