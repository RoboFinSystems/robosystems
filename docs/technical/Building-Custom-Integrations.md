# Building Custom Integrations

The supported way to connect your own data sources to RoboSystems is to build an **integration**: a small program in its own repository that speaks to the platform exclusively through the public API with an API key. It never runs inside the platform — so it survives every platform release, works identically against the managed cloud or a self-hosted deployment, holds its own source credentials, and runs anywhere: a GitHub Actions schedule, a cron job, a container.

**Start here:**

- [`robosystems-integration-template`](https://github.com/RoboFinSystems/robosystems-integration-template) — the scaffold. Click *Use this template*, wire your extract/transform, pick your lane's emitter, set two repo secrets, and it runs on a schedule with zero infrastructure.
- [`robosystems-marketing-integration`](https://github.com/RoboFinSystems/robosystems-marketing-integration) — a worked example built from the template: RFS's own marketing/usage metrics (GitHub, npm, PyPI, Docker Hub) modelled as a custom vocabulary and emitted as a monthly metric series.
- SDKs: [`robosystems-client`](https://pypi.org/project/robosystems-client/) (Python) and [`@robosystems/client`](https://www.npmjs.com/package/@robosystems/client) (TypeScript), regenerated from the live [OpenAPI spec](https://api.robosystems.ai/openapi.json).

## The three lanes

Pick the lane that matches your data's nature (an integration can use more than one):

| Lane | What you send | What the platform enforces |
|---|---|---|
| **Ledger** | Business events via `create-event-block` | Double-entry balance, capture-then-approve, closed-period gate, `(source, external_id)` idempotency |
| **Semantic facts** | A custom vocabulary (`create-taxonomy-block`) + observed metric series (`assert-metrics`) | Typed concepts, presentation structure, replace-per-period, provenance |
| **Raw graph** | Parquet/CSV → staging → `materialize` | Per-graph schema, bulk ingestion pipeline |

Lane 1 sources are registered as a connection (`provider: "external"`, claiming a `source_name`), so every event traces to a named integration and source names can't collide. See [Event-Driven Ledger](Event-Driven-Ledger), [Taxonomy & Frameworks](Taxonomy-and-Frameworks), and [File Uploads](File-Uploads) for the mechanics behind each lane.

## Why integrations live outside the core

Platform-operated deployments run an **unmodified core** — that's what keeps every deployment identical, auditable, and safe to upgrade, and it's why the in-core adapter registry (`robosystems/adapters/`) is maintained exclusively by the platform team. Custom code never enters it. The public API is deliberately rich enough that it doesn't need to: the platform's own QuickBooks adapter writes through the same event envelope an external integration would use.

Platform-built adapters keep expanding — both connection adapters (QuickBooks-class) and shared-repository adapters (SEC-class). If there's a source you'd like the platform to support natively, open a [discussion](https://github.com/orgs/RoboFinSystems/discussions).

## Self-hosted forks and `custom_*`

If you fork the open-source core and operate your **own** deployment in your **own** infrastructure, the `custom_*` adapter namespace remains available as a merge boundary for in-core additions (see the [Adapters README](https://github.com/RoboFinSystems/robosystems/blob/main/robosystems/adapters/README.md)). That pattern applies only to deployments you run yourself — it is not supported on platform-operated deployments, where the integration route above is the way to connect custom sources. For most cases the integration route is the better choice on a self-hosted deployment too: it's release-proof and portable between deployment modes.
