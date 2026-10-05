# Versioning and Compatibility

What stays stable as RoboSystems evolves, and where to read about what changed. There are two contracts to know: the HTTP API under `/v1`, and the two client SDKs generated from it.

## Table of Contents

- [The HTTP API](#the-http-api)
- [The Client SDKs](#the-client-sdks)
- [The Two-Tier SDK Contract](#the-two-tier-sdk-contract)
- [Pinning](#pinning)
- [Where Release Notes Live](#where-release-notes-live)
- [Related Documentation](#related-documentation)
- [Support](#support)

## The HTTP API

The platform API is served under `/v1` (`https://api.robosystems.ai/v1/...`); the extensions surface is served under `/extensions/...` beside it. The live OpenAPI specification is always at [`https://api.robosystems.ai/openapi.json`](https://api.robosystems.ai/openapi.json), and the [API reference](https://robosystems.ai/docs/api) is rendered from it.

The API evolves **additively**. New operations, new endpoints, new optional request fields, and new response fields are added without a path change. Write clients that tolerate that:

- Ignore response fields you do not recognise.
- Do not rely on the order of keys or of unsorted lists.
- Treat enumerated string values (statuses, categories) as open — handle an unknown value rather than crash on it.
- Branch on status codes and typed error codes, not on the wording of `detail` messages (see [Errors and Rate Limits](Errors-and-Rate-Limits)).

Changes that are not additive — a field or endpoint removed or renamed — are named in the platform's release notes. The per-graph GraphQL schema follows the same rule and is introspectable, so a client can check for a field before relying on it (see [GraphQL Reads](GraphQL-Reads)).

## The Client SDKs

Two SDKs are generated from the OpenAPI specification:

| Language | Package | Install |
| --- | --- | --- |
| Python | [`robosystems-client`](https://pypi.org/project/robosystems-client/) on PyPI (import as `robosystems_client`) | `pip install robosystems-client` / `uv add robosystems-client` |
| TypeScript / JavaScript | [`@robosystems/client`](https://www.npmjs.com/package/@robosystems/client) on npm | `npm install @robosystems/client` |

Source: [robosystems-python-client](https://github.com/RoboFinSystems/robosystems-python-client) and [robosystems-typescript-client](https://github.com/RoboFinSystems/robosystems-typescript-client).

The current major for both is **2** — 2.0.0 shipped on 2026-09-20 — and the integration template pins `robosystems-client>=2,<3`.

Beside the OpenAPI-generated operations, both SDKs carry a typed GraphQL client for the extensions reads, generated from the per-graph GraphQL schema rather than from OpenAPI. You reach it through the facade clients (`LedgerClient`, `InvestorClient`, and the rest), which return typed results rather than raw JSON.

The SDKs are regenerated as the API changes, so an operation can reach the API shortly before it reaches a released SDK. Until then it is callable through the SDK's authenticated HTTP client directly (the [integration template](https://github.com/RoboFinSystems/robosystems-integration-template)'s `raw_operation` is an example).

## The Two-Tier SDK Contract

Both SDKs follow semantic versioning, with the surface split into two tiers that carry different promises.

### Stable tier — breaks only on a major release

The stable tier is what integrations are built on. Breaking any of it costs the SDK a **major** version, after a deprecation cycle.

- **Python** (`robosystems_client`): the facade clients (`robosystems_client.clients`), the root exports, the error classes, the auth configuration, and every symbol the [integration template](https://github.com/RoboFinSystems/robosystems-integration-template) imports — `AuthenticatedClient`, `types.UNSET`, the six emit operations `create_file_upload`, `ingest_file`, `materialize`, `create_event_block`, `assert_metrics`, `create_taxonomy_block`, and their request models (`FileUploadRequest`, `IngestFileOp`, `MaterializeOp`, `CreateEventBlockRequest`, `AssertMetricsRequest`, `CreateTaxonomyBlockRequest`).
- **TypeScript** (`@robosystems/client`): the facade clients and their subpath exports (`/clients`, `/ledger`, `/investor`, `/library`, `/query`, `/operations`, `/client`), the React hooks, the error classes, the auth configuration, and the types those signatures expose through `/types`.

The integration template's emitters define the Python stable set: the way an operation joins the stable tier is by being used there. The template's `src/integration/emit/*.py` is the authoritative list of operations, and `src/integration/client.py` is where it imports `AuthenticatedClient`.

### Generated tier — tracks the API, moves on a minor release

Everything else the OpenAPI specification produces — the rest of `robosystems_client.api.*` and `robosystems_client.models.*` in Python, and the `/sdk` export in TypeScript — tracks the API surface. Operations there can be added, renamed, or removed on a **minor** release, and every removal is named in that release's notes.

One detail matters for the generated tier: in the Python SDK, an operation's module path comes from its tag in the OpenAPI specification (for example `robosystems_client.api.robo_ledger_ledger_events.create_event_block`). A generated function can therefore move between modules when the API regroups its operations. A regroup that moves only generated-tier operations rides a minor release; one that moves a stable-tier operation — any of the six emit operations — is a major, which is what 2.0.0 was. Import from the stable set where you can.

## Pinning

- **Building on the stable tier** (the facades, or the integration template's emit path): pin to the current major — `robosystems-client>=2,<3` in Python or `^2` in npm today, or in general `>=N,<N+1` / `^N` for the major you tested against. The integration template's `pyproject.toml` shows the pin it uses.
- **Depending on a generated-tier operation outside the stable set**: pin a minor range (`>=N.M,<N.M+1`, or `~N.M` in npm) and read the release notes before widening it. If you want that operation frozen, ask for it to be promoted into the stable set by opening an issue on the client repository.

## Where Release Notes Live

Every release is published on GitHub with its notes:

- **Platform API**: [github.com/RoboFinSystems/robosystems/releases](https://github.com/RoboFinSystems/robosystems/releases)
- **Python SDK**: [github.com/RoboFinSystems/robosystems-python-client/releases](https://github.com/RoboFinSystems/robosystems-python-client/releases)
- **TypeScript SDK**: [github.com/RoboFinSystems/robosystems-typescript-client/releases](https://github.com/RoboFinSystems/robosystems-typescript-client/releases)

Watch the repositories for releases (GitHub **Watch → Custom → Releases**) to be notified. SDK releases name every generated-tier removal and describe any major's migration.

## Related Documentation

**Wiki Guides:**

- **[Building Custom Integrations](Building-Custom-Integrations)** - The integration template and the three lanes
- **[Operations Contract](Operations-Contract)** - The envelope and retry contract shared by every write
- **[Errors and Rate Limits](Errors-and-Rate-Limits)** - Status codes and typed error codes to branch on

**API Reference:**

- **[API reference](https://robosystems.ai/docs/api)** - Rendered from the live OpenAPI specification

## Support

- [Issues](https://github.com/RoboFinSystems/robosystems/issues)
- [Wiki](https://github.com/RoboFinSystems/robosystems/wiki)
- [Projects](https://github.com/orgs/RoboFinSystems/projects)
- [Discussions](https://github.com/orgs/RoboFinSystems/discussions)
