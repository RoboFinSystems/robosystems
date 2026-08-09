# API Models

Pydantic request and response models for the REST and GraphQL surfaces. Every model here is centralized rather than declared inline in a router, so one endpoint's contract can be reused by another and reviewed in one place.

## These models are a public contract

The models in this directory generate the OpenAPI spec served at `/openapi.json`, which in turn generates the published **`robosystems-python-client`** and **`robosystems-typescript-client`** SDKs. They also back the Strawberry GraphQL types (see below).

That makes any change to a field name, type, or nullability a client-facing change. What it costs depends on which SDK tier it reaches:

- **Additive changes are cheap.** New optional fields and new models ride a client minor.
- **Renames, removals, and semantic changes reaching the SDK's stable tier are breaking.** That tier is the SDK facades plus the symbols `robosystems-integration-template` imports for its emit path. Those propagate as an SDK major and need a deprecation cycle — call them out explicitly in the PR so the regeneration lands as a coordinated release rather than silent drift.
- **The same edits elsewhere ride a minor.** Most models here back the generated SDK tier, which tracks this surface and moves with it. Removing a field or model that no facade and no template emitter consumes is a client minor with the removal named in the release notes, not a major.
- **Dead surface skips the deprecation cycle.** A field or model that never functioned, has no consumer, and whose removal changes only symbol resolution can go in a minor. Record those three facts in the PR.

Prefer widening a model over replacing it.

## Layout

| Path | Contents |
| ---- | -------- |
| `auth.py` | Login, registration, JWT tokens, SSO flows |
| `oauth.py` | OAuth provider integrations (QuickBooks, etc.) |
| `common.py` | Error responses, pagination, health checks |
| `user.py`, `orgs.py` | User profiles, API keys, usage analytics, organizations |
| `entity_graph.py` | Entity graph operation models |
| `information_block.py` | InformationBlock envelope (Structure + atoms + FactSet) |
| `fact_provenance.py` | Typed `FactProvenance` discriminated union |
| `event_block.py`, `event_handler.py` | Event Block envelope (REA business events) and handler DSL rules |
| `taxonomy_block.py` | Taxonomy Block envelope (CoA, custom ontology) |
| `library.py` | Taxonomy/framework library models |
| `search.py`, `memory.py` | Document search and per-graph semantic memory |
| `admin/` | Admin API models (cache, credits, graphs, invoice, orgs, subscription, users) |
| `billing/` | Checkout, credits, customer, invoice, offering, subscription |
| `graphs/` | Graph platform models (core, backups, connections, health, limits, mcp, members, metrics, operations, operator, query, schema, subgraphs, tables, tier) |
| `views/` | Analytical view models (fact_grid, view_config, view_response) |
| `extensions/` | RoboLedger and RoboInvestor request/response models |

### `fact_provenance.py`

`FactProvenance` is a discriminated union tagged on `origin`. Each arm records how a fact came to exist: `pivot`, `schedule`, `derived`, `asserted`, `document`, `forecast`, `filed`. Adding an arm means adding a class with a new `origin` literal and extending the union — Pydantic dispatches on the tag.

### `extensions/`

Per-graph request/response models for the two product domains: entity, accounts, account rollups, AR/AP, closing book, fiscal calendar, forecasts, investor, journal entries, publish lists, reports, report packages, rollforward, schedules, summary, taxonomies, text blocks, transactions, trial balance.

These are also the models the GraphQL read surface derives Strawberry types from — see the [GraphQL README](../../graphql/README.md). A field added to a Pydantic response model here appears on both the REST response and the GraphQL type with no further edit.

## Where the layers sit

| Layer | Responsibility |
| ----- | -------------- |
| `models/api/` (here) | Pydantic request/response wire shapes |
| `routers/` | HTTP handling — auth, status codes, envelope |
| `operations/` | Business logic (the single source of truth) |
| `models/core/` | Platform SQLAlchemy entities (users, orgs, graphs, billing) |
| `models/extensions/` | Extensions OLTP SQLAlchemy entities, schema-per-graph tenancy |

## Usage

```python
from robosystems.models.api.information_block import InformationBlockEnvelope

@router.get("/block", response_model=InformationBlockEnvelope)
async def get_block(...) -> InformationBlockEnvelope:
    ...
```

## Adding a model

1. Put it in the file that matches its domain — check first whether an existing model already covers the shape.
2. Export it from the package `__init__.py`.
3. Give every field a `Field(..., description=...)`; the description becomes the OpenAPI documentation and the SDK docstring.
4. Follow the naming convention: `*Request`, `*Response`, `*Info`, `*Summary`.

## Related

- [Platform SQLAlchemy Models](../core/README.md)
- [Extensions SQLAlchemy Models](../extensions/README.md)
- [Extensions GraphQL](../../graphql/README.md)
- [Operations Layer](../../operations/README.md)
