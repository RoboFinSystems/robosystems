# Operations

The business logic kernel. **All domain logic lives here.** GraphQL resolvers, REST operation routers, MCP tools, and AI Operators are transports that delegate to these functions.

## The single-source-of-truth contract

| Transport | Reads | Writes |
| --------- | ----- | ------ |
| GraphQL (`POST /extensions/{g}/graphql`) | `graphql/resolvers/*.py` → ops `reads/*.py` | — |
| Named command ops (`POST /extensions/{domain}/{g}/operations/{op}`) | — | ops `commands/*.py` |
| Analytical view ops (`POST /extensions/{domain}/{g}/operations/{view}`) | `routers/extensions/{domain}/views.py` → ops `views/*.py` | — |
| MCP tools (`middleware/mcp/tools/*.py`) | the same `reads/*.py` and `views/*.py` | the same `commands/*.py` |
| AI Operators (`operators/`) | via MCP tools (`ctx.tools`) | via MCP tools |

**Putting business logic in a router, resolver, view handler, MCP tool, or Operator is the mistake this directory exists to prevent.** If a transport needs new behavior, add it to the ops layer and call it from there — that is what keeps every surface agreeing by construction.

## Four operation shapes

Every operation on the platform is one of four shapes, and the shape determines write semantics, audit chain, idempotency, error envelope, and how transports expose it. When designing a new operation, the first question is *which shape is this* — not *what verb do I use*.

- **Block envelopes** (`information_block/`, `taxonomy_block/`, `event_block/`) — molecule-level compositional writes, dispatched through a registry keyed by block type. Multiple atoms validated and persisted atomically under one envelope.
- **Events** (`event_block/python_handlers/` plus tenant-configurable rules in the `event_handlers` DSL registry) — REA business occurrences created via `create-event-block`, discriminated on `event_type`. Events *explain* the ledger: every triggered Transaction and Entry carries `triggered_by_event_id` back to the originating event.
- **Workflows** (`roboledger/fiscal_calendar/`, `graph/`, `library/`, `taxonomy_block/`) — procedural operations on the books: close a period, materialize the graph, regenerate a report, run the rules engine. Workflows *manage* state. Closing a period is not an event — the customers didn't change behavior on March 31. Workflows get an `OperationEnvelope` audit log from the dispatch middleware.
- **Master data CRUD** (`roboledger/commands/agent.py`, `event_handler.py`, `taxonomies.py`) — reference data that supports the other shapes: counterparties, dynamic rule rows, mapping associations, entity adoption links.

## Layout

| Path | Contents |
| ---- | -------- |
| `information_block/` | Cross-domain Information Block registry, handlers, and the rule engine |
| `taxonomy_block/` | Cross-domain Taxonomy Block (CoA, custom ontology, library authoring) |
| `event_block/` | Cross-domain Event Block registry, engine, and Python handlers |
| `roboledger/` | RoboLedger domain kernel — `reads/`, `commands/`, `views/`, plus `fiscal_calendar/`, `reports/`, `schedules/`, `fact_set.py` |
| `roboinvestor/` | RoboInvestor domain kernel — `reads/`, `commands/` |
| `operators/` | AI Operator system (`CypherOperator`, `MappingOperator`) — see [its README](operators/README.md) |
| `graph/` | Platform graph lifecycle: creation, subscriptions, credits, pricing, tiers, subgraphs, deprovisioning, storage reclaim, backup/ingestion under `engine/`, worker tasks under `tasks/` |
| `extensions/` | OLTP→OLAP materialization (`materialize.py`, `loader.py`, `staleness.py`) |
| `billing/` | Subscription billing lifecycle shared by routers and off-boarding |
| `admin/` | Support-plane actions with no self-serve surface: guarded account deletion, account activate/deactivate, SCIM tenant bootstrap |
| `library/` | Taxonomy/framework library reads (shared and tenant) |
| `search/` | Document search — client, embeddings, markdown parsing |
| `memory/` | Per-graph semantic memory service |
| `serialization/` | Block serialization (bundle, flavors, RDF) |
| `providers/` | External provider integrations and the provider registry |
| `aws/` | S3 and SES helpers |
| `connection_service.py`, `document_service.py` | Connection and document lifecycle |
| `oidc.py` | OIDC login kernel: connection config, flow state, ID-token validation, link-only user resolution |
| `user_provisioning.py` | Account-creation kernel shared by registration and IdP-driven (SCIM) provisioning |

Cross-domain block envelopes sit at the top level; domain kernels hold reads, commands, and services; cross-cutting infrastructure is top level too.

## Domain kernels (CQRS)

`reads/*.py` and `commands/*.py` follow a strict contract:

- **Input**: an already-open extensions session (`session: Session`). The caller opens it via `extensions_session(graph_id)`.
- **Output**: a Pydantic response model, or `None`. Never an HTTP error.
- **Errors**: domain exceptions (`PeriodNotFoundError`, `ScheduleNotFoundError`, …). Each transport translates them — HTTP 404 for REST, a typed GraphQL error code, a structured MCP error.

```python
# reads/fiscal_calendar.py
def get_fiscal_calendar(session: Session) -> FiscalCalendarResponse | None:
    ...  # query, return Pydantic model

# commands/fiscal_calendar.py
def close_period(session: Session, body: ClosePeriodRequest) -> ClosePeriodResponse:
    ...  # validate, mutate, return Pydantic model
```

## Information Block registry

`information_block/` is cross-domain rather than nested under `roboledger/` because block types span domains: statements come from report ingestion, schedules from ledger close workflows, metrics derive from other blocks.

`registry.py` maps each `block_type` string to a `BlockTypeRegistryEntry` holding display metadata, the typed mechanics schema, create/update/delete request models, and the dispatch handlers. Adding a block type is a code change — no database rows, no runtime registration.

| `block_type` | Construction mode | Handler | Surfaces in library |
| ------------ | ----------------- | ------- | ------------------- |
| `schedule` | declarative | `schedule.py` | No |
| `rollforward` | declarative | `rollforward.py` | No |
| `forecast` | declarative | `forecast.py` | No |
| `balance_sheet` | compositional | `statement.py` | Yes |
| `income_statement` | compositional | `statement.py` | Yes |
| `cash_flow_statement` | compositional | `statement.py` | Yes |
| `equity_statement` | compositional | `statement.py` | Yes |
| `comprehensive_income` | compositional | `statement.py` | Yes |
| `regulatory_disclosure` | compositional | `disclosure.py` | No |
| `metric` | derivative | `metric.py` | No |

Not every block type is authored through `create-information-block`. Statements are produced by `create-report`; disclosure structures are authored as vocabulary via `create-taxonomy-block`; metrics are written by `compute-metrics` / `assert-metrics`. Those types install not-implemented create/update/delete handlers (HTTP 501) while their `build_envelope` paths are fully wired and serve read envelopes normally.

Supporting modules: `envelope.py` (shared ORM→Lite projectors), `metrics.py` (the metric write paths), `chart.py` (chart View projection), `text_block.py` (narrative disclosure envelopes), and `forecast_compute.py` / `forecast_articulation.py` / `forecast_history.py` (the forecast derivation engine). See the [Information Block README](information_block/README.md) for the subsystem in detail.

## Rule evaluation engine

`information_block/rules/`:

- **`engine.py`** — entry point. Loads rules scoped to a structure via `envelope.load_rules_for_structure`, binds `$Variable` references to in-scope facts by qname, dispatches per pattern, writes `VerificationResult` rows, and calls `session.flush()` before returning. The caller owns the commit.
- **`evaluators.py`** — per-pattern dispatch: `EqualTo` and `RollForward` (strict arithmetic equality with configurable tolerance), `RollUp` (`$Parent = Σ children`), `Exists`, `CoExists`, `SumEquals`. Any other pattern returns `skipped`.
- **`expressions.py`** — a safe AST parser. `$Variable` becomes `_var_Name`, the expression is parsed with `ast.parse(mode='eval')` and walked through a whitelist, then evaluated recursively over the tree. **`eval()` is never called.**

## Platform services

`graph/`, `providers/`, `aws/`, and the top-level service modules are platform infrastructure. They predate the domain kernels and use async service classes rather than the session-in / Pydantic-out function contract. They cover graph lifecycle (creation, subscriptions, credits, tiers), LadybugDB backup and ingestion (under `graph/engine/`), and external provider management.
