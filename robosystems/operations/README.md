# Operations Directory

Business logic kernel for the platform. All domain logic lives here — GraphQL resolvers, named command operation routers, and MCP tools are transports that delegate to these functions.

## Architectural shapes

Every operation on the platform fits one of four shapes. Knowing
which shape an op has tells you what to expect: write semantics, audit
chain, idempotency, error envelope, transport exposure pattern.

- **Block envelopes** (`taxonomy_block/`, `information_block/`,
  `event_block/`) — molecule-level compositional writes with a
  registry that dispatches by block type. Multiple atoms validated
  and persisted atomically under one envelope. Future:
  `report_block/`.
- **Events** (handlers under `event_block/python_handlers/` plus
  tenant-configurable rules in the `event_handlers` DSL registry) —
  REA business occurrences via `create-event-block`. The discriminator
  is `event_type`; the handler dispatches accordingly. Events EXPLAIN
  the ledger: every triggered Transaction / Entry carries
  `triggered_by_event_id` back to the originating event.
- **Workflows** (kernels like `roboledger/fiscal_calendar/`,
  `graph/`, `library/`, `taxonomy/writers/`) — procedural operations
  on the books: close a period, materialize the graph, regenerate a
  report, run the rules engine. Workflows MANAGE state. Not events:
  closing a period isn't a real-world occurrence — the customers
  didn't change behavior on March 31. Workflows have their own
  `OperationEnvelope` audit log via the dispatch middleware.
- **Master Data CRUD** (kernels under `roboledger/commands/agent.py`,
  `event_handler.py`, `taxonomies.py`) — reference data writes that
  support the other shapes: counterparties, dynamic rule rows,
  per-row mapping associations, entity adoption links. Standard
  create / update / delete verbs.

The directory layout below mirrors these shapes — cross-domain Block
envelopes sit at the top level (`information_block/`,
`taxonomy_block/`, `event_block/`); domain kernels (`roboledger/`,
`roboinvestor/`) hold reads + commands + services; cross-cutting
infrastructure (`graph/`, `library/`, `providers/`, `agents/`, `aws/`)
lives at the top level too.

When designing a new op, the first question is *which shape am I
building*, not *what verb to use*. The shape tells you which directory
to write into, what envelope wraps it, and how MCP / GraphQL / REST
expose it.

## Directory Structure

```
operations/
├── information_block/         # Cross-domain Information Block registry and operations
│   ├── __init__.py            # Public API: create_information_block, get/list, REGISTRY
│   ├── registry.py            # BlockTypeRegistryEntry, REGISTRY dict (frozen at import)
│   ├── types.py               # BlockTypeRegistryEntry dataclass, ConstructionMode literal
│   ├── commands.py            # create/update/delete_information_block (generic dispatch)
│   ├── reads.py               # get/list_information_blocks (envelope reads)
│   ├── envelope.py            # Cross-type atom → Lite projection helpers
│   ├── schedule.py            # Schedule block type handler (declarative construction)
│   ├── statement.py           # Statement block type handlers (compositional, stub)
│   ├── metric.py              # Metric block type handler (derivative, stub)
│   ├── classify.py            # Association classifier scaffold (Phase δ.3)
│   └── rules/                 # Rule evaluation engine
│       ├── engine.py          # evaluate_rules_for_structure — entry point
│       ├── evaluators.py      # Per-pattern dispatch (EqualTo, RollUp, Exists, CoExists, …)
│       ├── expressions.py     # Safe AST parser — no eval(); $Variable → _var_Name
│       └── commands.py        # cmd_evaluate_rules (mounted as evaluate-rules operation)
├── roboledger/                # RoboLedger domain kernel (CQRS subtree)
│   ├── reads/                 # Pure reads: session + args → Pydantic response
│   │   └── accounts.py, entity.py, fiscal_calendar.py, reports.py, schedules.py, ...
│   ├── commands/              # Pure writes: session + request → Pydantic response
│   │   └── fiscal_calendar.py, reports.py, schedules.py, taxonomies.py, ...
│   ├── fiscal_calendar/       # PeriodCloseService, FiscalCalendarService
│   ├── reports/               # fact_grid, guard_rails
│   ├── schedules/             # ScheduleService
│   └── views/                 # Graph-backed analytical queries (build-fact-grid)
├── roboinvestor/              # RoboInvestor domain kernel (CQRS subtree)
│   ├── reads/                 # portfolios, securities, positions, holdings
│   └── commands/              # portfolios, securities, positions
├── agents/                    # AI agent system (CloseAgent, MappingAgent, CypherAgent)
├── graph/                     # Platform graph database operations
│   ├── graph_creation_service.py        # Unified graph creation (entity + generic)
│   ├── subscription_service.py          # Graph subscription management
│   ├── credit_service.py                # Credit-based billing and consumption tracking
│   ├── pricing_service.py               # Pricing calculations
│   ├── metrics_service.py               # Analytics and performance metrics
│   └── repository_subscription_service.py
├── lbug/                      # LadybugDB infrastructure operations
│   ├── backup_manager.py      # Database backup and restore
│   └── ingest.py              # S3-based bulk data ingestion
├── providers/                 # External provider integrations
│   └── registry.py            # Provider registry and management
├── connection_service.py      # External service connection lifecycle
└── user_limits_service.py     # User quota enforcement
```

## Single-Source-of-Truth Contract

The domain kernels (`roboledger/`, `roboinvestor/`, `information_block/`) are the only place domain logic lives. Three transports call the same functions:

| Transport | Reads | Writes |
|-----------|-------|--------|
| GraphQL (`/extensions/{g}/graphql`) | `resolvers/*.py` → ops `reads/*.py` | — |
| Named command ops (`/extensions/{domain}/{g}/operations/{op}`) | — | ops `commands/*.py` |
| MCP tools (`middleware/mcp/tools/*.py`) | same ops `reads/*.py` | same ops `commands/*.py` |

Adding business logic in a router, resolver, or MCP tool file is a mistake — route it through the ops layer.

## Extension Domain Kernels (CQRS Pattern)

`reads/*.py` and `commands/*.py` follow a strict contract:

- **Input**: an already-open extensions DB session (`session: Session`) — caller opens via `extensions_session(graph_id)`
- **Output**: Pydantic response model (or `None`) — never HTTP errors
- **Errors**: domain exceptions (`PeriodNotFoundError`, `ScheduleNotFoundError`, etc.) — caller translates for its transport (HTTP 404 for REST, typed GraphQL error code, structured MCP error)

```python
# reads/fiscal_calendar.py
def get_fiscal_calendar(session: Session) -> FiscalCalendarResponse | None:
    ...  # query, return Pydantic model

# commands/fiscal_calendar.py
def close_period(session: Session, body: ClosePeriodRequest) -> ClosePeriodResponse:
    ...  # validate, mutate, return Pydantic model
```

## Information Block Registry

`information_block/` is the cross-domain home for the Information Block subsystem. It does not live under `roboledger/` because block types span domains — statements come from report ingestion, schedules come from ledger close workflows, metrics derive from other blocks.

The registry (`registry.py`) maps `block_type` strings to `BlockTypeRegistryEntry` objects. Each entry holds display metadata, the typed mechanics schema, create/update/delete request models, and four dispatch handlers. Adding a block type is a code change — no DB rows, no runtime registration.

**Registered block types:**

| block_type | Construction mode | Handler | surfaces_in_library |
|------------|------------------|---------|---------------------|
| `schedule` | declarative | `schedule.py` | No (tenant-only) |
| `balance_sheet` | compositional | `statement.py` | Yes |
| `income_statement` | compositional | `statement.py` | Yes |
| `cash_flow_statement` | compositional | `statement.py` | Yes |
| `equity_statement` | compositional | `statement.py` | Yes |
| `metric` | derivative | `metric.py` | No |

Statement and metric dispatch handlers currently raise `NotImplementedError` (→ HTTP 501) — their data models are wired, but the construction logic ships in later phases.

## Rule Evaluation Engine

`information_block/rules/` implements the Phase δ.3 rule evaluation engine:

- **`engine.py`** — entry point: loads rules scoped to a structure (via `envelope.load_rules_for_structure`), binds `$Variable` references to in-scope facts via qname lookup, dispatches per-pattern, writes `VerificationResult` rows, calls `session.flush()` before returning
- **`evaluators.py`** — pattern dispatchers: `EqualTo`/`RollUp`/`RollForward` (arithmetic equality with configurable tolerance), `Exists` (fact presence), `CoExists` (all-or-nothing binding); other patterns return `skipped`
- **`expressions.py`** — safe AST parser: `$Variable` → `_var_Name` substitution + whitelist node walk; `eval()` is never called; expression tree is evaluated recursively over `ast.BinOp` / `ast.UnaryOp` nodes

## Platform Services

`graph/`, `lbug/`, `providers/`, and the top-level modules are platform infrastructure services. These predate the extension domain kernels and follow a different pattern (async service classes rather than session-in/pydantic-out functions). They handle graph lifecycle (creation, subscriptions, credits, tiers), LadybugDB backup/ingestion, and external provider management.
