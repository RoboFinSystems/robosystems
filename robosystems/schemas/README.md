# Schemas

Graph schema definitions for LadybugDB, organized as **base + extensions**. The
base schema declares the ontology every graph shares; each extension adds the
node types, relationships, and properties of one business domain. This is the
declaration that everything else is built against — graph DDL, the OLTP mirror,
API surfaces, data pipelines, and the schema AI Operators are shown when they
plan a query.

## Layout

The module splits along a declaration/runtime line, and the split is enforced:
the top level is side-effect free and touches no database.

**Declaration layer** — what the ontology *is*:

| File | Contents |
| --- | --- |
| `base.py` | `BASE_NODES` and `BASE_RELATIONSHIPS` |
| `models.py` | `Node` / `Property` / `Relationship` / `Schema` dataclasses and their `to_cypher()` |
| `loader.py` | `LadybugSchemaLoader` — composition and introspection |
| `aliases.py` | Extension-name aliases |
| `extensions/` | One module per domain: `roboledger.py`, `roboinvestor.py`, `knowledge.py` |

**Runtime layer** (`runtime/`) — things that consume the declarations:

| File | Contents |
| --- | --- |
| `manager.py` | `SchemaManager`, `SchemaConfiguration`, `SchemaCompatibility` |
| `builder.py` | `LadybugSchemaBuilder` — compose and emit Cypher DDL |
| `custom.py` | `CustomSchemaParser` / `CustomSchemaManager` for user-supplied JSON/YAML |
| `validator.py` | `LadybugSchemaValidator` — validate nodes and relationships at runtime |
| `parser.py` | Cypher DDL back to metadata |

Applying a schema — running `CREATE NODE TABLE` against a live database — is not
here. That lives in
[`graph_api/core/ladybug/manager.py`](/robosystems/graph_api/core/ladybug/README.md).
This module declares and compiles; it does not install.

## Ontology invariants

Two rules govern where a concept belongs. They are load-bearing: breaking them
produces asymmetries that force migrations against already-materialized
databases.

### 1. Base is aspirational

**Base contains concepts universally applicable to the ontology, regardless of
how many extensions use them today.**

Period, Unit, Element, Taxonomy, Dimension, Association and Structure are
declared in `base.py` even though roboledger is currently the only extension
populating most of them. That is deliberate. RoboInvestor and future RoboX
products grow into them, and "promote to base when a second consumer appears" is
a breaking schema change against live data.

When adding a concept, ask *"is this universally applicable to the ontology?"* —
not *"do two extensions use it today?"*. Waiting for a second consumer turns
every promotion into a migration.

### 2. Aspects attach only to measured events

**Period, Unit, and Dimension qualify measured observations. They never attach
to declarative nodes.**

Facts and LineItem dimensional tags carry aspect edges. Entity, Report,
Taxonomy, and Portfolio do not. Any proposed
`(Entity | Report | Taxonomy | Portfolio)_HAS_(Period | Unit | Dimension)` edge
is a category error — rewrite it as a node property, or as a query over the
underlying events.

The same conceptual type can legitimately appear in both roles. An Entity may
carry a static `reporting_currency` property (a *declaration*) while a Fact has
a `FACT_HAS_UNIT` edge to a Unit node (an *observation*). Both are correct; they
are different roles.

### Before adding a node or edge

1. Does it satisfy Invariant 1? If you cannot justify "universally applicable",
   it belongs in an extension.
2. Does it satisfy Invariant 2? If the new edge hangs an aspect off a
   declarative node, rewrite it.
3. Does the concept already exist latently? The schema often models what you
   need through existing edges.

## Base schema

| Node | Purpose | Key properties |
| --- | --- | --- |
| **Entity** | Organizations, companies, subsidiaries | identifier, cik, ticker, name, entity_type |
| **Agent** | REA counterparty (customer, vendor, employee) | identifier, agent_type, name, source |
| **Event** | REA business event with a canonical action verb | identifier, event_type, event_action, status |
| **Period** | Time periods | start_date, end_date, period_type |
| **Unit** | Measurement units | measure, value, numerator_uri |
| **Element** | XBRL taxonomy elements | qname, period_type, is_numeric |
| **Label** | Human-readable element labels | value, type, language |
| **Reference** | Authoritative element references | value, type |
| **Taxonomy** | Global XBRL taxonomies | name, version, namespace |
| **Dimension** | XBRL dimensional axis/member tags | axis, member, dimension_type |
| **Structure** | Named element collection (network) | network_uri, definition, type |
| **Association** | Element relationships (calculation, presentation) | arcrole, order_value, association_type |
| **Trait** | FASB us-gaap metamodel vocabulary | identifier, category, type, source |
| **Classification** | Structural pattern classification for associations | identifier, category, type, source |

Selected relationships:

- `ELEMENT_HAS_LABEL`, `ELEMENT_HAS_REFERENCE`, `ELEMENT_HAS_TRAIT`
- `ENTITY_HAS_TAXONOMY`, `TAXONOMY_EXTENDS_TAXONOMY`
- `STRUCTURE_HAS_ASSOCIATION`, `ASSOCIATION_HAS_FROM_ELEMENT` / `_TO_ELEMENT`
- `DIMENSION_HAS_AXIS_ELEMENT`, `DIMENSION_HAS_MEMBER_ELEMENT`
- `ENTITY_HAS_AGENT`, `ENTITY_HAS_EVENT` — an entity owns its REA records
- `EVENT_INVOLVES_AGENT` — counterparty in an event
- `EVENT_AFFECTS_RESOURCE` — REA stockflow; the element plays the Resource role
- `EVENT_OBLIGATED_BY_EVENT` — commitment to fulfillment
- `EVENT_DISCHARGES_EVENT` — settlement / reciprocity
- `EVENT_REPLACES_EVENT` — correction chain

Parent–subsidiary ownership has **no edge**: nothing writes one on either path
(SEC or OLTP materialization), so `base.py` deliberately defers
`ENTITY_OWNS_ENTITY` until multi-entity consolidation ships. The designated
source when it does is OLTP `entities.parent_entity_id`, mirrored today on the
`Entity.parent_entity_id` property.

`Agent` and `Event` are universal REA primitives; every planned RoboX extension
needs them. `Event.event_action` carries the canonical action vocabulary
(`models/extensions/roboledger/event.py:EVENT_ACTIONS`), refining the coarser
`event_category`. SEC-flavored repositories get these tables with no rows.

Fiscal calendar and fiscal period stay OLTP-only — they are operational state
(rolling close pointers, status mutations) rather than curated graph content.
Operators query period membership through `Entry.posting_date` ranges.

## Extensions

Three extension modules exist. Everything else in the RoboX portfolio is
unbuilt: those schemas land alongside their OLTP models and adapters when the
product is designed, not as speculative placeholders.

| Module | Nodes | Domain |
| --- | --- | --- |
| `roboledger` | Report, Fact, FactSet, Transaction, Entry, LineItem | Financial reporting and general ledger |
| `roboinvestor` | Portfolio, Security, Position, Trade, Benchmark, MarketData | Portfolio and investment management |
| `knowledge` | Concept, Observation, Session | Agent-built knowledge graph |

`memory` is accepted as an alias for `knowledge`. Subgraphs persisted with
`schema_extensions=["memory"]` still resolve — every loader routes names through
`resolve_extension_alias` before importing.

### Schema extension vs product extension

An extension may be a schema and nothing more, or it may have grown a full
product stack around that schema. The distinction determines what else you have
to build when you touch one.

| Layer | Schema extension | Product extension |
| --- | --- | --- |
| Graph schema | yes | yes |
| OLTP database | no | yes (schema-per-tenant PostgreSQL) |
| API routes | no | yes (feature-flagged) |
| Data pipelines | no | yes (Dagster assets) |
| Frontend app | no | yes (own domain) |
| Trademark / brand | no | yes |

`roboledger` ([roboledger.ai](https://roboledger.ai)) and `roboinvestor`
([roboinvestor.ai](https://roboinvestor.ai)) are product extensions.
`knowledge` is schema-only.

### RoboLedger

Models financial reporting (XBRL/SEC), the general ledger, and the chart of
accounts. The chart of accounts is not its own node type — it is expressed
through the base `Element` / `Association` pattern shared with reporting.

- **Reporting**: Report, Fact, FactSet. Dimensional analysis, fact aggregation,
  taxonomy navigation.
- **Transactions**: Transaction → Entry → LineItem, a three-level model with
  dimensional tagging (department, class, location).
- **McCarthy bridge**: `EVENT_TRIGGERS_TRANSACTION` makes every GL Transaction
  traceable to its originating Event where one exists. Materialized from
  `transactions.triggered_by_event_id`; manual Transactions have no edge.

The product stack around it: OLTP tables in the `extensions` database, 38
GraphQL fields, roughly 25 command operations, graph-backed analytical views, a
QuickBooks ELT pipeline, and a dedicated frontend.

### RoboInvestor

Portfolio management, securities, and position tracking, with lot-level
positions and cross-graph report sharing (investor access to ledger reports).
Its product stack: OLTP models in the `extensions` database, an operations
kernel at `operations/roboinvestor/{reads,commands}/`, a command operations
router, 7 GraphQL fields, and a dedicated frontend.

## URL shape and feature flags

Product extensions are served under `/extensions/*`, split between one GraphQL
read endpoint and named command operations for writes. The domain name appears
in the write URL and in the flag name; reads are namespaced inside the Strawberry
schema instead of the URL.

```
# Reads — one endpoint, schema composed from whichever domains are enabled
POST /extensions/{graph_id}/graphql

# Writes — named command operations, one router per domain
POST /extensions/roboledger/{graph_id}/operations/{op}
POST /extensions/roboinvestor/{graph_id}/operations/{op}

# Analytical view operations — graph-backed, read-only, gated independently
POST /extensions/{domain}/{graph_id}/operations/{view_name}
```

`graph_id` is always a URL path parameter. GraphQL queries do **not** take a
`graphId` argument — the URL is the scope, and auth plus per-graph access are
resolved by FastAPI dependencies before the handler runs.

| Flag | Effect |
| --- | --- |
| `ROBOLEDGER_ENABLED` | Mounts the roboledger operations router; adds `LedgerQuery` to the GraphQL schema |
| `ROBOINVESTOR_ENABLED` | Mounts the roboinvestor operations router; adds `InvestorQuery` |
| `EXTENSIONS_GRAPHQL_ENABLED` | Kill switch for `/extensions/{graph_id}/graphql` (default `true`) |
| `FACT_GRID_ENABLED` | Gates the graph-backed fact-grid view independently of roboledger, so SEC-only deployments can use it |
| `EXTENSIONS_ENABLED` | Derived (`ROBOLEDGER_ENABLED or ROBOINVESTOR_ENABLED`); controls the extensions DB engine |

The schema is composed at class-construction time from the enabled domain
mixins, so a ledger-only deployment exposes only ledger fields — investor types
never appear in introspection, and there are no runtime `*_NOT_INITIALIZED`
errors. `EXTENSIONS_ENABLED` is a derived property, not an env var.

## Loading and building

```python
from robosystems.schemas.loader import (
    get_schema_loader,
    get_contextual_schema_loader,
)

loader = get_schema_loader()                                  # all extensions
loader = get_schema_loader(extensions=["roboledger"])         # a specific set

loader.list_node_types()
loader.get_node_schema("Entity")
loader.get_node_primary_key("Entity")
loader.validate_node_properties("Entity", {"identifier": "e1", "name": "Acme"})
```

### Context-aware loading

RoboLedger is one schema serving several use cases, and showing an AI Operator
tables that will always be empty is worse than not showing them. `RoboLedgerContext`
selects the subset a context can actually populate:

| Context | Nodes |
| --- | --- |
| `sec_repository`, `reporting_only` | Reporting only — SEC has aggregated reports, no transactions |
| `full_accounting` | Reporting + transactions |
| `transaction_only` | General ledger only |

```python
loader = get_contextual_schema_loader("repository", "sec")
loader = get_contextual_schema_loader("application", "roboledger")
loader = get_contextual_schema_loader("application", "roboinvestor")
```

### Compiling DDL

```python
from robosystems.schemas.runtime.builder import LadybugSchemaBuilder

builder = LadybugSchemaBuilder({
    "name": "My Financial Graph",
    "base_schema": "base",
    "extensions": ["roboledger", "roboinvestor"],
})
builder.load_schemas()
cypher_ddl = builder.generate_cypher()
```

### Validating and checking compatibility

```python
from robosystems.schemas.runtime.manager import SchemaManager
from robosystems.schemas.runtime.validator import LadybugSchemaValidator

validator = LadybugSchemaValidator()
validator.validate_node("Entity", {"identifier": "e1", "name": "Acme Corp"})
validator.validate_relationship("Entity", "Taxonomy", "ENTITY_HAS_TAXONOMY", {...})

compatibility = SchemaManager().check_schema_compatibility(["roboledger", "roboinvestor"])
if not compatibility.compatible:
    print(compatibility.conflicts)
```

## Custom schemas

Users can supply their own schema as JSON or YAML, merged onto base:

```python
from robosystems.schemas.runtime.custom import CustomSchemaManager

manager = CustomSchemaManager()
schema = manager.create_from_json(json_string)   # or create_from_yaml / create_from_dict
merged = manager.merge_with_base(schema)
```

```json
{
  "name": "CustomAnalytics",
  "version": "1.0.0",
  "extends": "base",
  "nodes": [
    {
      "name": "Metric",
      "properties": [
        { "name": "id", "type": "STRING", "is_primary_key": true },
        { "name": "value", "type": "DOUBLE" }
      ]
    }
  ],
  "relationships": [
    { "name": "ENTITY_HAS_METRIC", "from_node": "Entity", "to_node": "Metric" }
  ]
}
```

Custom schemas extend base rather than replacing it. Node names are checked
against `CustomSchemaParser.RESERVED_NODE_NAMES` (`SystemConfig`,
`SchemaVersion`, `AuditLog`, `Permission`, `Role`, `Session`, `Lock`,
`Migration`, `SystemUser`) and relationship names against
`RESERVED_RELATIONSHIP_NAMES` (the `SYSTEM_*` set).

### Property types

`CustomSchemaParser.VALID_TYPES` is the authority:

| Category | Types |
| --- | --- |
| Strings | `STRING` |
| Integers | `INT8`, `INT16`, `INT32`, `INT64`, `INT128`, `UINT8`, `UINT16`, `UINT32`, `UINT64` |
| Floating point | `FLOAT`, `DOUBLE` |
| Temporal | `DATE`, `TIMESTAMP`, `INTERVAL` |
| Other scalars | `BOOLEAN`, `UUID`, `BLOB` |
| Composite | `LIST`, `MAP`, `STRUCT`, `UNION` |
| Graph | `NODE`, `REL` |

```python
Property(name="amount", type="DOUBLE")
Property(name="identifier", type="STRING", is_primary_key=True)
```

Primary keys are indexed automatically.

## Schema evolution

DDL is emitted as `CREATE ... IF NOT EXISTS`, so adding nodes and relationships
is safe against a live database. Adding a property to an existing table uses
`ALTER TABLE`. Renames, removals, and type changes need a coordinated migration
— never `DROP` and re-`CREATE` in production.

Schema and tier metadata are tracked on the platform `Graph` model
(`models/core/graph/graph.py`), not as a node inside the graph.

Access control is likewise a platform-database concern, enforced through
`models/core/user/` (`GraphUser` and friends). The base schema has no user or
permission node, and tenant isolation is at the database-file level.

Because the GraphQL schema and operation envelopes are wrapped by the published
Python and TypeScript SDKs, a breaking change here propagates as an SDK major.
Prefer additive evolution: new fields and types are free, renames and removals
are not.

## Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| Duplicate node names on load | Two extensions declare the same node | `check_schema_compatibility` before loading |
| Missing primary key | Node declared without one | Every node needs an `is_primary_key` property |
| Relationship references an unknown node | The other extension isn't loaded | Load both, or move the edge |
| Context loading falls back to base only | Unrecognized context, or the extension has no `RoboLedgerContext` | Use a defined context name |

```python
loader = get_schema_loader(extensions=["roboledger"])
print(loader.list_node_types())
print(loader.list_relationship_types())
```
