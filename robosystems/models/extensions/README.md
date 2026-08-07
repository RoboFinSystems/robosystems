# Extensions OLTP Models

SQLAlchemy models for the **extensions PostgreSQL database** — per-graph transactional data for RoboLedger (general ledger, schedules, reports) and RoboInvestor (portfolios, positions, securities). These tables are the system of record; they materialize one-way into LadybugDB for analytical queries.

## The two databases

RoboSystems runs against two logically separate PostgreSQL databases that share one RDS instance in production:

| Database | Purpose | Base class | Models | Migration config |
| -------- | ------- | ---------- | ------ | ---------------- |
| `robosystems` (platform) | IAM, billing, graph metadata, connections, documents | `Model` / `Base` from `robosystems.database` | `robosystems/models/core/` | `migrations/platform.ini` |
| `extensions` (**this directory**) | Per-graph OLTP for product extensions | `ExtensionsBase` from `robosystems.db.extensions` | `robosystems/models/extensions/` | `migrations/extensions.ini` |

They use **different `DeclarativeBase` classes**, so a model inheriting from `ExtensionsBase` can never land in the platform database, and vice versa. Migration histories are independent: `just migrate-up extensions` never touches platform revision state.

For the platform side, see [`../core/README.md`](../core/README.md).

## Schema-per-graph-id multi-tenancy

Every graph gets its own PostgreSQL schema inside the `extensions` database, keyed by `graph_id`:

```
extensions database:
├── public/           → migration history (alembic_version), shared functions
├── kg123abc.../      → Entity, Transaction, Entry, LineItem, Element, Structure, Fact, Portfolio, ...
├── kg456def.../      → same tables, fully isolated from kg123
└── ...
```

Isolation is enforced by PostgreSQL via `SET search_path`. Two tenants can hold rows with the same ULID and never collide, because they live in different schemas.

**Every table in this directory is tenant-scoped.** None of the models set an explicit `__table_args__.schema` — that absence is the signal to `provision_tenant_schema` that the table belongs in each `kg*` schema, not in `public`.

### Opening a session

```python
from robosystems.db.extensions import extensions_session

with extensions_session("kg123abc456") as session:
    # SET search_path TO kg123abc456, public is applied before any query.
    txns = session.query(Transaction).order_by(Transaction.date).all()
```

The context manager validates the graph id against `_sanitize_schema` (hard stop on anything that isn't a legal PostgreSQL identifier — this is the barrier against SQL injection through a schema name), opens a session on the extensions engine, issues `SET search_path TO {schema}, public`, and commits or rolls back on exit.

**Never hand-roll a session against the extensions engine.** The `SET search_path` step is load-bearing for tenant isolation; skipping it leaks one tenant's writes into another's schema.

### Lazy schema provisioning

A new graph has no tenant schema until something needs one. `provision_tenant_schema(graph_id)` creates it on first extension access — typically during connection setup or manual entity creation:

```python
from robosystems.db.extensions import provision_tenant_schema

provision_tenant_schema("kg123abc456")
# CREATE SCHEMA kg123abc456, then create every table registered on
# ExtensionsBase.metadata that doesn't declare its own schema.
```

Once provisioned, the schema persists for the life of the graph, and later model changes arrive via migrations rather than re-provisioning.

## Base vs extension shelving

Top-level files mirror `schemas/base.py` — **base ontology concepts** applicable regardless of which extension consumes them. Extension-specific models live in `roboledger/` and `roboinvestor/`. `roboledger/__init__.py` re-exports the base concepts it depends on, so ledger call sites can import them from one place.

**Before adding a model file, decide where it belongs:**

- **Top level (base)** if the concept is universally applicable — present in `schemas/base.py` or a natural fit for a future extension. Entity, Taxonomy, Element, Dimension, Association, Structure.
- **Subfolder (extension)** if it is specific to one product vertical. Transaction/Entry/LineItem are ledger-specific; Portfolio/Position/Security are investor-specific.

`schemas/base.py` is the source of truth for what counts as base, and `schemas/README.md` states the governing rule (Invariant 1, "base is aspirational"): promote when the concept is universally applicable, not when a second consumer appears. Waiting for the second consumer turns every promotion into a migration against materialized data.

### Base concepts (top level)

| Module | Concept |
| ------ | ------- |
| `entity.py` | `Entity` — the parent entity for a graph |
| `taxonomy.py` | `Taxonomy` — grouping container (CoA, reporting, mapping, schedule) plus the extension chain |
| `element.py` | `Element` — CoA elements and US GAAP reporting elements, unified. `Account` is an alias of `Element` |
| `element_label.py`, `element_reference.py`, `element_trait.py` | Supplementary labels, ASC citations, trait bindings (attached by qname) |
| `trait.py` | `Trait` — the universal trait vocabulary (axes/categories) |
| `dimension.py` | `Dimension` — tags such as department, class, location, fund |
| `association.py` | `Association` — CoA→GAAP mappings, presentation and calculation ordering |
| `classification.py`, `association_classification.py` | Structural pattern classification and its junction table |
| `structure.py`, `structure_template.py` | `Structure` — a named element collection; reusable scaffolds |
| `reporting_style_network.py` | Per-style presentation network selection |
| `framework.py`, `framework_package.py`, `framework_bridge.py`, `bridge.py` | Addressable taxonomy bundles and cross-namespace equivalence overlays |
| `rule.py`, `verification_result.py` | Pattern/expression validations and their persisted outcomes |
| `entity_taxonomy.py` | Join table for `ENTITY_HAS_TAXONOMY` (multi-basis) |

### RoboLedger (`roboledger/`)

`agent.py` (`Agent` — the REA counterparty: customer, vendor, employee), `event.py` and `event_handler.py` (REA business events and the maps from event type to ledger postings), `transaction.py` / `entry.py` / `line_item.py` (the three-level ledger), `fact.py` and `fact_set.py`, `fiscal_calendar.py` and `fiscal_period.py`, `report.py`, `report_share.py`, `publish_list.py`, and `dimension_junctions.py` (the `transaction_dimensions` / `entry_dimensions` / `line_item_dimensions` association tables).

### RoboInvestor (`roboinvestor/`)

`portfolio.py`, `position.py` (lot-level), `security.py`.

## Domain notes

### Three-level ledger

```
Transaction (business event)
    └── Entry (journal entry — must balance)
            └── LineItem (individual debit or credit)
```

**Transaction** is what happened in the real world (a QuickBooks invoice, a bank deposit, a manual adjustment). **Entry** is the accounting interpretation; multiple entries per transaction support accruals, corrections, and multi-fund allocation, and each entry must balance on its own. **LineItem** is the individual debit or credit, tied to an `Element` (the account).

For QuickBooks-sourced data, `Transaction:Entry` is 1:1 — QuickBooks is pre-journalized. Manually authored accruals and closing entries are where the 1:many capability earns its keep.

### Dimensions

Dimensions are key/value tags attached through the association tables in `dimension_junctions.py`, so one line item can carry `department=eng`, `location=nyc`, and `fund=ops-budget` without widening the base tables. The junctions are exported from `roboledger/__init__.py` for queries that join through them.

### Chart of accounts via Element

There is no dedicated `Account` model. The chart of accounts is modeled with `Element` — the same model used for XBRL reporting elements — with company accounts and standardized US GAAP elements distinguished by their `Taxonomy` (`type='chart_of_accounts'` vs `type='reporting'`). `Association` then carries CoA→GAAP rollup mappings through the same pattern as XBRL presentation and calculation links. `Account = Element` in `element.py` is an alias for CoA-flavored call sites.

## Conventions

- **Primary keys are prefixed ULIDs** — `txn_01H…`, `entry_01H…`, `port_01H…`.
- **Amounts are `BIGINT` in minor currency units** (cents), not `NUMERIC`. The materialization pipeline converts to `DOUBLE` dollars for graph queries. This avoids rounding drift across debit and credit legs.
- **Timestamps are UTC-aware.**
- **CHECK constraints, not PostgreSQL enums** — statuses like `'pending' | 'posted' | 'void'` are enforced with `CheckConstraint` on a plain `String` column, for the same portability reason as the platform models.
- **Origin tracking on ledger rows.** `Entry.provenance` is a nullable `String` constrained by `ck_entries_provenance` to the values in `ENTRY_PROVENANCE_VALUES` (`source_sync`, `ai_generated`, `manual_entry`, `schedule_derived`, `system_computed`, `event_handler`). That tuple is the single source of truth for both the constraint and any write path — keep it in lockstep, and note that migrations carry their own static snapshot because they must not import model constants. `Transaction` instead has `source` (`String`, default `"native"`) plus a nullable `source_id`. Both flow through materialization, so audit queries like "all AI-generated entries for April" work end to end.
- **Indexes are explicit.** `__table_args__` lists the indexes routers and resolvers actually query on. Add them when you add the column; don't rely on autogenerate.

### Fact provenance

Distinct from the row-level `Entry.provenance` string, `fact_sets.provenance` is a JSONB column holding a typed `FactProvenance` descriptor — a discriminated union dispatched on the `origin` tag, defined in `models/api/fact_provenance.py`. Every fact is emitted through a `FactSet`, so each fact inherits a typed origin without a descriptor on every row.

Stamping is mandatory at emission: `operations/roboledger/fact_set.py::create_fact_set` is the only blessed writer, and a `before_insert` event on the model raises `ProvenanceRequiredError` for any `FactSet` inserted without a descriptor.

## Materialization to LadybugDB

Extension models are the OLTP source of record; analytical queries run against LadybugDB. Data flows one way:

```
PostgreSQL (tenant schema) → postgres_scanner → DuckDB staging → LadybugDB
```

The pipeline lives in `operations/extensions/` (`materialize.py`, `loader.py`, `staleness.py`). Each tenant schema is read through DuckDB's `postgres_scanner`, staged per model, and materialized into LadybugDB nodes and relationships.

**The graph never receives direct writes.** Consequences worth knowing:

- A new column is in PostgreSQL immediately but does not appear in the graph until the next materialization run. Write paths call `mark_graph_stale(graph_id, reason)`; the `stale_graph_materialization_sensor` picks that up and rematerializes, usually within seconds.
- Computed graph properties (depreciation, rollups) belong in the materializer, not in the SQLAlchemy model.
- The graph schema (`robosystems/schemas/extensions/roboledger.py`, `roboinvestor.py`) is the wire shape for LadybugDB nodes. It is not required to mirror these models 1:1, and often doesn't.

## Migrations

Always autogenerate. The workflow matches the platform's, with `extensions` as the second argument to every recipe:

```bash
# 1. Edit a SQLAlchemy model in this directory
just migrate-create "add bank_feeds table" extensions

# 2. Review the generated file in migrations/extensions/versions/
#    Verify every op.create_table / op.add_column call is schema-less
#    (tenant-scoped). Autogenerate against a multi-tenant database can
#    otherwise target the wrong schema.

just migrate-up extensions
```

`just migrate-current extensions`, `just migrate-history extensions`, `just migrate-down extensions` (local only), and `just migrate-reset extensions` (destructive, local only) round out the set. Config is in `migrations/extensions.ini`; revisions live under `migrations/extensions/versions/`.

### Multi-tenant caveat

Alembic applies migrations to `public` by default, not to tenant schemas. Because the generated `op.create_table` / `op.add_column` calls carry no `schema=` kwarg:

- **New tenant schemas** provisioned after the migration pick up the new shape automatically, because `provision_tenant_schema` reads `ExtensionsBase.metadata` at provision time.
- **Existing tenant schemas** need the same DDL applied per schema. `robosystems/db/extensions.py` has the tooling for iterating tenants during an alter; the pattern is to migrate `public` first, then loop over existing schemas.

That is the operational cost of schema-per-graph tenancy, paid for explicit PostgreSQL-level isolation of financial data.

## Adding a model

Adding `BankFeed` to the roboledger domain:

1. **Create the model file** under the right subfolder:

   ```python
   # robosystems/models/extensions/roboledger/bank_feed.py
   from datetime import UTC, datetime
   from sqlalchemy import BigInteger, CheckConstraint, Column, Date, DateTime, Index, String
   from robosystems.db.extensions import ExtensionsBase
   from robosystems.utils.ulid import generate_prefixed_ulid

   class BankFeed(ExtensionsBase):
       __tablename__ = "bank_feeds"
       __table_args__ = (
           Index("idx_bank_feeds_date", "transaction_date"),
           CheckConstraint("amount >= 0", name="check_bank_feed_amount"),
       )

       id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("bf"))
       transaction_date = Column(Date, nullable=False)
       amount = Column(BigInteger, nullable=False)  # cents
       created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
   ```

2. **Export it** from `roboledger/__init__.py` and from `extensions/__init__.py`.
3. **Generate the migration**: `just migrate-create "add bank_feeds table" extensions`.
4. **Review and apply**, then confirm the table exists in a tenant schema (`\dt kg….*` in psql).
5. **If it must reach the graph**, add the corresponding node type to `robosystems/schemas/extensions/roboledger.py` and a materialization step in `operations/extensions/`.
6. **If an API reads or writes it**, add Pydantic models under `robosystems/models/api/extensions/` and wire them through `operations/roboledger/{reads,commands}/`. The model file is a schema definition, not a domain object — no business logic there.

## Related

- [Platform Database Models](../core/README.md) — the other half of the model tree.
- [API Models](../api/README.md) — the Pydantic wire shapes GraphQL resolvers and command routes return.
- [Schema Extensions](../../schemas/README.md) — the LadybugDB schemas for the same domains, bridged by the materialization pipeline.
- [Extensions GraphQL](../../graphql/README.md) — the read surface built on these models through the operations layer.
- `robosystems/db/extensions.py` — `ExtensionsBase`, `extensions_session`, `provision_tenant_schema`.
