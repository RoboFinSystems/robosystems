# Extensions OLTP Models

SQLAlchemy models for the **extensions PostgreSQL database** — per-graph transactional data for RoboLedger (accounting general ledger, schedules, reports) and RoboInvestor (portfolios, positions, securities). These tables are the system of record for data the platform owns, and they materialize to LadybugDB through a controlled pipeline for analytical queries.

## The two databases, briefly

RoboSystems runs against **two logically separate PostgreSQL databases** that happen to live on the same shared RDS instance in production:

| Database                          | Purpose                                              | Base class                                        | Models location                  | Migration config            |
| --------------------------------- | ---------------------------------------------------- | ------------------------------------------------- | -------------------------------- | --------------------------- |
| `robosystems` (platform)          | IAM, billing, graph metadata, connections, documents | `Model` / `Base` from `robosystems.database`      | `robosystems/models/core/`       | `migrations/platform.ini`   |
| `extensions` (**this directory**) | Per-graph OLTP for product extensions                | `ExtensionsBase` from `robosystems.db.extensions` | `robosystems/models/extensions/` | `migrations/extensions.ini` |

They use **different `DeclarativeBase` classes**, so a model inheriting from `ExtensionsBase` never accidentally ends up in the platform database, and vice versa. The two databases also have independent Alembic migration histories — running `just migrate-up extensions` migrates extensions without touching the platform.

For the platform side, see [`../core/README.md`](../core/README.md).

## Schema-per-graph-id multi-tenancy

Every graph gets its **own PostgreSQL schema** inside the `extensions` database, keyed by `graph_id`:

```
extensions database:
├── public/           → migration history (alembic_version), shared functions
├── kg123abc.../      → Entity, Transaction, Entry, LineItem, Account, Structure, Fact, Portfolio, ...
├── kg456def.../      → same tables, fully isolated from kg123
└── kg789ghi.../      → ...
```

Isolation is enforced at the PostgreSQL level via `SET search_path` — two tenants can have transactions with the same ULID and never collide because they live in different schemas, with zero cross-tenant read/write paths through application code.

**All tables in this directory are tenant-scoped.** None of the models set an explicit `__table_args__.schema` — that's the signal to `provision_tenant_schema` that these tables belong in each `kg*` schema, not in `public`.

### Opening a session

```python
from robosystems.db.extensions import extensions_session

with extensions_session("kg123abc456") as session:
    # SET search_path TO kg123abc456, public is applied before any query.
    # Every ORM operation is implicitly scoped to this tenant.
    txns = session.query(Transaction).order_by(Transaction.date).all()
```

The context manager:

1. Validates the graph_id against `_sanitize_schema` (hard stop on anything that isn't a legal PostgreSQL identifier — prevents SQL injection via schema names).
2. Opens a session from the extensions engine.
3. Issues `SET search_path TO {schema}, public` so unqualified table names resolve to the tenant schema.
4. Yields the session. On exit it commits or rolls back and returns the connection to the pool.

**Never hand-roll your own session against the extensions engine** — always go through `extensions_session`. The `SET search_path` step is load-bearing for tenant isolation, and skipping it leaks one tenant's writes into another's schema. This was the Celery incident a while back (see `feedback_session_isolation.md` in auto-memory).

### Lazy schema provisioning

When a new graph is created, its tenant schema does **not** exist yet. It's provisioned on first extension access via `provision_tenant_schema(graph_id)`:

```python
from robosystems.db.extensions import provision_tenant_schema

provision_tenant_schema("kg123abc456")
# → CREATE SCHEMA kg123abc456
# → For every model class registered on ExtensionsBase.metadata (that doesn't set
#   an explicit schema), creates the corresponding table inside the new schema.
```

This is called from the extensions-enabled code path when a graph first needs ledger or investor storage — typically during QuickBooks connection setup or manual entity creation. Once provisioned, the schema persists for the lifetime of the graph and any model changes are applied via migrations, not re-provisioning.

## Directory layout

```
models/extensions/
├── __init__.py                  # Re-exports every extension model — flat namespace
├── entity.py                    # Entity — shared base (single parent entity per graph for now)
├── roboledger/                  # Accounting OLTP — 15 tables
│   ├── account.py               # (currently modeled via Element; file present for future CoA split)
│   ├── association.py           # Association — CoA → GAAP mappings, presentation ordering
│   ├── classification_rule.py   # ClassificationRule — auto-classification rules for GLiNER-style NER
│   ├── dimension.py             # Dimension — tags (department, class, location, fund, trust)
│   ├── element.py               # Element — CoA elements + US GAAP reporting elements (unified)
│   ├── entry.py                 # Entry — journal entries within a transaction (must balance)
│   ├── fact.py                  # Fact — element × period × dimension → amount (XBRL-style)
│   ├── fiscal_calendar.py       # FiscalCalendar — one row per graph, holds close target
│   ├── fiscal_period.py         # FiscalPeriod — monthly/quarterly period state (open/closed)
│   ├── line_item.py             # LineItem — individual debits/credits within an entry
│   ├── publish_list.py          # PublishList + PublishListMember — report sharing rings
│   ├── report.py                # Report — snapshot of facts at creation time (immutable)
│   ├── report_share.py          # ReportShare — cross-graph report shares (investor access)
│   ├── structure.py             # Structure — named element collection (income statement, schedule, ...)
│   ├── taxonomy.py              # Taxonomy — grouping container (CoA, reporting, mapping, schedule)
│   └── transaction.py           # Transaction — business event (what happened in the real world)
└── roboinvestor/                # Portfolio OLTP — 3 tables
    ├── portfolio.py             # Portfolio — an investor's holdings container
    ├── position.py               # Position — lot-level position within a portfolio
    └── security.py              # Security — the thing being held, optionally linked to an Entity
```

### The three-level ledger model

Transactions, entries, and line items form a **three-level hierarchy** (introduced March 2026):

```
Transaction (business event)
    └── Entry (journal entry — must balance)
            └── LineItem (individual debit or credit)
```

- **Transaction** = what happened in the real world (QuickBooks invoice, bank deposit, manual adjustment).
- **Entry** = the accounting interpretation. Multiple entries per transaction are supported for accruals, corrections, multi-fund allocation. Each entry must balance independently.
- **LineItem** = the individual debits and credits within an entry, each tied to an `Element` (the account).

For QuickBooks-sourced data, `Transaction:Entry` is 1:1 (QB is pre-journalized). Manually authored accruals and closing entries are where the 1:many capability is used.

### Dimensions

`Dimension` is the tagging system for entries and line items. Dimensions are key/value pairs attached via association tables (`transaction_dimensions`, `entry_dimensions`, `line_item_dimensions`) — this is how a single line item can carry `department=eng`, `location=nyc`, `fund=ops-budget` without bloating the base tables. The association tables are exported from `roboledger/__init__.py` for explicit use in queries that need to join through them.

### Chart of Accounts via Element

RoboLedger **does not have a dedicated `Account` model** for the chart of accounts — the CoA is modeled via `Element` nodes (the same Element model used for XBRL reporting elements). Company-specific accounts and standardized US GAAP reporting elements coexist in the same table, distinguished by their associated `Taxonomy` (`type='chart_of_accounts'` vs `type='reporting'`). The `Association` model then handles CoA → GAAP rollup mappings via the same pattern used for XBRL presentation/calculation links.

This is a deliberate unification — see the Taxonomy System notes for the architectural rationale. The `account.py` file in this directory is reserved for a future split if it's ever needed.

## Base class pattern

Every model in this directory inherits from `ExtensionsBase`:

```python
from sqlalchemy import Column, String
from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid

class Transaction(ExtensionsBase):
    __tablename__ = "transactions"
    # No __table_args__.schema → this is a tenant table

    id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("txn"))
    # ...
```

`ExtensionsBase` is a **separate `DeclarativeBase`** from the platform `Base` — it has its own `.metadata` registry, which is how `provision_tenant_schema` enumerates which tables to create in a new tenant schema without accidentally trying to create `users` or `graphs` there.

## Conventions

- **Primary keys are prefixed ULIDs** — `txn_01H…`, `entry_01H…`, `port_01H…`. Same pattern as platform models, different prefixes per domain.
- **Amounts are `BIGINT` in minor currency units** (cents), not `NUMERIC`. Converted to dollars as `DOUBLE` during the materialization pipeline for graph queries. This avoids rounding drift across debit/credit legs.
- **Timestamps are UTC-aware.**
- **CHECK constraints, not enums** — statuses like `'pending' | 'posted' | 'void'` are enforced via `CheckConstraint` on a plain `String` column rather than PostgreSQL `ENUM` types. Same portability rationale as core models.
- **Provenance field on every ledger row** — `Transaction` and `Entry` carry a `provenance` field (`source_sync`, `ai_generated`, `manual_entry`, `schedule_derived`, `system_computed`) enforced by a CHECK constraint. This flows through materialization to the graph so audit queries like "show me all AI-generated entries for April" work end-to-end.
- **Indexes are explicit** — `__table_args__` always lists the indexes the routers and resolvers actually query on. Don't rely on autogenerate to add indexes; add them when you add the column.

## Materialization to LadybugDB

Extension models are the **OLTP source of record**, but analytical queries run against LadybugDB, not PostgreSQL. Data flows one-way:

```
PostgreSQL (extensions schema) → postgres_scanner → DuckDB staging → LadybugDB
```

This is driven by the extensions materializer (`operations/graph/extensions_materializer.py`). Each tenant schema is scanned via DuckDB's `postgres_scanner` extension, converted into a DuckDB staging table per graph-schema model, and then materialized into LadybugDB nodes and relationships via the standard materialization endpoint.

**The graph never receives direct writes.** All data enters LadybugDB through this pipeline. That means:

- If you add a new column to a SQLAlchemy model, it's in PostgreSQL immediately but won't appear in the graph until the next materialization run (triggered by the `mark_graph_stale` sensor, typically seconds later).
- Computed graph properties (depreciation, rollups) live in the materializer — not in the SQLAlchemy model.
- The graph schema (`robosystems/schemas/extensions/roboledger.py`, `roboinvestor.py`) is the wire shape for LadybugDB nodes; it doesn't have to be a 1:1 mirror of the SQLAlchemy models, and often isn't (e.g., the three-level ledger model → graph Transaction/Entry/LineItem node hierarchy).

See the [Extensions Materialization project notes](../../../../.claude/projects/-Users-french-Projects-robosystems/memory/project_ledger_materialization.md) for the pipeline details.

## Migrations

Always autogenerate — never write migrations by hand. The workflow for extensions is identical to the platform workflow except every command takes an `extensions` argument:

```bash
# 1. Edit a SQLAlchemy model in this directory
# 2. Autogenerate a migration against the current extensions DB state
just migrate-create "add depreciation schedule columns" extensions

# 3. Review the generated file in migrations/extensions/versions/
#    Pay extra attention — autogenerate against a multi-tenant schema sometimes
#    produces migrations that target the wrong schema. Always verify the
#    op.create_table / op.add_column calls are schema-less (tenant-scoped).

# 4. Apply
just migrate-up extensions
```

Other recipes (all take the `extensions` argument):

```bash
just migrate-current extensions    # Show current revision
just migrate-history extensions    # Show migration history
just migrate-down extensions       # Rollback one migration (local only)
just migrate-reset extensions      # Downgrade to base and re-upgrade — destructive, local only
```

Migrations for this database are config'd in `migrations/extensions.ini` and live under `migrations/extensions/versions/`. The migration history is completely independent from the platform database — bumping the extensions schema never changes platform revision state, and vice versa.

### Multi-tenant caveat

Alembic migrations apply to the `public` schema by default, not to tenant schemas. When you add a table or column here, the migration's `op.create_table` / `op.add_column` calls are schema-less (no `schema=` kwarg), which means:

- **New tenant schemas** provisioned after the migration runs automatically pick up the new shape (because `provision_tenant_schema` uses `ExtensionsBase.metadata` at provision time).
- **Existing tenant schemas** need the migration re-applied to each schema. There's tooling in `robosystems/db/extensions.py` for iterating tenants during an alter; for new-column migrations the pattern is: migrate public first, then loop over existing schemas and apply the same DDL.

This is a known operational cost of schema-per-graph tenancy. The tradeoff is explicit PostgreSQL-level isolation, which is worth it for financial data.

## Adding a new extension model

Concrete walkthrough — adding a `BankFeed` table to the roboledger domain:

1. **Create the model file** under the appropriate subfolder:

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
       provenance = Column(String, nullable=False, default="source_sync")
       created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
   ```

2. **Export it from `roboledger/__init__.py`**:

   ```python
   from .bank_feed import BankFeed
   __all__ = [..., "BankFeed"]
   ```

3. **Export it from `extensions/__init__.py`** (top-level package):

   ```python
   from .roboledger import (..., BankFeed)
   __all__ = [..., "BankFeed"]
   ```

4. **Generate the migration**:

   ```bash
   just migrate-create "add bank_feeds table" extensions
   ```

5. **Review** the generated migration under `migrations/extensions/versions/`, apply with `just migrate-up extensions`, and verify the table exists in a tenant schema via `\dt kg...*.*` in psql.

6. **If the table needs to appear in the graph**, also update the roboledger graph schema (`robosystems/schemas/extensions/roboledger.py`) with the corresponding `NodeType` and add a materialization step to the extensions materializer.

7. **If the table is read/written by an API**, add Pydantic request/response models in `robosystems/models/api/extensions/`, then wire them through the CQRS layer at `operations/roboledger/{reads,commands}/bank_feeds.py`. Don't add business logic to the model file itself — the model is a schema definition, not a domain object.

## Related

- [Platform Database Models](../core/README.md) — the other half of the model tree, backed by the separate `robosystems` platform database.
- [API Models](../api/README.md) — Pydantic request/response models. Extension API models live under `models/api/extensions/` and are what the GraphQL resolvers and command routes actually return; this directory is the storage layer one level down.
- [Schema Extensions](../../schemas/README.md) — graph (LadybugDB) schemas for the same extensions. The PostgreSQL models in this directory and the graph schemas there are two different wire shapes for the same domain data, bridged by the materialization pipeline.
- [GraphQL Extensions](../../graphql/README.md) — the read surface built on top of these models via the operations layer.
- [Database Connection Layer](../../db/) — `db/extensions.py` defines `ExtensionsBase`, `extensions_session`, `provision_tenant_schema`.
