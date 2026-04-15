# Platform Database Models

SQLAlchemy models for the **platform PostgreSQL database** — the single source of truth for users, organizations, graphs, billing, connections, and documents. Everything that needs to exist before any extension (RoboLedger, RoboInvestor) can even provision itself for a tenant lives here.

## The two databases, briefly

RoboSystems runs against **two logically separate PostgreSQL databases** that happen to live on the same shared RDS instance in production:

| Database                     | Purpose                                                               | Base class                                        | Models location                             | Migration config            |
| ---------------------------- | --------------------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------- | --------------------------- |
| `robosystems` (**platform**) | IAM, billing, graph metadata, connections, documents                  | `Model` / `Base` from `robosystems.database`      | `robosystems/models/core/` (this directory) | `migrations/platform.ini`   |
| `extensions`                 | Per-graph OLTP for product extensions (accounting ledger, portfolios) | `ExtensionsBase` from `robosystems.db.extensions` | `robosystems/models/extensions/`            | `migrations/extensions.ini` |

They use **different `DeclarativeBase` classes**, so a model inheriting from `Model` never accidentally ends up in the extensions database, and vice versa. The two databases also have independent Alembic migration histories — running `just migrate-up` migrates the platform, `just migrate-up extensions` migrates extensions.

This README covers the platform side. For extensions, see [`../extensions/README.md`](../extensions/README.md).

## Directory layout

```
models/core/
├── __init__.py           # Re-exports every model — import from `robosystems.models.core`
├── billing/              # Stripe subscriptions, invoices, audit log
│   ├── audit_log.py      # BillingAuditLog — every Stripe webhook and billing event
│   ├── customer.py       # BillingCustomer — Stripe customer id per graph/org
│   ├── invoice.py        # BillingInvoice + BillingInvoiceLineItem
│   └── subscription.py   # BillingSubscription — graph/org subscription state
├── connection/           # External data source connections (QuickBooks, Plaid, SEC)
│   ├── connection.py           # Connection — provider, graph_id, user_id, status, sync state
│   └── connection_credentials.py # ConnectionCredentials — Fernet-encrypted OAuth tokens
├── document/             # Uploaded documents (policies, user content indexed in OpenSearch)
│   └── document.py       # Document — metadata for markdown/text uploads
├── graph/                # Graph resources — the biggest subtree
│   ├── graph.py          # Graph — graph_id, tier, status, schema_type, subgraph parentage
│   ├── graph_backup.py   # GraphBackup — S3 snapshot records with restore metadata
│   ├── graph_credits.py  # GraphCredits + GraphCreditTransaction — credit balance + ledger
│   ├── graph_file.py     # GraphFile — files ingested into a graph
│   ├── graph_schema.py   # GraphSchema — per-graph schema extension list
│   ├── graph_table.py    # GraphTable — staging table metadata
│   ├── graph_usage.py    # GraphUsage — usage events for metering
│   ├── graph_user.py     # GraphUser — graph access control (shares, roles)
│   └── source_file.py    # SourceFile — upstream file provenance
├── org/                  # Organizations and membership
│   ├── org.py            # Org — organization entity
│   ├── org_limits.py     # OrgLimits — per-org quotas
│   └── org_user.py       # OrgUser — membership with roles
└── user/                 # Users, API keys, tokens, shared repository access
    ├── user.py                       # User — auth identity, email, password hash
    ├── user_api_key.py               # UserAPIKey — `rfs…` keys for programmatic access
    ├── user_repository.py            # UserRepository — access to shared repos (SEC, etc.)
    ├── user_repository_credits.py    # Credits + transactions for shared-repo usage
    └── user_token.py                 # UserToken — JWT refresh, password reset, etc.
```

## Base class pattern

Every model in this directory inherits from `Model`:

```python
from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid

class User(Model):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("user"))
    email = Column(String, unique=True, nullable=False, index=True)
    # ...
```

`Model` (and its alias `Base`) are re-exported from `robosystems.database`, which is a thin compatibility shim over `robosystems.db.platform`. New code can import from either path — the shim exists so the 130+ files that already import from `robosystems.database` don't need to change.

## Conventions

- **Primary keys are prefixed ULIDs** — `user_01H…`, `graph_01H…`, `conn_01H…`. Generated via `generate_prefixed_ulid(prefix)` from `robosystems.utils.ulid`. Never use auto-increment integers.
- **Timestamps are UTC-aware** — `DateTime` columns use `default=lambda: datetime.now(UTC)`. Never store naive datetimes.
- **Enums are stored as strings**, not PostgreSQL `ENUM` types. This lets us add new enum values without a DB migration (see `RepositoryType` / `repository_type` column on `UserRepository` for the canonical example). The Python enum class in the model file is a convenience for callers, not a DB constraint.
- **No circular imports in `__init__.py`** — models are re-exported in a flat namespace, and the subfolder `__init__.py` files import each model explicitly so `from robosystems.models.core import User` works without touching other subtrees.
- **Relationships use `back_populates`**, not `backref` — makes the relationship explicit on both sides and helps type checkers follow the graph.

## Migrations

Always autogenerate — never write migrations by hand. The workflow is:

```bash
# 1. Edit a SQLAlchemy model in this directory
# 2. Autogenerate a migration against the current DB state
just migrate-create "add foo column to users"

# 3. Review the generated file in migrations/platform/versions/
#    Alembic autogenerate misses: enum changes, CHECK constraints, some index changes —
#    fix those by hand before applying.

# 4. Apply
just migrate-up
```

Other useful recipes:

```bash
just migrate-current              # Show current revision
just migrate-history              # Show migration history
just migrate-down                 # Rollback one migration (local only)
just migrate-reset                # Downgrade to base and re-upgrade — destructive, local only
```

Migrations for this database are config'd in `migrations/platform.ini` and live under `migrations/platform/versions/`. They're **completely independent** from the extensions database — `just migrate-up` never touches the extensions schema.

## Relationship to the rest of the platform

- **Graph IDs are first-class here.** The `Graph` model owns `graph_id`, tier, status, schema extensions list, and subgraph parentage. Every other subsystem (graph routing, billing, rate limiting, extensions session scoping) looks up graphs through this model.
- **The `Connection` model is PostgreSQL-only** — there are no Connection graph nodes anymore (removed Feb 2026). OAuth credentials are stored encrypted in `ConnectionCredentials` via Fernet. See the Connection System notes in the auto-memory for the full rationale.
- **Graph subscription and credits** are platform concerns, not extension concerns. `GraphCredits` + `GraphCreditTransaction` track the credit balance; billing webhooks write through these models. Extensions consume credits via `operations/graph/credit_service.py` which reads/writes these models.
- **Shared repository access** (SEC, etc.) lives under `user/user_repository.py`. The `repository_type` column is a plain string so new shared repos can land via adapter manifests without a DB migration.

## Related

- [Extensions OLTP Models](../extensions/README.md) — the other half of the model tree, backed by the separate `extensions` database with schema-per-graph-id tenancy.
- [API Models](../api/README.md) — Pydantic request/response models. These are the wire shapes for the FastAPI routers; they reference SQLAlchemy models here only indirectly via the operations layer.
- [Database Connection Layer](../../db/) — `db/platform.py` defines `Base`, `Model`, `engine`, `SessionFactory`; `db/extensions.py` is the parallel file for the extensions database.
