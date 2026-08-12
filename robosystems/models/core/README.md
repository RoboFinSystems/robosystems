# Platform Database Models

SQLAlchemy models for the **platform PostgreSQL database** (`robosystems`) — users, organizations, graphs, billing, connections, and documents. Everything that must exist before an extension (RoboLedger, RoboInvestor) can provision itself for a tenant lives here.

## The two databases

RoboSystems runs against two logically separate PostgreSQL databases that share one RDS instance in production:

| Database | Purpose | Base class | Models | Migration config |
| -------- | ------- | ---------- | ------ | ---------------- |
| `robosystems` (**platform**) | IAM, billing, graph metadata, connections, documents | `Model` / `Base` from `robosystems.database` | `robosystems/models/core/` (this directory) | `migrations/platform.ini` |
| `extensions` | Per-graph OLTP for product extensions | `ExtensionsBase` from `robosystems.db.extensions` | `robosystems/models/extensions/` | `migrations/extensions.ini` |

They use **different `DeclarativeBase` classes**, so a model inheriting from `Model` can never land in the extensions database, and vice versa. Migration histories are independent: `just migrate-up` migrates the platform, `just migrate-up extensions` migrates extensions, and neither touches the other.

For the extensions side, see [`../extensions/README.md`](../extensions/README.md).

## Layout

| Subpackage | Contents |
| ---------- | -------- |
| `billing/` | `BillingCustomer`, `BillingSubscription`, `BillingInvoice` + line items, `BillingAuditLog` |
| `connection/` | `Connection` (provider, graph, sync state, write policy) and `ConnectionCredentials` (Fernet-encrypted OAuth tokens) |
| `document/` | `Document` — metadata for uploaded markdown/text indexed in OpenSearch |
| `graph/` | `Graph`, `GraphBackup`, `GraphCredits` + `GraphCreditTransaction`, `GraphFile`, `GraphSchema`, `GraphTable`, `GraphUsage`, `GraphUser`, `SourceFile` |
| `org/` | `Org`, `OrgUser` (membership + role), `OrgInvitation`, `OrgLimits` |
| `user/` | `User`, `UserAPIKey`, `UserToken`, `UserIdentity`, `ScimToken`, `UserRepository`, and shared-repository credits |

`__init__.py` re-exports everything flat, so `from robosystems.models.core import User, Graph` works without reaching into subpackages.

## Base class

```python
from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid

class User(Model):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("user"))
    email = Column(String, unique=True, nullable=False, index=True)
```

`robosystems.database` is a thin re-export of `robosystems.db.platform`, which defines `Base`, `Model`, `engine`, and `SessionFactory`. New code can import from either path.

## Conventions

- **Primary keys are prefixed ULIDs** — `user_01H…`, `graph_01H…`, `conn_01H…`, via `generate_prefixed_ulid(prefix)`. Never auto-increment integers.
- **Timestamps are UTC-aware** — `DateTime` columns default to `lambda: datetime.now(UTC)`. Never store naive datetimes.
- **Enums are stored as strings**, not PostgreSQL `ENUM` types, so new values don't require a migration. The Python enum in the model file is a convenience for callers, not a database constraint. `UserRepository.repository_type` is the canonical example — new shared repositories land via adapter manifests with no schema change.
- **Relationships use `back_populates`**, not `backref`, so both sides are explicit and type checkers can follow them.
- **Subpackage `__init__.py` files import each model explicitly**, which keeps the flat re-export free of circular imports.

## Migrations

Always autogenerate — never hand-write a migration.

```bash
# 1. Edit a SQLAlchemy model in this directory
# 2. Autogenerate against the current DB state
just migrate-create "add foo column to users"

# 3. Review the generated file under migrations/platform/versions/
#    Autogenerate misses enum changes, CHECK constraints, and some index
#    changes — add those by hand before applying.

# 4. Apply
just migrate-up
```

Other recipes: `just migrate-current`, `just migrate-history`, `just migrate-down` (rollback one, local only), `just migrate-reset` (downgrade to base and re-upgrade — destructive, local only). Each takes an optional database argument that defaults to `platform`.

## How these models are used

- **`Graph` owns `graph_id`.** Tier, status, schema-extension list, and subgraph parentage all live there. Graph routing, billing, rate limiting, and extensions session scoping all resolve graphs through this model.
- **`Connection` is PostgreSQL-only** — there is no Connection node in the graph. It carries the provider, sync state, `last_cdc_watermark` for delta sync, `deleted_at` for soft delete, and the outbound `write_policy` (`native` / `qb_authoritative` / `hybrid`). `default_write_policy_for_provider` picks the default: QuickBooks gets `qb_authoritative`, everything else `native`.
- **Credits are a platform concern.** `GraphCredits` and `GraphCreditTransaction` hold the balance and ledger; billing webhooks write through them, and extensions consume credits via `operations/graph/credit_service.py`.
- **Shared repository access** lives on `user/user_repository.py`, with its own credit pool in `user_repository_credits.py`.

## Related

- [Extensions OLTP Models](../extensions/README.md) — the other half of the model tree, with schema-per-graph-id tenancy.
- [API Models](../api/README.md) — Pydantic request/response shapes. Routers talk to those; they reach these models only through the operations layer.
- `robosystems/db/` — `platform.py` defines `Base` / `Model` / `engine` / `SessionFactory`; `extensions.py` is its counterpart for the extensions database.
