"""Guard: extensions migrations must fan tenant-table DDL out to every schema.

The extensions database is multi-tenant via schema-per-graph. A new tenant's
tables come from the SQLAlchemy model (``provision_tenant_schema`` →
``create_all``), but an *already-provisioned* tenant only receives a later
migration's change if that migration applies the DDL to every existing tenant
schema via ``for_each_tenant_schema`` (``migrations/extensions/helpers.py``).

A migration that issues ``op.add_column`` / ``op.create_table`` / etc. against a
tenant table WITHOUT that fan-out reaches only ``public``. It is **silent in
dev** (dev tenants are always freshly provisioned at head, so they get the
column from the model) and a **runtime failure in prod** (the column is missing
in every existing customer schema). This guard fails CI before such a migration
ships — the first ``0019+`` against a live tenant is where the trap first bites.

Scope/limitation: this is a file-level heuristic. It catches the common trap —
a migration that issues tenant-table DDL with *no* fan-out at all. It does not
prove that *every* op in a migration is individually fanned out (a migration
that fans out a column but forgets an index would pass). For a genuinely
public-only change to a tenant table, opt out with the marker comment
``# tenant-fanout: not-required``.
"""

from __future__ import annotations

import re
from pathlib import Path

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase

_VERSIONS = (
  Path(__file__).resolve().parents[2] / "migrations" / "extensions" / "versions"
)

# Structural DDL ops (public-schema Alembic ops) that must reach tenant schemas.
_DDL_OPS = (
  "add_column",
  "drop_column",
  "alter_column",
  "create_table",
  "drop_table",
  "rename_table",
  "create_index",
  "drop_index",
  "create_check_constraint",
  "create_unique_constraint",
  "create_foreign_key",
  "create_primary_key",
)
_DDL_RE = re.compile(r"\bop\.(?:" + "|".join(_DDL_OPS) + r")\s*\(")

# Anything that fans a change out to tenant schemas counts as fan-out: the raw
# helper, or the one-call convenience wrappers that call it internally.
_FANOUT_RE = re.compile(
  r"\b(?:for_each_tenant_schema|add_tenant_column|create_tenant_table)\s*\("
)

# Escape hatch for a legitimately public-only change to a tenant table.
_OPT_OUT = "# tenant-fanout: not-required"

# Identifiers appearing as quoted string literals (table/column/index names).
_IDENT_RE = re.compile(r"""["']([A-Za-z_][A-Za-z0-9_]*)["']""")


def _tenant_table_names() -> set[str]:
  """Tables the model places in a tenant schema (schema is None) — the set
  that ``provision_tenant_schema`` builds per tenant, so the set that later
  migrations must fan out to."""
  return {t.name for t in ExtensionsBase.metadata.sorted_tables if t.schema is None}


def _migration_files() -> list[Path]:
  # Match any numbered migration (0001…), not just the 0xxx range, so the
  # guard scans every revision file.
  return sorted(p for p in _VERSIONS.glob("[0-9]*.py"))


def test_versions_dir_and_tenant_set_resolve() -> None:
  assert _migration_files(), "no extensions migrations discovered"
  assert _tenant_table_names(), "no tenant tables resolved from the model"


def test_every_tenant_table_ddl_fans_out() -> None:
  tenant = _tenant_table_names()
  offenders: list[tuple[str, list[str]]] = []

  for path in _migration_files():
    src = path.read_text()
    if _OPT_OUT in src:
      continue
    if not _DDL_RE.search(src):
      continue  # data-only or function-only migration — no structural DDL
    if _FANOUT_RE.search(src):
      continue  # issues a fan-out (raw or via helper) — good
    touched = sorted(set(_IDENT_RE.findall(src)) & tenant)
    if touched:
      offenders.append((path.name, touched))

  assert not offenders, (
    "Extensions migration(s) issue DDL on tenant tables without a "
    "`for_each_tenant_schema` fan-out — the change reaches only `public` and "
    "will be MISSING in every already-provisioned tenant schema:\n"
    + "\n".join(f"  {name}: {tables}" for name, tables in offenders)
    + "\n\nFix: fan the DDL out to tenant schemas via `add_tenant_column(...)` "
    "or `TenantOps` + `for_each_tenant_schema` (see migrations/extensions/"
    "helpers.py; pattern 0015/0017/0018). If the change is genuinely "
    f"public-only, add the marker `{_OPT_OUT}`."
  )
