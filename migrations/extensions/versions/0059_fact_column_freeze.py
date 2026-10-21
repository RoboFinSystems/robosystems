"""Per-fact provenance and the column freeze: a report's columns freeze one
by one as the calendar closes their periods, and a reopen retracts the close's
statement sets instead of deleting them (spec ``fact-lineage`` §9.6).

Four nullable columns, in ``public`` and every tenant:

- ``facts.provenance`` — the fact's own typed descriptor. Backfilled from the
  set's descriptor only where that is exact per fact: every origin but
  ``pivot``, since a statement set pivots its leaves and computes its
  subtotals, and the set's label cannot say which a fact is. A pivot set's
  facts stay NULL, unknown, until the next stamp writes them.
- ``facts.frozen_at`` — when the fact's column froze. A close-stamped set's
  facts froze at the close; a filed report's at filing.
- ``fact_sets.retracted_at`` — a close-stamped set a reopen retired.
- ``reports.bundle_stamped_at`` — when the published holon was built; a report
  that has one was stamped when it was last generated.

Written by hand in the tenant style: columns and the backfill, nothing else.

Revision ID: 0059
Revises: 0058
Create Date: 2026-10-20
"""

from __future__ import annotations

from alembic import op
from sqlalchemy.engine import Connection

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

revision = "0059"
down_revision = "0058"
branch_labels = None
depends_on = None


def _expand(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.add_column("facts", "provenance", "JSONB")
  t.add_column("facts", "frozen_at", "TIMESTAMP WITH TIME ZONE")
  t.add_column("fact_sets", "retracted_at", "TIMESTAMP WITH TIME ZONE")
  t.add_column("reports", "bundle_stamped_at", "TIMESTAMP WITHOUT TIME ZONE")
  t.execute(
    "UPDATE {s}.facts f SET provenance = fs.provenance FROM {s}.fact_sets fs "
    "WHERE f.fact_set_id = fs.id AND f.provenance IS NULL "
    "  AND fs.provenance IS NOT NULL AND fs.provenance->>'origin' <> 'pivot'"
  )
  t.execute(
    "UPDATE {s}.facts f SET frozen_at = fs.created_at AT TIME ZONE 'UTC' "
    "FROM {s}.fact_sets fs "
    "WHERE f.fact_set_id = fs.id AND f.frozen_at IS NULL "
    "  AND fs.report_id IS NULL AND fs.factset_type = 'report' "
    "  AND fs.scenario_id IS NULL"
  )
  t.execute(
    "UPDATE {s}.facts f "
    "SET frozen_at = COALESCE(r.filed_at, r.created_at AT TIME ZONE 'UTC') "
    "FROM {s}.fact_sets fs JOIN {s}.reports r ON r.id = fs.report_id "
    "WHERE f.fact_set_id = fs.id AND f.frozen_at IS NULL "
    "  AND r.filing_status IN ('filed', 'archived')"
  )
  t.execute(
    "UPDATE {s}.reports SET bundle_stamped_at = last_generated "
    "WHERE bundle_url IS NOT NULL AND bundle_stamped_at IS NULL "
    "  AND last_generated IS NOT NULL"
  )


def _contract(conn: Connection, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_column("reports", "bundle_stamped_at")
  t.drop_column("fact_sets", "retracted_at")
  t.drop_column("facts", "frozen_at")
  t.drop_column("facts", "provenance")


def upgrade() -> None:
  conn = op.get_bind()
  _expand(conn, "public")
  for_each_tenant_schema(conn, _expand)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _contract)
  _contract(conn, "public")
