"""report package lifecycle

Plan-C foundation. Adds the filing lifecycle (draft → under_review →
filed → archived) and restatement chain (supersedes_id /
superseded_by_id) to the existing ``reports`` table, so the Report
itself becomes the package container — no separate ReportBlock concept.
The Report is the unit that materialises to the graph, so unifying the
package + materialisation concerns onto one row keeps the graph version
chain in 1:1 correspondence with the OLTP chain.

``filing_status`` is orthogonal to the existing ``generation_status``:
``generation_status`` tracks computation (pending → generating →
complete → published); ``filing_status`` tracks business state
(draft → under_review → filed → archived). Both are useful and
non-overlapping.

``reports`` is tenant-scoped (schema-per-graph-id), so columns and
indexes are added in the public schema (ORM metadata anchor) and in
every existing tenant schema. New tenants pick up the columns via
``provision_tenant_schema`` from the SQLAlchemy model.

Revision ID: 0006
Revises: 0005
Create Date: 2026-04-25
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


_FILING_STATUS_CHECK = "filing_status IN ('draft', 'under_review', 'filed', 'archived')"


def _add_lifecycle_columns_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)

  t.add_column("reports", "filing_status", "VARCHAR", nullable=False, default="'draft'")
  t.add_column("reports", "filed_at", "TIMESTAMP")
  t.add_column("reports", "filed_by", "VARCHAR")
  t.add_column("reports", "supersedes_id", "VARCHAR")
  t.add_column("reports", "superseded_by_id", "VARCHAR")

  t.add_check(
    "reports",
    f"check_{schema}_report_filing_status",
    _FILING_STATUS_CHECK,
  )

  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_reports_filing_status "
      f"ON {schema}.reports (filing_status)"
    )
  )
  conn.execute(
    text(
      f"CREATE INDEX IF NOT EXISTS idx_{schema}_reports_supersedes "
      f"ON {schema}.reports (supersedes_id)"
    )
  )

  t.add_foreign_key(
    "reports",
    f"fk_{schema}_reports_supersedes",
    ["supersedes_id"],
    "reports",
    ["id"],
  )
  t.add_foreign_key(
    "reports",
    f"fk_{schema}_reports_superseded_by",
    ["superseded_by_id"],
    "reports",
    ["id"],
  )


def _drop_lifecycle_columns_in_tenant(conn, schema: str) -> None:
  t = TenantOps(conn, schema)
  t.drop_constraint("reports", f"fk_{schema}_reports_superseded_by")
  t.drop_constraint("reports", f"fk_{schema}_reports_supersedes")
  t.drop_index(f"idx_{schema}_reports_supersedes")
  t.drop_index(f"idx_{schema}_reports_filing_status")
  t.drop_constraint("reports", f"check_{schema}_report_filing_status")
  t.drop_column("reports", "superseded_by_id")
  t.drop_column("reports", "supersedes_id")
  t.drop_column("reports", "filed_by")
  t.drop_column("reports", "filed_at")
  t.drop_column("reports", "filing_status")


def upgrade() -> None:
  # Public schema — ORM metadata anchor; tenant schemas hold the data.
  op.add_column(
    "reports",
    sa.Column(
      "filing_status",
      sa.String(),
      nullable=False,
      server_default="draft",
    ),
  )
  op.add_column("reports", sa.Column("filed_at", sa.DateTime(), nullable=True))
  op.add_column("reports", sa.Column("filed_by", sa.String(), nullable=True))
  op.add_column("reports", sa.Column("supersedes_id", sa.String(), nullable=True))
  op.add_column("reports", sa.Column("superseded_by_id", sa.String(), nullable=True))
  op.create_check_constraint(
    "check_report_filing_status", "reports", _FILING_STATUS_CHECK
  )
  op.create_index(
    "idx_reports_filing_status", "reports", ["filing_status"], unique=False
  )
  op.create_index("idx_reports_supersedes", "reports", ["supersedes_id"], unique=False)
  op.create_foreign_key(
    "fk_reports_supersedes", "reports", "reports", ["supersedes_id"], ["id"]
  )
  op.create_foreign_key(
    "fk_reports_superseded_by",
    "reports",
    "reports",
    ["superseded_by_id"],
    ["id"],
  )

  conn = op.get_bind()
  for_each_tenant_schema(conn, _add_lifecycle_columns_in_tenant)


def downgrade() -> None:
  conn = op.get_bind()
  for_each_tenant_schema(conn, _drop_lifecycle_columns_in_tenant)

  op.drop_constraint("fk_reports_superseded_by", "reports", type_="foreignkey")
  op.drop_constraint("fk_reports_supersedes", "reports", type_="foreignkey")
  op.drop_index("idx_reports_supersedes", table_name="reports")
  op.drop_index("idx_reports_filing_status", table_name="reports")
  op.drop_constraint("check_report_filing_status", "reports", type_="check")
  op.drop_column("reports", "superseded_by_id")
  op.drop_column("reports", "supersedes_id")
  op.drop_column("reports", "filed_by")
  op.drop_column("reports", "filed_at")
  op.drop_column("reports", "filing_status")
