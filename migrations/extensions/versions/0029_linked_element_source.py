"""admit 'linked' as an element source — concepts that arrive with a shared report

A report shared from another graph carries facts, and those facts cite
concepts. Library concepts resolve on both sides (``copy_library_into_tenant``
gives every tenant the same deterministic UUID5 ids), but the sender's own
reporting extension does not: those are ``elem_*`` ULIDs minted in the
sender's schema alone. ``facts.element_id`` has no foreign key, so the
share wrote them happily and the recipient's next materialization then
failed on the whole database — LadybugDB rejects an edge with an unknown
endpoint, and blue/green answers a partial run by abandoning the WIP.

``_ensure_shared_elements`` now copies the sender's reporting-extension
taxonomies alongside the report. Those copies need a source value that is
**not** in ``COA_SOURCES``: the recipient must be able to read the sender's
concepts without the sender's revenue accounts turning up in the
recipient's own chart of accounts. ``'linked'`` mirrors
``Entity.source='linked'``, which marks the entity the same share creates.

No data changes — this only widens the closed list so the new writes are
legal. The downgrade narrows it again and will fail if any linked concepts
are already present, which is the honest outcome: those rows would
otherwise violate the re-added constraint.

Revision ID: 0029
Revises: 0028
Create Date: 2026-08-09

"""

from __future__ import annotations

from alembic import op

from migrations.extensions.helpers import TenantOps, for_each_tenant_schema

# revision identifiers, used by Alembic.
revision = "0029"
down_revision = "0028"
branch_labels = None
depends_on = None

# Byte-identical to 0024's _SOURCE_WIDENED plus 'linked'.
_SOURCE_WIDENED = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles', 'cm', 'rs-metric', 'rs-driver', "
  "'linked'"
  ")"
)
_SOURCE_ORIGINAL = (
  "source IN ("
  "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
  "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
  "'disclosures', 'checklist', 'styles', 'cm', 'rs-metric', 'rs-driver'"
  ")"
)


def _widen(conn, schema: str) -> None:
  TenantOps(conn, schema).add_check("elements", "check_element_source", _SOURCE_WIDENED)


def _narrow(conn, schema: str) -> None:
  TenantOps(conn, schema).add_check(
    "elements", "check_element_source", _SOURCE_ORIGINAL
  )


def upgrade() -> None:
  conn = op.get_bind()
  _widen(conn, "public")
  for_each_tenant_schema(conn, _widen)


def downgrade() -> None:
  conn = op.get_bind()
  _narrow(conn, "public")
  for_each_tenant_schema(conn, _narrow)
