"""Promote Phase C disclosure packages to is_required=true.

The four Phase C entries (`rs-gaap-disclosures`,
`rs-gaap-disclosure-mechanics`, `rs-gaap-reporting-checklist`, the
`rs-gaap-disclosures-to-rs-gaap-textblocks` bridge) plus
`rs-gaap-reporting-styles` were originally seeded as `is_required=false`
in migration 0007 while authoring was in flight. They are now fully
authored, wired through the framework loader, and load-bearing for
every tenant (every graph carries a `reporting_style_id`; every tenant
receives the disclosure registry via `expand_framework_to_pin`).

The runtime impact of the flag is small — `expand_framework_to_pin`
ignores it — but admin tooling and API surfaces query
``framework_packages`` directly, so the database needs to reflect
reality. This migration updates the existing rows; the
``rs-gaap/v1.json`` manifest is the source of truth and was
flipped alongside it.

Revision ID: 0011
Revises: 0010
Create Date: 2026-05-13
"""

from __future__ import annotations

from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None


_PACKAGES_NOW_REQUIRED = (
  ("rs-gaap-disclosures", "v1"),
  ("rs-gaap-disclosure-mechanics", "v1"),
  ("rs-gaap-reporting-checklist", "v1"),
  ("rs-gaap-reporting-styles", "v1"),
)

_BRIDGES_NOW_REQUIRED = (("rs-gaap-disclosures-to-rs-gaap-textblocks", "v1"),)


def upgrade() -> None:
  conn = op.get_bind()

  for standard, version in _PACKAGES_NOW_REQUIRED:
    conn.execute(
      text(
        """
        UPDATE public.framework_packages
        SET is_required = TRUE
        WHERE package_standard = :standard AND package_version = :version
        """
      ),
      {"standard": standard, "version": version},
    )

  for bridge_name, version in _BRIDGES_NOW_REQUIRED:
    conn.execute(
      text(
        """
        UPDATE public.framework_bridges fb
        SET is_required = TRUE
        FROM public.bridges b
        WHERE fb.bridge_id = b.id
          AND b.bridge = :bridge AND b.version = :version
        """
      ),
      {"bridge": bridge_name, "version": version},
    )


def downgrade() -> None:
  conn = op.get_bind()

  for standard, version in _PACKAGES_NOW_REQUIRED:
    conn.execute(
      text(
        """
        UPDATE public.framework_packages
        SET is_required = FALSE
        WHERE package_standard = :standard AND package_version = :version
        """
      ),
      {"standard": standard, "version": version},
    )

  for bridge_name, version in _BRIDGES_NOW_REQUIRED:
    conn.execute(
      text(
        """
        UPDATE public.framework_bridges fb
        SET is_required = FALSE
        FROM public.bridges b
        WHERE fb.bridge_id = b.id
          AND b.bridge = :bridge AND b.version = :version
        """
      ),
      {"bridge": bridge_name, "version": version},
    )
