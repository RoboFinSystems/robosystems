"""The equity roll-forward's header concepts, in the rs-gaap library.

The export presents a roll-forward in the XBRL shape: a header with the
capital concept under ``periodStartLabel``, the movements, and the capital
concept again under ``periodEndLabel`` (``serialization.bundle``
``_apply_preferred_labels``). The header is
``StatementOfStockholdersEquityAbstract`` on the corporate form and
``StatementOfPartnersCapitalAbstract`` on the partnership and LLC forms,
under their us-gaap names.

No Reporting Style network presents them; the engine keeps the capital
concept as the parent of its movements. So they seed ``public`` only and stay
out of every tenant copy (``tenant-exclude/v1.json`` is regenerated with them
excluded); the bundle declares the header it uses.

Revision ID: 0050
Revises: 0049
Create Date: 2026-10-16
"""

from __future__ import annotations

from pathlib import Path

from alembic import op

# revision identifiers, used by Alembic.
revision = "0050"
down_revision = "0049"
branch_labels = None
depends_on = None

# tenant-fanout: not-required (public library elements only; no tenant copies
# them)

_NEW_CONCEPTS = frozenset(
  {
    "rs-gaap:StatementOfStockholdersEquityAbstract",
    "rs-gaap:StatementOfPartnersCapitalAbstract",
  }
)


def _seed_path() -> Path:
  from robosystems.taxonomy.discovery import FRAMEWORKS_DIR

  return FRAMEWORKS_DIR / "rs-gaap" / "packages" / "rs-gaap" / "v1" / "taxonomy.jsonld"


def upgrade() -> None:
  from sqlalchemy.orm import Session as _Session

  from robosystems.operations.taxonomy_block.library_creator import (
    create_library_taxonomy_elements,
  )
  from robosystems.taxonomy import load_taxonomy_package

  rs_gaap = load_taxonomy_package(_seed_path())
  only_new = rs_gaap.model_copy(
    update={"elements": [e for e in rs_gaap.elements if e.qname in _NEW_CONCEPTS]}
  )
  session = _Session(bind=op.get_bind())
  try:
    _taxonomy_id, counts = create_library_taxonomy_elements(session, only_new)
    session.flush()
    print(f"  [rs-gaap] elements={counts['elements']}")
  finally:
    session.close()


def downgrade() -> None:
  """Intentionally a no-op: library content tracks the committed source."""
