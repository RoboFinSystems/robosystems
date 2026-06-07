"""ReportingStyleNetwork — Reporting Style → Network composition.

Tenant-scoped junction table. One row per (reporting_style, statement_type)
pair, pointing at the Network Structure the renderer should use for that
statement type when this Reporting Style is in effect.

Library-seeded: each default-family Style (Default / Partnership / LLC)
ships with 4 rows (BS / IS / CF / SE) pinned to canonical rs-gaap
Networks. Additional styles (BSU/NET layouts, single-step IS, direct CF,
vertical profiles) land later as pure package content or customer-authored
Taxonomy Blocks.

No DB foreign key on ``network_id`` / ``reporting_style_id``: both are
``Structure.id`` values, validated at the application layer. Library
immutability triggers (see ``robosystems/db/extensions.py``) block tenant
writes to library-seeded rows.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  String,
)

from robosystems.db.extensions import ExtensionsBase


class ReportingStyleNetwork(ExtensionsBase):
  __tablename__ = "reporting_style_networks"
  __table_args__ = (
    CheckConstraint(
      "statement_type IN ("
      "'balance_sheet', 'income_statement', "
      "'cash_flow_statement', 'equity_statement', "
      "'comprehensive_income'"
      ")",
      name="check_statement_type",
    ),
  )

  reporting_style_id = Column(String, primary_key=True)
  statement_type = Column(String, primary_key=True)
  network_id = Column(String, nullable=False)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="library-seeder")

  def __repr__(self) -> str:
    return (
      f"<ReportingStyleNetwork {self.reporting_style_id}"
      f"[{self.statement_type}] → {self.network_id}>"
    )
