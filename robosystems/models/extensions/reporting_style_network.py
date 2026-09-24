"""ReportingStyleNetwork — Reporting Style → Network composition.

One row per (reporting_style, statement_type): the Network Structure the
renderer uses for that statement under that style. Both ids are
``Structure.id`` values validated in the application (no DB FK); library
immutability triggers block tenant writes to seeded rows.
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
