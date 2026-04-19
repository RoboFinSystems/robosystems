"""ElementLabel model — XBRL label linkbase entries per element.

One element can have multiple labels across different roles (standard,
verbose, terse, documentation, periodStart, periodEnd, negated, total)
and languages. Mirrors XBRL label linkbase semantics.

Tenant-scoped table. Library-origin labels are seeded from the taxonomy
library and replicated into tenant schemas.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
  Text,
  UniqueConstraint,
)

from robosystems.db.extensions import ExtensionsBase


class ElementLabel(ExtensionsBase):
  __tablename__ = "element_labels"
  __table_args__ = (
    UniqueConstraint(
      "element_id",
      "role",
      "language",
      name="uq_element_labels_element_role_language",
    ),
    Index("idx_element_labels_element", "element_id"),
    Index("idx_element_labels_role", "role"),
    CheckConstraint(
      "role IN ("
      "'standard', 'verbose', 'terse', 'documentation', "
      "'periodStart', 'periodEnd', 'negated', 'total', "
      "'commentaryGuidance', 'deprecatedLabel', 'other'"
      ")",
      name="check_element_label_role",
    ),
  )

  # Identity — deterministic UUID5 from (element_id, role, language)
  id = Column(String, primary_key=True)

  # Element reference
  element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # Label properties
  role = Column(String, nullable=False, default="standard")
  language = Column(String, nullable=False, default="en")
  text = Column(Text, nullable=False)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<ElementLabel {self.element_id} [{self.role}/{self.language}]>"
