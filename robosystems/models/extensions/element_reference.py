"""ElementReference model — XBRL reference linkbase entries per element.

Cross-references from elements to authoritative sources (FASB ASC, SEC
Regulation, SFAC, etc). Mirrors XBRL reference linkbase semantics.

Tenant-scoped table. Library-origin references are seeded from the taxonomy
library and replicated into tenant schemas.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Column,
  DateTime,
  ForeignKey,
  Index,
  String,
)

from robosystems.db.extensions import ExtensionsBase


class ElementReference(ExtensionsBase):
  __tablename__ = "element_references"
  __table_args__ = (
    Index("idx_element_references_element", "element_id"),
    Index("idx_element_references_type", "ref_type"),
  )

  # Identity — deterministic UUID5 from (element_id, ref_type, citation)
  id = Column(String, primary_key=True)

  # Element reference
  element_id = Column(String, ForeignKey("elements.id"), nullable=False)

  # Reference properties
  ref_type = Column(String, nullable=True)  # 'ASC', 'SEC', 'SFAC', 'IFRS', 'Other'
  citation = Column(String, nullable=False)  # e.g., 'FASB ASC 842-10-25-1'
  uri = Column(String, nullable=True)  # dereferenceable URL if available

  # Free-form attributes from the source reference linkbase (Publisher, Number,
  # Paragraph, etc.). Stored as a string for flexibility; structured as JSON
  # if the ingest pipeline chooses.
  attributes = Column(String, nullable=True)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  created_by = Column(String, nullable=False, default="system")

  def __repr__(self) -> str:
    return f"<ElementReference {self.element_id} {self.ref_type}:{self.citation}>"
