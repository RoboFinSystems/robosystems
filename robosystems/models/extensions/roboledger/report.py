"""Report model — the package-mode container and unit of materialization.

A Report is a named, period-scoped, lockable container for the FactSets
generated for one reporting period. It is the digital equivalent of a
signed financial-report package ("Q1 2026 Financial Statements") and
the atomic unit that materialises to the graph (one graph Report node
+ its FactSet/Fact nodes per Report row).

The Report has two orthogonal lifecycles:

* ``generation_status`` (pending → generating → complete → published) —
  the *computation* lifecycle. Tracks whether facts have been generated.
* ``filing_status`` (draft → under_review → filed → archived) — the
  *business* lifecycle. Tracks whether the package has been reviewed and
  filed. ``filed`` is the immutable, locked state.

Restatements create a new Report with ``supersedes_id`` pointing at the
prior filed version; the prior row's ``superseded_by_id`` closes the
link. The graph version chain follows the OLTP chain 1:1.

The package-mode viewer renders a Report by loading its attached
FactSets (one per Structure on the Report) and rehydrating each as an
``InformationBlockEnvelope`` via ``get_information_block_for_fact_set``.
"""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  Date,
  DateTime,
  Float,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Report(ExtensionsBase):
  __tablename__ = "reports"
  __table_args__ = (
    Index("idx_reports_taxonomy", "taxonomy_id"),
    Index("idx_reports_status", "generation_status"),
    Index("idx_reports_filing_status", "filing_status"),
    Index("idx_reports_supersedes", "supersedes_id"),
    CheckConstraint(
      "filing_status IN ('draft', 'under_review', 'filed', 'archived')",
      name="check_report_filing_status",
    ),
  )

  # Identity
  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("rpt"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)

  # Taxonomy — determines which structures (IS, BS, CF) are available
  taxonomy_id = Column(String, nullable=False)

  # Configuration
  mapping_id = Column(String, nullable=True)
  period_type = Column(String, nullable=False, default="monthly")
  period_start = Column(Date, nullable=True)
  period_end = Column(Date, nullable=True)
  comparative = Column(Boolean, nullable=False, default=True)

  # Multi-period support — ordered list of period specs for N-column reports
  # Each entry: {"start": "2026-01-01", "end": "2026-01-31", "label": "Jan 2026"}
  # When set, overrides period_start/period_end/comparative for fact generation.
  periods = Column(JSONB, nullable=True)

  # Generated output references. ``generation_count`` monotonically
  # increments on each (re)generation so the exported bundle in object
  # storage stays addressable per-version; ``bundle_url`` points at the
  # latest exported JSON-LD artifact (null pre-serialization-feature).
  graph_report_id = Column(String, nullable=True)
  last_generated = Column(DateTime, nullable=True)
  generation_status = Column(String, nullable=False, default="pending")
  generation_count = Column(Integer, nullable=False, default=0, server_default="0")
  bundle_url = Column(String, nullable=True)

  # Filing lifecycle — orthogonal to generation_status. ``filed`` is
  # the immutable locked state; ``archived`` is for superseded versions.
  filing_status = Column(String, nullable=False, default="draft")
  filed_at = Column(DateTime(timezone=True), nullable=True)
  filed_by = Column(String, nullable=True)

  # Restatement chain — restating a filed Report creates a new row with
  # ``supersedes_id`` pointing at the prior version; the prior row's
  # ``superseded_by_id`` closes the link.
  supersedes_id = Column(String, ForeignKey("reports.id"), nullable=True)
  superseded_by_id = Column(String, ForeignKey("reports.id"), nullable=True)

  # AI provenance
  ai_generated = Column(Boolean, nullable=False, default=False)
  ai_intent = Column(String, nullable=True)
  ai_workspace_id = Column(String, nullable=True)
  ai_confidence = Column(Float, nullable=True)

  # Sharing provenance (populated on received/shared reports, null for local)
  source_graph_id = Column(String, nullable=True)
  source_report_id = Column(String, nullable=True)
  shared_at = Column(DateTime, nullable=True)

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)

  def __repr__(self) -> str:
    return f"<Report {self.name} {self.taxonomy_id}>"
