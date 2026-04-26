"""GraphQL types for the Report-package read.

The Report is the package container; its items are its FactSets, each
rehydrated as an :class:`InformationBlock`. The resolver lives in
``resolvers/ledger.py`` next to the existing ``report`` field; types
are split into this file because they're hand-written rather than
auto-derived from Pydantic (the ``block`` field needs the manual
``InformationBlock.from_pydantic`` projection).
"""

from __future__ import annotations

import strawberry

from robosystems.graphql.types.information_block import InformationBlock
from robosystems.models.api.extensions.report_package import (
  ReportPackageEnvelope as PydanticReportPackageEnvelope,
)
from robosystems.models.api.extensions.report_package import (
  ReportPackageItem as PydanticReportPackageItem,
)


@strawberry.type
class ReportPackageItem:
  """One item in a Report package — a pinned FactSet rendered as an
  ``InformationBlock`` envelope plus assembly metadata."""

  fact_set_id: str
  structure_id: str | None
  display_order: int
  block: InformationBlock

  @classmethod
  def from_pydantic(cls, item: PydanticReportPackageItem) -> ReportPackageItem:
    return cls(
      fact_set_id=item.fact_set_id,
      structure_id=item.structure_id,
      display_order=item.display_order,
      block=InformationBlock.from_pydantic(item.block),
    )


@strawberry.type
class ReportPackage:
  """A Report rehydrated as a package — metadata + ordered rendered items."""

  id: strawberry.ID
  name: str
  description: str | None
  taxonomy_id: str
  period_type: str
  period_start: str | None
  period_end: str | None

  generation_status: str
  last_generated: str | None

  filing_status: str
  filed_at: str | None
  filed_by: str | None

  supersedes_id: str | None
  superseded_by_id: str | None

  source_graph_id: str | None
  source_report_id: str | None
  shared_at: str | None

  entity_name: str | None
  ai_generated: bool
  created_at: str
  created_by: str

  items: list[ReportPackageItem]

  @classmethod
  def from_pydantic(cls, envelope: PydanticReportPackageEnvelope) -> ReportPackage:
    return cls(
      id=strawberry.ID(envelope.id),
      name=envelope.name,
      description=envelope.description,
      taxonomy_id=envelope.taxonomy_id,
      period_type=envelope.period_type,
      period_start=envelope.period_start.isoformat() if envelope.period_start else None,
      period_end=envelope.period_end.isoformat() if envelope.period_end else None,
      generation_status=envelope.generation_status,
      last_generated=(
        envelope.last_generated.isoformat() if envelope.last_generated else None
      ),
      filing_status=envelope.filing_status,
      filed_at=envelope.filed_at.isoformat() if envelope.filed_at else None,
      filed_by=envelope.filed_by,
      supersedes_id=envelope.supersedes_id,
      superseded_by_id=envelope.superseded_by_id,
      source_graph_id=envelope.source_graph_id,
      source_report_id=envelope.source_report_id,
      shared_at=envelope.shared_at.isoformat() if envelope.shared_at else None,
      entity_name=envelope.entity_name,
      ai_generated=envelope.ai_generated,
      created_at=envelope.created_at.isoformat(),
      created_by=envelope.created_by,
      items=[ReportPackageItem.from_pydantic(it) for it in envelope.items],
    )


__all__ = [
  "ReportPackage",
  "ReportPackageItem",
]
