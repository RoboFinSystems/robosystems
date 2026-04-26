"""GraphQL types for the Report-package read.

The Report is the package container; its items are its FactSets, each
rehydrated as an :class:`InformationBlock`. The resolver lives in
``resolvers/ledger.py`` next to the existing ``report`` field; types
are split into this file because they're hand-written rather than
auto-derived from Pydantic (the ``block`` field needs the manual
``InformationBlock.from_pydantic`` projection).

Date / datetime fields use Strawberry's native scalar types so the
GraphQL schema is consistent with the auto-derived ``Report`` type
(which exposes them via Strawberry's pydantic integration). Codegen
on the client side maps these to typed Date / DateTime values.
"""

from __future__ import annotations

import datetime as _dt

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
  period_start: _dt.date | None
  period_end: _dt.date | None

  generation_status: str
  last_generated: _dt.datetime | None

  filing_status: str
  filed_at: _dt.datetime | None
  filed_by: str | None

  supersedes_id: str | None
  superseded_by_id: str | None

  source_graph_id: str | None
  source_report_id: str | None
  shared_at: _dt.datetime | None

  entity_name: str | None
  ai_generated: bool
  created_at: _dt.datetime
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
      period_start=envelope.period_start,
      period_end=envelope.period_end,
      generation_status=envelope.generation_status,
      last_generated=envelope.last_generated,
      filing_status=envelope.filing_status,
      filed_at=envelope.filed_at,
      filed_by=envelope.filed_by,
      supersedes_id=envelope.supersedes_id,
      superseded_by_id=envelope.superseded_by_id,
      source_graph_id=envelope.source_graph_id,
      source_report_id=envelope.source_report_id,
      shared_at=envelope.shared_at,
      entity_name=envelope.entity_name,
      ai_generated=envelope.ai_generated,
      created_at=envelope.created_at,
      created_by=envelope.created_by,
      items=[ReportPackageItem.from_pydantic(it) for it in envelope.items],
    )


__all__ = [
  "ReportPackage",
  "ReportPackageItem",
]
