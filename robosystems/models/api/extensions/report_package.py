"""Report package mode — request/response models.

A Report is the package container (see
``models/extensions/roboledger/report.py`` and
``local/docs/specs/financial-viewer.md`` §"Report Block"). Its items
are its FactSets, found via ``fact_sets.report_id``. Reading a Report
in package mode rehydrates each FactSet into a full
``InformationBlockEnvelope`` so the frontend renders the package
without per-section refetches.

Models live in their own file (instead of ``reports.py``) because
``InformationBlockEnvelope`` lives in ``models.api.information_block``
and that module already pulls from ``models.api.extensions.schedules``;
importing the envelope at module load time of any file aggregated by
``models/api/extensions/__init__.py`` would re-create the circular
import that bit Phase 1.
"""

from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field

from robosystems.models.api.information_block import InformationBlockEnvelope

# ── Request models ─────────────────────────────────────────────────────────


class FileReportRequest(BaseModel):
  """Transition a Report to ``filed`` — locks the package.

  Acceptable from ``draft`` or ``under_review``. ``filed_by`` and
  ``filed_at`` are stamped from the auth context + server clock; the
  request itself carries no fields today (kept as a model for OpenAPI
  shape consistency and to avoid breaking changes if we add fields).
  Use ``transition-filing-status`` for the non-file legs of the
  lifecycle (`draft ↔ under_review`, `filed → archived`).
  """

  report_id: str = Field(..., description="The Report to file.")

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {"report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0"},
      ]
    }
  )


class TransitionFilingStatusRequest(BaseModel):
  """Generic filing-status transition — escape hatch for non-file moves.

  Used for ``draft → under_review`` (submit for review) and
  ``filed → archived`` (supersede / retire). Filing the package goes
  through :class:`FileReportRequest` so ``filed_at`` / ``filed_by``
  audit fields land cleanly.
  """

  report_id: str = Field(..., description="The Report to transition.")
  target_status: str = Field(
    ...,
    description=(
      "Target lifecycle state: `under_review` (submit a draft for "
      "review) or `archived` (supersede / retire a filed report). "
      "Reaching `filed` goes through `file-report` so audit fields "
      "land cleanly."
    ),
  )

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "target_status": "under_review",
        },
        {
          "report_id": "rpt_01HVF8T0M2YTAY3BBNRH0V0",
          "target_status": "archived",
        },
      ]
    }
  )


# ── Response models ────────────────────────────────────────────────────────


class ReportPackageItem(BaseModel):
  """One item in a Report package — pinned envelope + ordering metadata."""

  fact_set_id: str = Field(..., description="The FactSet snapshot pinned to this item.")
  structure_id: str | None = Field(
    None,
    description=(
      "The Structure shape this item renders. Comes from "
      "``fact_sets.structure_id``; null only for legacy rows where the "
      "FactSet wasn't structure-linked."
    ),
  )
  display_order: int = Field(
    0,
    description=(
      "Display position in the package. Derived from "
      "``Structure.structure_type`` ordering (BS → IS → CF → Equity → "
      "Schedule), with ties broken by ``fact_set.created_at``."
    ),
  )
  block: InformationBlockEnvelope = Field(
    ...,
    description=(
      "The rehydrated InformationBlockEnvelope for ``(structure_id, "
      "fact_set_id)``. Pre-built server-side so package-mode renders "
      "without per-item refetches."
    ),
  )


class ReportPackageEnvelope(BaseModel):
  """A Report rehydrated as a package — metadata + N rendered items."""

  id: str
  name: str
  description: str | None = None
  taxonomy_id: str
  period_type: str
  period_start: date | None = None
  period_end: date | None = None

  # Generation lifecycle (computation)
  generation_status: str
  last_generated: datetime | None = None

  # Filing lifecycle (business)
  filing_status: str
  filed_at: datetime | None = None
  filed_by: str | None = None

  # Restatement chain
  supersedes_id: str | None = None
  superseded_by_id: str | None = None

  # Sharing provenance (populated for received reports)
  source_graph_id: str | None = None
  source_report_id: str | None = None
  shared_at: datetime | None = None

  # Authoring
  entity_name: str | None = None
  ai_generated: bool = False
  created_at: datetime
  created_by: str

  # Package items — one per FactSet attached to the Report.
  items: list[ReportPackageItem] = Field(default_factory=list)
