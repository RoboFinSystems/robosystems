"""API response models for the Information Block envelope.

Wire-facing types for the cross-domain Information Block construct
(see ``local/docs/specs/information-block.md``). Used by the REST
``create-information-block`` operation, the GraphQL ``informationBlock``/
``informationBlocks`` fields, and the MCP read tools.

Phase a ships only the ``schedule`` block type; new block types register
their own ``*Mechanics`` model and add it to the ``ArtifactMechanics``
discriminated union here. The envelope shape itself stays invariant —
adding a block type is a union-arm edit, not an envelope redesign.
"""

from __future__ import annotations

from datetime import date
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

# ── Atom "Lite" projections ────────────────────────────────────────────────


class ElementLite(BaseModel):
  """Element projection for bundling inside an Information Block envelope.

  Narrower than :class:`LibraryElementResponse` — excludes the heavy fields
  (labels, references, classifications) that library browsing needs but
  block consumers don't. Agents + frontends ask for those on demand via
  the full library GraphQL fields when they need them.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  qname: str | None = None
  name: str
  code: str | None = None
  element_type: str = Field(
    ..., description="concept | abstract | axis | member | hypercube"
  )
  is_abstract: bool = False
  is_monetary: bool = True
  balance_type: str | None = None
  period_type: str | None = None


class ConnectionLite(BaseModel):
  """Connection (= Association) projection.

  Renamed at the API boundary to match Charlie's ontology vocabulary.
  The underlying storage is still ``associations``; Phase g keeps that
  table name stable.
  """

  model_config = ConfigDict(from_attributes=True)

  id: str
  from_element_id: str
  to_element_id: str
  association_type: str = Field(
    ...,
    description=(
      "presentation | calculation | mapping | equivalence | "
      "general-special | essence-alias"
    ),
  )
  arcrole: str | None = None
  order_value: float | None = None
  weight: float | None = None


class FactLite(BaseModel):
  """Fact projection — just the values the envelope caller cares about."""

  model_config = ConfigDict(from_attributes=True)

  id: str
  element_id: str
  value: float
  period_start: date | None = None
  period_end: date
  period_type: str
  unit: str = "USD"
  fact_scope: str = Field(..., description="historical | in_scope")
  fact_set_id: str | None = None


# ── Artifact Mechanics — discriminated union on `kind` ─────────────────────


class ScheduleMechanics(BaseModel):
  """Closing-entry generator mechanics for ``block_type='schedule'``.

  Mirrors the shape already stored in ``structures.metadata_`` (fields
  ``entry_template`` + ``schedule_metadata``) for Schedule POC rows.
  Phase d migrates this onto a typed ``artifact_mechanics`` column; in
  Phase a it's read-through validation over the existing JSONB.
  """

  kind: Literal["closing_entry_generator"] = "closing_entry_generator"
  entry_template: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Debit/credit elements + memo template + auto_reverse flag that "
      "drive fact→entry generation for each in-scope period."
    ),
  )
  schedule_metadata: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Method (straight_line / declining_balance / units_of_production), "
      "original_amount, residual_value, useful_life_months, optional "
      "asset_element_id for net-book-value cross-reference."
    ),
  )


# New block-type mechanics models add a `kind` literal and extend this
# union. Pydantic dispatches on `kind` via the discriminator tag.
ArtifactMechanics = Annotated[ScheduleMechanics, Field(discriminator="kind")]


# ── Information Model + Artifact envelope components ──────────────────────


class InformationModelResponse(BaseModel):
  """The block's intrinsic shape — concept + member arrangement patterns."""

  concept_arrangement: str | None = Field(
    None,
    description=(
      "roll_up | roll_forward | variance | adjustment | set | arithmetic | "
      "textblock. Null for block types where the concept arrangement is "
      "implicit in their mechanics."
    ),
  )
  member_arrangement: str | None = Field(
    None, description="aggregation | nonaggregation, or null if non-hypercube."
  )


class ArtifactResponse(BaseModel):
  """The block's producible-artifact envelope — topic, template, mechanics."""

  topic: str | None = Field(
    None, description="Structure.description — the block's human-readable topic."
  )
  parenthetical_note: str | None = Field(
    None, description="e.g. 'in thousands', 'except per share'. Phase d."
  )
  template: dict[str, Any] | None = Field(
    None,
    description=(
      "Reusable layout (ordering, subtotals, styling) when attached. "
      "Phase i delivers first-class templates; null in Phase a."
    ),
  )
  mechanics: ArtifactMechanics


# ── Envelope root ──────────────────────────────────────────────────────────


class InformationBlockEnvelope(BaseModel):
  """The Information Block exchange format.

  One envelope per block instance. Carries the block's identity + type,
  Information-Model attributes, the Artifact branch (mechanics +
  topic/template), and bundled atoms (elements, connections, facts).
  Rules / dimensions / FactSet / verificationResults are present-but-
  empty until the corresponding phases land.
  """

  id: str
  block_type: str = Field(..., description="Discriminator — 'schedule', …")
  name: str
  display_name: str = Field(
    ..., description="Registry-sourced display label (e.g., 'Schedule')."
  )
  category: str = Field(
    ..., description="Registry-sourced sidebar grouping ('Close', 'Reporting', …)."
  )

  information_model: InformationModelResponse
  artifact: ArtifactResponse

  elements: list[ElementLite] = Field(default_factory=list)
  connections: list[ConnectionLite] = Field(default_factory=list)
  facts: list[FactLite] = Field(default_factory=list)

  # Reserved for later phases — declared so the envelope shape is stable.
  rules: list[dict[str, Any]] = Field(default_factory=list)
  dimensions: list[dict[str, Any]] = Field(default_factory=list)
  fact_set: dict[str, Any] | None = None
  verification_results: list[dict[str, Any]] = Field(default_factory=list)


# ── Request models ─────────────────────────────────────────────────────────


class CreateInformationBlockRequest(BaseModel):
  """Generic create request — discriminator + typed-at-dispatch payload.

  ``block_type`` selects the registry entry. ``payload`` is validated
  against ``BlockTypeRegistryEntry.create_request_model`` (e.g.
  :class:`CreateScheduleRequest` for ``block_type='schedule'``) by the
  command dispatcher. Chosen over a Pydantic discriminated union on the
  top-level request so adding a block type is one registry line, not a
  union-arm edit at the request-model layer.
  """

  block_type: str = Field(
    ...,
    description=(
      "Block type discriminator. Must match a registered entry in "
      "robosystems.operations.information_block.registry.REGISTRY."
    ),
  )
  payload: dict[str, Any] = Field(
    default_factory=dict,
    description=(
      "Block-type-specific creation payload. Shape-validated against the "
      "registry entry's `create_request_model` at dispatch time; the "
      "validation error surfaces as a 422 at the API boundary."
    ),
  )


__all__ = [
  "ArtifactMechanics",
  "ArtifactResponse",
  "ConnectionLite",
  "CreateInformationBlockRequest",
  "ElementLite",
  "FactLite",
  "InformationBlockEnvelope",
  "InformationModelResponse",
  "ScheduleMechanics",
]
