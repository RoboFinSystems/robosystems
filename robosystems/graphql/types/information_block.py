"""Strawberry types for the Information Block GraphQL surface.

Leaf types wrap Pydantic response models via
``@strawberry.experimental.pydantic.type(model=..., all_fields=True)``
— same pattern as :mod:`robosystems.graphql.types.library`. The
top-level :class:`InformationBlock` is hand-written because its
``artifact.mechanics`` field is a discriminated union on ``kind`` and
Strawberry's pydantic decorator can't unwrap union types cleanly; the
``from_pydantic`` classmethod does the construction explicitly.

Phase a ships ``ScheduleMechanicsType`` as the only mechanics variant.
New block types add their ``*MechanicsType`` wrapper and extend the
union in :class:`InformationBlock.artifact_mechanics`'s return type.
"""

from __future__ import annotations

from typing import Any

import strawberry

from robosystems.models.api.information_block import (
  ArtifactResponse as PydanticArtifact,
)
from robosystems.models.api.information_block import (
  ConnectionLite as PydanticConnection,
)
from robosystems.models.api.information_block import (
  ElementLite as PydanticElement,
)
from robosystems.models.api.information_block import (
  FactLite as PydanticFact,
)
from robosystems.models.api.information_block import (
  InformationBlockEnvelope as PydanticInformationBlock,
)
from robosystems.models.api.information_block import (
  InformationModelResponse as PydanticInformationModel,
)

# ── Leaf types — auto-derived from Pydantic ────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticElement, all_fields=True)
class InformationBlockElement:
  """An element bundled inside an Information Block envelope."""


@strawberry.experimental.pydantic.type(model=PydanticConnection, all_fields=True)
class InformationBlockConnection:
  """A connection (association) bundled inside the envelope.

  Renamed from the underlying `associations` at the API boundary to
  match Charlie Hoffman's Seattle Method vocabulary.
  """


@strawberry.experimental.pydantic.type(model=PydanticFact, all_fields=True)
class InformationBlockFact:
  """A fact bundled inside the envelope (period-scoped value)."""


@strawberry.experimental.pydantic.type(model=PydanticInformationModel, all_fields=True)
class InformationModel:
  """Intrinsic shape of the block — concept + member arrangement patterns."""


# The `mechanics` field is exposed as ``scalars.JSON`` with a `kind`
# discriminator in the payload. Phase b will add the second mechanics
# shape (StatementMechanics) and promote this to a typed
# ``strawberry.union(...)``; Pydantic's discriminated union on `kind`
# already shape-validates the payload on the server side.
MechanicsPayload = strawberry.scalars.JSON


# ── Artifact — hand-written so the mechanics JSON is explicit ──────────────


@strawberry.type
class Artifact:
  """The block's producible-artifact envelope (topic, template, mechanics)."""

  topic: str | None
  parenthetical_note: str | None
  template: MechanicsPayload | None
  mechanics: MechanicsPayload

  @classmethod
  def from_pydantic(cls, artifact: PydanticArtifact) -> Artifact:
    return cls(
      topic=artifact.topic,
      parenthetical_note=artifact.parenthetical_note,
      template=artifact.template,
      # Pydantic's discriminated union dumps cleanly to a JSON object
      # including the `kind` tag; the client can branch on that.
      mechanics=artifact.mechanics.model_dump(mode="json"),
    )


# ── Top-level envelope — hand-written so we can override `artifact` ────────


@strawberry.type
class InformationBlock:
  """Information Block envelope — the molecular exchange format.

  See ``local/docs/specs/information-block.md`` §2 for the envelope
  contract. Consumers (agents via MCP, the React `FinancialViewer`,
  SDK clients) receive the same shape regardless of the block_type.
  """

  id: strawberry.ID
  block_type: str
  name: str
  display_name: str
  category: str

  information_model: InformationModel
  artifact: Artifact

  elements: list[InformationBlockElement]
  connections: list[InformationBlockConnection]
  facts: list[InformationBlockFact]

  # Reserved for later phases — typed as JSON until the phase that
  # populates them lands (then each gets its own typed wrapper).
  rules: list[MechanicsPayload]
  dimensions: list[MechanicsPayload]
  fact_set: MechanicsPayload | None
  verification_results: list[MechanicsPayload]

  @classmethod
  def from_pydantic(cls, envelope: PydanticInformationBlock) -> InformationBlock:
    return cls(
      id=strawberry.ID(envelope.id),
      block_type=envelope.block_type,
      name=envelope.name,
      display_name=envelope.display_name,
      category=envelope.category,
      information_model=InformationModel.from_pydantic(envelope.information_model),
      artifact=Artifact.from_pydantic(envelope.artifact),
      elements=[InformationBlockElement.from_pydantic(e) for e in envelope.elements],
      connections=[
        InformationBlockConnection.from_pydantic(c) for c in envelope.connections
      ],
      facts=[InformationBlockFact.from_pydantic(f) for f in envelope.facts],
      rules=[_as_jsonable(r) for r in envelope.rules],
      dimensions=[_as_jsonable(d) for d in envelope.dimensions],
      fact_set=(_as_jsonable(envelope.fact_set) if envelope.fact_set else None),
      verification_results=[_as_jsonable(v) for v in envelope.verification_results],
    )


def _as_jsonable(value: Any) -> Any:
  """Coerce envelope-reserved fields (rules, dimensions, …) to JSON.

  The reserved fields are typed as ``list[dict[str, Any]]`` on the
  Pydantic side so they're already dict-shaped; this helper exists so
  the call site is consistent should a future phase store typed rows
  there.
  """
  return value


__all__ = [
  "Artifact",
  "InformationBlock",
  "InformationBlockConnection",
  "InformationBlockElement",
  "InformationBlockFact",
  "InformationModel",
]
