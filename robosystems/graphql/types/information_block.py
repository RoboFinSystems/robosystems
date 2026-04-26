"""Strawberry types for the Information Block GraphQL surface.

Leaf types wrap Pydantic response models via
``@strawberry.experimental.pydantic.type(model=..., all_fields=True)``
— same pattern as :mod:`robosystems.graphql.types.library`. The
top-level :class:`InformationBlock` is hand-written because its
``artifact.mechanics`` field is a discriminated union on ``kind`` and
Strawberry's pydantic decorator can't unwrap union types cleanly; the
``from_pydantic`` classmethod does the construction explicitly.

The ``mechanics`` field is exposed as ``scalars.JSON`` with a ``kind``
discriminator embedded in the payload. Promoting it to a typed
``strawberry.union(...)`` is deferred until each mechanics arm grows
typed fields worth exposing as a union; until then, clients branch on
the embedded ``kind`` tag.
"""

from __future__ import annotations

import strawberry

from robosystems.models.api.information_block import (
  ArtifactResponse as PydanticArtifact,
)
from robosystems.models.api.information_block import (
  ClassificationLite as PydanticClassification,
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
  FactSetLite as PydanticFactSet,
)
from robosystems.models.api.information_block import (
  InformationBlockEnvelope as PydanticInformationBlock,
)
from robosystems.models.api.information_block import (
  InformationModelResponse as PydanticInformationModel,
)
from robosystems.models.api.information_block import (
  RenderingLite as PydanticRendering,
)
from robosystems.models.api.information_block import (
  RenderingPeriodLite as PydanticRenderingPeriod,
)
from robosystems.models.api.information_block import (
  RenderingRowLite as PydanticRenderingRow,
)
from robosystems.models.api.information_block import (
  RuleLite as PydanticRule,
)
from robosystems.models.api.information_block import (
  RuleTargetLite as PydanticRuleTarget,
)
from robosystems.models.api.information_block import (
  RuleVariableLite as PydanticRuleVariable,
)
from robosystems.models.api.information_block import (
  ValidationLite as PydanticValidation,
)
from robosystems.models.api.information_block import (
  VerificationResultLite as PydanticVerificationResult,
)
from robosystems.models.api.information_block import (
  ViewProjections as PydanticViewProjections,
)

# ── Leaf types — auto-derived from Pydantic ────────────────────────────────


@strawberry.experimental.pydantic.type(model=PydanticElement, all_fields=True)
class InformationBlockElement:
  """An element bundled inside an Information Block envelope."""


@strawberry.experimental.pydantic.type(model=PydanticClassification, all_fields=True)
class InformationBlockClassification:
  """An association-level classification bundled inside the envelope."""


@strawberry.experimental.pydantic.type(model=PydanticConnection, all_fields=True)
class InformationBlockConnection:
  """A connection (association) bundled inside the envelope.

  Renamed from the underlying `associations` at the API boundary to
  match Charlie Hoffman's Seattle Method vocabulary.
  """


@strawberry.experimental.pydantic.type(model=PydanticFact, all_fields=True)
class InformationBlockFact:
  """A fact bundled inside the envelope (period-scoped value)."""


@strawberry.experimental.pydantic.type(model=PydanticFactSet, all_fields=True)
class InformationBlockFactSet:
  """Period-specific instantiation of the Structure."""


@strawberry.experimental.pydantic.type(model=PydanticInformationModel, all_fields=True)
class InformationModel:
  """Intrinsic shape of the block — concept + member arrangement patterns."""


@strawberry.experimental.pydantic.type(model=PydanticRuleTarget, all_fields=True)
class InformationBlockRuleTarget:
  """Polymorphic pointer to the structure/element/association a rule targets."""


@strawberry.experimental.pydantic.type(model=PydanticRuleVariable, all_fields=True)
class InformationBlockRuleVariable:
  """A `$Variable` binding inside a rule expression — name and qname."""


@strawberry.experimental.pydantic.type(model=PydanticRule, all_fields=True)
class InformationBlockRule:
  """A verification rule bundled inside the envelope."""


@strawberry.experimental.pydantic.type(
  model=PydanticVerificationResult, all_fields=True
)
class InformationBlockVerificationResult:
  """Persisted outcome of a rule evaluation."""


@strawberry.experimental.pydantic.type(model=PydanticRenderingRow, all_fields=True)
class InformationBlockRenderingRow:
  """One row of a server-side rendered statement."""


@strawberry.experimental.pydantic.type(model=PydanticRenderingPeriod, all_fields=True)
class InformationBlockRenderingPeriod:
  """One period column in a rendered statement."""


@strawberry.experimental.pydantic.type(model=PydanticValidation, all_fields=True)
class InformationBlockValidation:
  """Outcome of guard-rail validation on a rendered statement."""


@strawberry.experimental.pydantic.type(model=PydanticRendering, all_fields=True)
class InformationBlockRendering:
  """Pre-computed rendering projection — rows + periods + validation."""


@strawberry.experimental.pydantic.type(model=PydanticViewProjections, all_fields=True)
class InformationBlockViewProjections:
  """Charlie's six type-of View arms surfaced in the envelope."""


# Mechanics + template are exposed as ``scalars.JSON`` with a ``kind``
# discriminator embedded in the payload — see the module docstring for
# why this is preferred over a typed Strawberry union.
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

  taxonomy_id: str | None
  taxonomy_name: str | None

  information_model: InformationModel
  artifact: Artifact

  elements: list[InformationBlockElement]
  connections: list[InformationBlockConnection]
  facts: list[InformationBlockFact]
  rules: list[InformationBlockRule]

  # Dimensions stay typed as JSON until the dimension catalog exposes
  # typed fields worth promoting; fact_set + verification_results are
  # typed leaves driven by their Pydantic models above.
  dimensions: list[MechanicsPayload]
  fact_set: InformationBlockFactSet | None
  verification_results: list[InformationBlockVerificationResult]

  view: InformationBlockViewProjections

  @classmethod
  def from_pydantic(cls, envelope: PydanticInformationBlock) -> InformationBlock:
    return cls(
      id=strawberry.ID(envelope.id),
      block_type=envelope.block_type,
      name=envelope.name,
      display_name=envelope.display_name,
      category=envelope.category,
      taxonomy_id=envelope.taxonomy_id,
      taxonomy_name=envelope.taxonomy_name,
      information_model=InformationModel.from_pydantic(envelope.information_model),
      artifact=Artifact.from_pydantic(envelope.artifact),
      elements=[InformationBlockElement.from_pydantic(e) for e in envelope.elements],
      connections=[
        InformationBlockConnection.from_pydantic(c) for c in envelope.connections
      ],
      facts=[InformationBlockFact.from_pydantic(f) for f in envelope.facts],
      rules=[InformationBlockRule.from_pydantic(r) for r in envelope.rules],
      # dimensions still passes through as JSON; fact_set and
      # verification_results are typed leaves driven by their Pydantic
      # arms.
      dimensions=list(envelope.dimensions),
      fact_set=(
        InformationBlockFactSet.from_pydantic(envelope.fact_set)
        if envelope.fact_set is not None
        else None
      ),
      verification_results=[
        InformationBlockVerificationResult.from_pydantic(vr)
        for vr in envelope.verification_results
      ],
      view=InformationBlockViewProjections.from_pydantic(envelope.view),
    )


__all__ = [
  "Artifact",
  "InformationBlock",
  "InformationBlockClassification",
  "InformationBlockConnection",
  "InformationBlockElement",
  "InformationBlockFact",
  "InformationBlockFactSet",
  "InformationBlockRendering",
  "InformationBlockRenderingPeriod",
  "InformationBlockRenderingRow",
  "InformationBlockRule",
  "InformationBlockRuleTarget",
  "InformationBlockRuleVariable",
  "InformationBlockValidation",
  "InformationBlockVerificationResult",
  "InformationBlockViewProjections",
  "InformationModel",
]
