"""Registry types for Information Block construction.

Defines :class:`BlockTypeRegistryEntry` — the code-owned descriptor for
each block type the system knows about. One entry binds the block type's
display metadata, default Information Model values, typed mechanics and
create-request schemas, and the two dispatch handlers (``create`` and
``build_envelope``) that make the generic construction machinery work.

The registry itself (``registry.py``) is a ``dict[str, BlockTypeRegistryEntry]``
populated at module import. Adding a block type is a code change — a new
``BlockTypeRegistryEntry`` literal plus the module that holds its
handlers. No DB rows, no runtime registration.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.information_block import InformationBlockEnvelope


ConstructionMode = Literal["declarative", "compositional", "derivative"]


@dataclass(frozen=True)
class BlockTypeRegistryEntry:
  """Declarative description of a single Information Block type.

  One entry per block type. Instantiated at module scope in
  ``registry.py`` and looked up by ``id`` when the generic construction
  or read paths dispatch.
  """

  id: str
  """Stable discriminator — e.g. 'schedule'. Matches the block_type
  string on the wire and the structure_type value in the DB (Phase γ
  renames the column)."""

  display_name: str
  """Human-readable singular name, e.g. 'Schedule'."""

  display_plural: str
  """Human-readable plural name, e.g. 'Schedules'."""

  category: str
  """Sidebar grouping — e.g. 'Close', 'Reporting', 'Disclosures'."""

  icon: str
  """Frontend icon hint (Lucide-style name)."""

  description: str
  """One-paragraph description of the block type. Shown in registry
  documentation and surfaced to agents via the MCP read tool."""

  concept_arrangement_default: str | None
  """Default concept arrangement pattern for this block type
  (roll_up, roll_forward, variance, …). None when the pattern is
  implicit in the mechanics."""

  member_arrangement_default: str | None
  """Default member arrangement pattern (aggregation / nonaggregation
  / None)."""

  mechanics_schema: type[BaseModel]
  """Pydantic model for the block's Artifact Mechanics (e.g.
  :class:`ScheduleMechanics`). Used by the envelope builder to
  shape-validate what lives in the metadata_ JSONB (Phase a) or in
  the typed ``artifact_mechanics`` column (Phase d)."""

  create_request_model: type[BaseModel]
  """Pydantic model that the ``create-information-block`` dispatcher
  validates the opaque ``payload`` against before invoking the
  handler. Raising ``pydantic.ValidationError`` here surfaces as a
  422 at the API boundary."""

  update_request_model: type[BaseModel]
  """Pydantic model for ``update-information-block`` payload validation.
  For block types whose ``dispatch_update`` always raises (e.g. the
  statement family), this can be a placeholder like
  ``_EmptyUpdateRequest``."""

  delete_request_model: type[BaseModel]
  """Pydantic model for ``delete-information-block`` payload validation.
  Typically carries just the structure_id."""

  construction_mode: ConstructionMode
  """Declarative (user declares shape + seed params; atoms generated) /
  compositional (atoms exist from ingest; block is a view materialized
  on read) / derivative (atoms come from other blocks via rules).
  Shapes what the dispatcher does — Phase a only ships declarative."""

  dispatch_create: Callable[[Session, BaseModel, str], str]
  """Handler that creates the block. Signature:
  ``(session, typed_payload, created_by) -> structure_id``.
  Typically delegates to an existing domain command (for Schedule,
  this delegates to ``cmd_create_schedule``)."""

  dispatch_update: Callable[[Session, BaseModel, str], str]
  """Handler that updates the block in place. Signature:
  ``(session, typed_payload, updated_by) -> structure_id``.
  Block types that don't support updates (library-seeded statement
  structures, e.g.) raise :class:`NotImplementedError` — the registrar
  translates this to HTTP 501."""

  dispatch_delete: Callable[[Session, BaseModel, str], str]
  """Handler that deletes the block. Signature:
  ``(session, typed_payload, deleted_by) -> structure_id``.
  Returns the deleted structure_id for the response envelope. Block
  types that don't support deletion raise :class:`NotImplementedError`."""

  dispatch_build_envelope: Callable[[Session, str], InformationBlockEnvelope | None]
  """Handler that reads the block and packs its envelope. Signature:
  ``(session, structure_id) -> InformationBlockEnvelope | None``.
  Returns None when the structure_id doesn't exist or its row doesn't
  belong to this block type."""

  surfaces_in_library: bool = False
  """When True, ``list_information_blocks`` surfaces this block type on
  the library sentinel (``graph_id='library'``). Default False so
  canonical library structures (CoA templates, etc.) don't leak as
  block envelopes until their block type is registered and modeled."""


__all__ = [
  "BlockTypeRegistryEntry",
  "ConstructionMode",
]
