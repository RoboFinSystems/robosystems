"""Registry types for Taxonomy Block construction.

Defines :class:`TaxonomyBlockRegistryEntry` — the code-owned descriptor
for each taxonomy block type the system knows about. One entry binds
the block type's display metadata, request/response schemas, and the
four dispatch handlers (create, update, delete, build_envelope) that
make the generic construction and read machinery work.

The registry itself is added in Phase 2.2 as a
``dict[str, TaxonomyBlockRegistryEntry]`` populated at module import.
Adding a block type is a code change — a new registry literal plus the
module that holds its handlers.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

if TYPE_CHECKING:
  from sqlalchemy.orm import Session

  from robosystems.models.api.taxonomy_block import TaxonomyBlockEnvelope


ConstructionMode = Literal["declarative", "extend", "read_only"]


@dataclass(frozen=True)
class TaxonomyBlockRegistryEntry:
  """Declarative description of a single Taxonomy Block type."""

  id: str
  """Stable discriminator matching ``taxonomies.taxonomy_type`` —
  'reporting_standard', 'reporting_extension', 'chart_of_accounts',
  'custom_ontology'."""

  display_name: str
  """Human-readable singular name, e.g. 'Chart of Accounts'."""

  display_plural: str
  """Human-readable plural name, e.g. 'Charts of Accounts'."""

  category: str
  """Sidebar grouping — 'Chart' | 'Reporting' | 'Custom' | 'Library'."""

  icon: str
  """Frontend icon hint (Lucide-style name)."""

  description: str
  """One-paragraph description of the block type. Shown in registry
  documentation and surfaced to agents via MCP."""

  construction_mode: ConstructionMode
  """'declarative' (atoms authored directly — CoA, custom_ontology) /
  'extend' (atoms extend a parent library taxonomy — reporting_extension) /
  'read_only' (library-origin, no tenant writes — reporting_standard)."""

  requires_parent_taxonomy: bool
  """True only for ``reporting_extension`` — the envelope must carry a
  ``parent_taxonomy_id`` referencing the library ``reporting_standard``
  being extended."""

  create_request_model: type[BaseModel]
  """Pydantic model the command dispatcher validates against before
  invoking ``dispatch_create``. Raising ``pydantic.ValidationError``
  here surfaces as a 422 at the API boundary."""

  update_request_model: type[BaseModel]
  """Pydantic model for update-envelope payload validation. Block types
  that don't support updates (e.g. ``reporting_standard``) raise
  :class:`NotImplementedError` from ``dispatch_update`` — the registrar
  translates this to HTTP 501."""

  delete_request_model: type[BaseModel]
  """Pydantic model for delete-envelope payload validation."""

  dispatch_create: Callable[[Session, BaseModel, str], str]
  """Handler that creates the taxonomy. Signature:
  ``(session, typed_payload, created_by) -> taxonomy_id``."""

  dispatch_update: Callable[[Session, BaseModel, str], str]
  """Handler that mutates the taxonomy. Signature:
  ``(session, typed_payload, updated_by) -> taxonomy_id``. Read-only
  block types raise :class:`NotImplementedError`."""

  dispatch_delete: Callable[[Session, BaseModel, str], str | int]
  """Handler that deletes the taxonomy. Signature:
  ``(session, typed_payload, deleted_by) -> facts_deleted`` (int) or
  ``taxonomy_id`` (str) for handlers that don't report cascades. Read-
  only block types raise :class:`NotImplementedError`."""

  dispatch_build_envelope: Callable[[Session, str], TaxonomyBlockEnvelope | None]
  """Handler that reads the taxonomy and packs its envelope. Signature:
  ``(session, taxonomy_id) -> TaxonomyBlockEnvelope | None``. Returns
  None when the taxonomy_id doesn't exist or doesn't belong to this
  block type."""

  surfaces_in_library: bool = False
  """When True, library-sentinel reads surface this block type. Default
  False; ``reporting_standard`` sets True so library browsing returns
  the locked reporting taxonomies."""


__all__ = [
  "ConstructionMode",
  "TaxonomyBlockRegistryEntry",
]
