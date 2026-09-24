"""Create/update/delete Taxonomy Block commands, routed by ``taxonomy_type``.

The ``TAXONOMY_AUTHORING_ENABLED`` gate lives here, at the dispatch
chokepoint, so it covers both the REST operations and the MCP tools.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  DeleteTaxonomyBlockResponse,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import registry as registry_module

# Framework-shaped block types: authoring these creates tenant-owned taxonomy
# content that anchors into the library and must survive future library
# evolution. chart_of_accounts is the core product path and is never gated;
# schedule is closing-book machinery; reporting_standard is read-only.
GATED_TAXONOMY_TYPES = frozenset({"reporting_extension", "custom_ontology"})


class TaxonomyAuthoringDisabledError(PermissionError):
  """Framework authoring is disabled in this environment.

  Raised on create/update of a gated taxonomy type when
  ``TAXONOMY_AUTHORING_ENABLED`` is off. Delete is deliberately not
  gated — removing gated content must always be possible.
  """


def _check_authoring_enabled(taxonomy_type: str) -> None:
  if taxonomy_type in GATED_TAXONOMY_TYPES and not env.TAXONOMY_AUTHORING_ENABLED:
    raise TaxonomyAuthoringDisabledError(
      f"authoring {taxonomy_type!r} taxonomy blocks is disabled in this "
      f"environment (TAXONOMY_AUTHORING_ENABLED is off)"
    )


def _get_entry_or_422(taxonomy_type: str):
  """Registry lookup with ``KeyError → ValueError`` for uniform 422 mapping."""
  try:
    return registry_module.get(taxonomy_type)
  except KeyError as exc:
    raise ValueError(str(exc)) from exc


def create_taxonomy_block(
  session: Session,
  body: CreateTaxonomyBlockRequest,
  created_by: str,
) -> TaxonomyBlockEnvelope:
  """Create a taxonomy block and return its full envelope.

  Unknown types raise :class:`ValueError`.
  """
  entry = _get_entry_or_422(body.taxonomy_type)
  _check_authoring_enabled(body.taxonomy_type)
  taxonomy_id = entry.dispatch_create(session, body, created_by)

  envelope = entry.dispatch_build_envelope(session, taxonomy_id)
  if envelope is None:
    raise RuntimeError(
      f"dispatch_build_envelope returned None for freshly-created "
      f"{body.taxonomy_type} block {taxonomy_id}"
    )
  return envelope


def update_taxonomy_block(
  session: Session,
  body: UpdateTaxonomyBlockRequest,
  created_by: str,
) -> TaxonomyBlockEnvelope:
  """Mutate a taxonomy block and return the refreshed envelope.

  Dispatches on the stored row's ``taxonomy_type``; immutable types raise
  :class:`NotImplementedError`.
  """
  from robosystems.models.extensions import Taxonomy
  from robosystems.operations.locking import lock_by_id

  # One update per taxonomy at a time: the apply steps delete-then-recreate
  # and check-then-insert, which race under READ COMMITTED.
  taxonomy = lock_by_id(
    session,
    Taxonomy,
    body.taxonomy_id,
    detail=f"taxonomy {body.taxonomy_id!r} is being updated by another request",
  )
  if taxonomy is None:
    raise ValueError(f"taxonomy_id {body.taxonomy_id!r} not found")

  entry = _get_entry_or_422(taxonomy.taxonomy_type)
  _check_authoring_enabled(taxonomy.taxonomy_type)
  taxonomy_id = entry.dispatch_update(session, body, created_by)

  envelope = entry.dispatch_build_envelope(session, taxonomy_id)
  if envelope is None:
    raise RuntimeError(
      f"dispatch_build_envelope returned None for just-updated "
      f"{taxonomy.taxonomy_type} block {taxonomy_id}"
    )
  return envelope


def delete_taxonomy_block(
  session: Session,
  body: DeleteTaxonomyBlockRequest,
  created_by: str,
) -> DeleteTaxonomyBlockResponse:
  """Delete a taxonomy block and return a thin confirmation response."""
  from robosystems.models.extensions import Taxonomy

  taxonomy = session.get(Taxonomy, body.taxonomy_id)
  if taxonomy is None:
    raise ValueError(f"taxonomy_id {body.taxonomy_id!r} not found")

  entry = _get_entry_or_422(taxonomy.taxonomy_type)
  captured_name = taxonomy.name
  result = entry.dispatch_delete(session, body, created_by)
  facts_deleted = int(result) if isinstance(result, int) else 0

  return DeleteTaxonomyBlockResponse(
    taxonomy_id=body.taxonomy_id,
    name=captured_name,
    facts_deleted=facts_deleted,
    cascade_applied=body.cascade_facts,
  )


__all__ = [
  "GATED_TAXONOMY_TYPES",
  "TaxonomyAuthoringDisabledError",
  "create_taxonomy_block",
  "delete_taxonomy_block",
  "update_taxonomy_block",
]
