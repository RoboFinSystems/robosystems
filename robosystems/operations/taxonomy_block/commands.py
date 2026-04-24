"""Write commands for Taxonomy Blocks — the generic construction entry.

Three public commands dispatch by ``taxonomy_type`` to the registered
handler: :func:`create_taxonomy_block`, :func:`update_taxonomy_block`,
:func:`delete_taxonomy_block`. No business logic lives here — the
commands are pure routing. Domain-specific mutation lives in the
block-type handler module (``chart_of_accounts.py`` today; future
sub-phases add reporting_extension / custom_ontology / reporting_standard).

These are mounted as the ``create-taxonomy-block`` /
``update-taxonomy-block`` / ``delete-taxonomy-block`` CQRS operations
in ``routers/extensions/roboledger/operations.py``. The REST registrar's
error_map routes ``ValueError → 422`` and ``NotImplementedError → 501``.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  DeleteTaxonomyBlockResponse,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import registry as registry_module


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

  Dispatches on ``body.taxonomy_type``. Unknown types raise
  :class:`ValueError` → 422. The Pydantic-level validation on
  :class:`CreateTaxonomyBlockRequest` (e.g. parent_taxonomy_id required
  for reporting_extension) has already run by the time we get here.
  """
  entry = _get_entry_or_422(body.taxonomy_type)
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

  Dispatch uses the existing taxonomy row's ``taxonomy_type`` to pick
  the handler, not a wire-level discriminator. Block types that can't
  be mutated (``reporting_standard``) raise :class:`NotImplementedError`
  → HTTP 501.
  """
  from robosystems.models.extensions import Taxonomy

  taxonomy = session.get(Taxonomy, body.taxonomy_id)
  if taxonomy is None:
    raise ValueError(f"taxonomy_id {body.taxonomy_id!r} not found")

  entry = _get_entry_or_422(taxonomy.taxonomy_type)
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
  "create_taxonomy_block",
  "delete_taxonomy_block",
  "update_taxonomy_block",
]
