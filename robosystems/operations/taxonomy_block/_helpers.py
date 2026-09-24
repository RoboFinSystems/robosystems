"""Shared helpers for taxonomy block handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.models.extensions.structure import Structure

if TYPE_CHECKING:
  from robosystems.models.api.taxonomy_block import TaxonomyBlockStructureRequest


def structure_from_request(
  req: TaxonomyBlockStructureRequest,
  *,
  taxonomy_id: str,
  created_by: str,
) -> Structure:
  """Project a structure request onto a new ``Structure`` row.

  ``role_uri`` rides in ``metadata_``. The caller ``session.add``s the row.
  """
  structure_metadata = dict(req.metadata)
  if req.role_uri:
    structure_metadata["role_uri"] = req.role_uri
  return Structure(
    name=req.name,
    description=req.description,
    block_type=req.block_type,
    concept_arrangement=req.concept_arrangement,
    taxonomy_id=taxonomy_id,
    is_active=True,
    metadata_=structure_metadata,
    created_by=created_by,
  )


def qname_for(
  standard: str | None,
  default_namespace: str,
  code: str | None,
  name: str,
) -> str:
  """Derive the envelope-local qname when the tenant didn't supply one.

  ``<standard or default_namespace>:<code or name-without-spaces>``; the
  envelope treats qname as the identifier though the column is nullable.
  """
  ns = standard or default_namespace
  token = code or name.replace(" ", "")
  return f"{ns}:{token}"


__all__ = ["qname_for", "structure_from_request"]
