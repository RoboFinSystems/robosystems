"""Shared helpers for taxonomy block handlers.

Small utilities shared across the CoA / custom_ontology /
reporting_extension handlers. Factored here so the per-block-type
modules don't drift on the common derivations.
"""

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

  Centralizes the request→Structure derivation — block_type,
  concept_arrangement, and the role_uri-rides-in-``metadata_`` convention the
  envelope readers rely on — so the create / update / ontology handlers don't
  drift as request fields are added. The caller still ``session.add``s the row.
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

  Falls back to ``<standard or default_namespace>:<code or name-without-spaces>``
  so qname is always set; the DB column is nullable but the envelope
  treats qname as the canonical identifier.
  """
  ns = standard or default_namespace
  token = code or name.replace(" ", "")
  return f"{ns}:{token}"


__all__ = ["qname_for", "structure_from_request"]
