"""Shared helpers for taxonomy block handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from robosystems.models.extensions.structure import Structure
from robosystems.operations.roboledger.reports.calc_dag import EXTENDS_NETWORK_KEY

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
  return Structure(
    name=req.name,
    description=req.description,
    block_type=req.block_type,
    concept_arrangement=req.concept_arrangement,
    taxonomy_id=taxonomy_id,
    is_active=True,
    metadata_=structure_metadata(req),
    created_by=created_by,
  )


def structure_metadata(req: TaxonomyBlockStructureRequest) -> dict:
  """The request's metadata with its ``role_uri``. The network a structure
  extends is resolved from ``extends_network`` and checked, never taken from
  the caller's metadata."""
  metadata = {k: v for k, v in req.metadata.items() if k != EXTENDS_NETWORK_KEY}
  if req.role_uri:
    metadata["role_uri"] = req.role_uri
  return metadata


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


__all__ = ["qname_for", "structure_from_request", "structure_metadata"]
