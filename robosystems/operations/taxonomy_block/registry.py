"""Taxonomy Block type registry.

Single source of truth for every taxonomy-block type the system knows
about. Populated at module import; frozen thereafter.

Phase 2.2 registers the ``chart_of_accounts`` block only. Later sub-
phases add ``reporting_standard`` (library, read-only),
``reporting_extension`` (extends a library reporting taxonomy), and
``custom_ontology`` (declarative tenant ontology).
"""

from __future__ import annotations

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import chart_of_accounts as coa_handlers
from robosystems.operations.taxonomy_block import (
  custom_ontology as custom_ontology_handlers,
)
from robosystems.operations.taxonomy_block import (
  reporting_extension as reporting_extension_handlers,
)
from robosystems.operations.taxonomy_block import (
  reporting_standard as reporting_standard_handlers,
)
from robosystems.operations.taxonomy_block import (
  schedule_container as schedule_container_handlers,
)
from robosystems.operations.taxonomy_block.types import TaxonomyBlockRegistryEntry

CHART_OF_ACCOUNTS_BLOCK = TaxonomyBlockRegistryEntry(
  id=coa_handlers.COA_BLOCK_TYPE,
  display_name=coa_handlers.COA_DISPLAY_NAME,
  display_plural=coa_handlers.COA_DISPLAY_PLURAL,
  category=coa_handlers.COA_CATEGORY,
  icon="list-tree",
  description=(
    "Tenant-authored chart of accounts. Declares the full envelope "
    "(taxonomy + structures + elements + parent-child associations) in "
    "one atomic call; mappings to library reporting concepts are layered "
    "on via create-mapping-association."
  ),
  construction_mode="declarative",
  requires_parent_taxonomy=False,
  create_request_model=CreateTaxonomyBlockRequest,
  update_request_model=UpdateTaxonomyBlockRequest,
  delete_request_model=DeleteTaxonomyBlockRequest,
  dispatch_create=coa_handlers.create,
  dispatch_update=coa_handlers.update,
  dispatch_delete=coa_handlers.delete,
  dispatch_build_envelope=coa_handlers.build_envelope,
  surfaces_in_library=False,
)

SCHEDULE_CONTAINER_BLOCK = TaxonomyBlockRegistryEntry(
  id=schedule_container_handlers.SCHEDULE_BLOCK_TYPE,
  display_name=schedule_container_handlers.SCHEDULE_DISPLAY_NAME,
  display_plural=schedule_container_handlers.SCHEDULE_DISPLAY_PLURAL,
  category=schedule_container_handlers.SCHEDULE_CATEGORY,
  icon="calendar-clock",
  description=(
    "Thin container grouping Schedule Information Blocks under a single "
    "taxonomy row. The actual Schedule artifacts live as info blocks; "
    "this container carries just name + description."
  ),
  construction_mode="declarative",
  requires_parent_taxonomy=False,
  create_request_model=CreateTaxonomyBlockRequest,
  update_request_model=UpdateTaxonomyBlockRequest,
  delete_request_model=DeleteTaxonomyBlockRequest,
  dispatch_create=schedule_container_handlers.create,
  dispatch_update=schedule_container_handlers.update,
  dispatch_delete=schedule_container_handlers.delete,
  dispatch_build_envelope=schedule_container_handlers.build_envelope,
  surfaces_in_library=False,
)


REPORTING_STANDARD_BLOCK = TaxonomyBlockRegistryEntry(
  id=reporting_standard_handlers.REPORTING_STANDARD_BLOCK_TYPE,
  display_name=reporting_standard_handlers.DISPLAY_NAME,
  display_plural=reporting_standard_handlers.DISPLAY_PLURAL,
  category=reporting_standard_handlers.CATEGORY,
  icon="library",
  description=(
    "Library reporting taxonomy (FAC, us-gaap, rs-gaap). Read-only on "
    "the public surface — seeded via the admin-only library_creator "
    "(operations/taxonomy_block/library_creator.py)."
  ),
  construction_mode="read_only",
  requires_parent_taxonomy=False,
  create_request_model=CreateTaxonomyBlockRequest,
  update_request_model=UpdateTaxonomyBlockRequest,
  delete_request_model=DeleteTaxonomyBlockRequest,
  dispatch_create=reporting_standard_handlers.create,
  dispatch_update=reporting_standard_handlers.update,
  dispatch_delete=reporting_standard_handlers.delete,
  dispatch_build_envelope=reporting_standard_handlers.build_envelope,
  surfaces_in_library=True,
)

REPORTING_EXTENSION_BLOCK = TaxonomyBlockRegistryEntry(
  id=reporting_extension_handlers.REPORTING_EXTENSION_BLOCK_TYPE,
  display_name=reporting_extension_handlers.DISPLAY_NAME,
  display_plural=reporting_extension_handlers.DISPLAY_PLURAL,
  category=reporting_extension_handlers.CATEGORY,
  icon="diff",
  description=(
    "Tenant-authored reporting extension that layers native concepts "
    "under a library reporting_standard (us-gaap, ifrs, ...). Elements "
    "may reference library parents by qname; arcs may point at library "
    "elements. Library rows are never re-created or mutated."
  ),
  construction_mode="extend",
  requires_parent_taxonomy=True,
  create_request_model=CreateTaxonomyBlockRequest,
  update_request_model=UpdateTaxonomyBlockRequest,
  delete_request_model=DeleteTaxonomyBlockRequest,
  dispatch_create=reporting_extension_handlers.create,
  dispatch_update=reporting_extension_handlers.update,
  dispatch_delete=reporting_extension_handlers.delete,
  dispatch_build_envelope=reporting_extension_handlers.build_envelope,
  surfaces_in_library=False,
)

CUSTOM_ONTOLOGY_BLOCK = TaxonomyBlockRegistryEntry(
  id=custom_ontology_handlers.CUSTOM_ONTOLOGY_BLOCK_TYPE,
  display_name=custom_ontology_handlers.DISPLAY_NAME,
  display_plural=custom_ontology_handlers.DISPLAY_PLURAL,
  category=custom_ontology_handlers.CATEGORY,
  icon="sparkles",
  description=(
    "Tenant free-form ontology. Declarative envelope with no "
    "classification or balance-type discipline — for climate "
    "disclosures, KPI definitions, or domain vocabulary that doesn't "
    "need accounting semantics."
  ),
  construction_mode="declarative",
  requires_parent_taxonomy=False,
  create_request_model=CreateTaxonomyBlockRequest,
  update_request_model=UpdateTaxonomyBlockRequest,
  delete_request_model=DeleteTaxonomyBlockRequest,
  dispatch_create=custom_ontology_handlers.create,
  dispatch_update=custom_ontology_handlers.update,
  dispatch_delete=custom_ontology_handlers.delete,
  dispatch_build_envelope=custom_ontology_handlers.build_envelope,
  surfaces_in_library=False,
)


REGISTRY: dict[str, TaxonomyBlockRegistryEntry] = {
  CHART_OF_ACCOUNTS_BLOCK.id: CHART_OF_ACCOUNTS_BLOCK,
  SCHEDULE_CONTAINER_BLOCK.id: SCHEDULE_CONTAINER_BLOCK,
  REPORTING_STANDARD_BLOCK.id: REPORTING_STANDARD_BLOCK,
  REPORTING_EXTENSION_BLOCK.id: REPORTING_EXTENSION_BLOCK,
  CUSTOM_ONTOLOGY_BLOCK.id: CUSTOM_ONTOLOGY_BLOCK,
}


def get(taxonomy_type: str) -> TaxonomyBlockRegistryEntry:
  """Return the registry entry for ``taxonomy_type`` or raise ``KeyError``."""
  try:
    return REGISTRY[taxonomy_type]
  except KeyError as exc:
    raise KeyError(
      f"Unknown taxonomy_type for taxonomy block: {taxonomy_type}"
    ) from exc


def list_registered() -> list[TaxonomyBlockRegistryEntry]:
  """Return every registered entry in registration order."""
  return list(REGISTRY.values())


__all__ = [
  "CHART_OF_ACCOUNTS_BLOCK",
  "CUSTOM_ONTOLOGY_BLOCK",
  "REGISTRY",
  "REPORTING_EXTENSION_BLOCK",
  "REPORTING_STANDARD_BLOCK",
  "SCHEDULE_CONTAINER_BLOCK",
  "get",
  "list_registered",
]
