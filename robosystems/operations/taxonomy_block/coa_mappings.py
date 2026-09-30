"""A chart's mappings into reporting frameworks.

Each ``coa_mapping`` Structure is anchored to its own ``mapping`` Taxonomy,
whose ``source_taxonomy_id`` is the chart it maps from and whose
``target_taxonomy_id`` is the framework it maps into. The chart owns those
mapping taxonomies: the chart's Taxonomy Block is the chart plus every mapping
taxonomy sourced from it, and they are created, read, edited and deleted
together. A chart holds at most one active mapping per framework.

Nothing on the Structure names the framework; it is read through the target
taxonomy's ``standard``.
"""

from __future__ import annotations

from sqlalchemy import ColumnElement, delete, or_, select
from sqlalchemy.orm import Session

from robosystems.models.extensions import Structure, Taxonomy
from robosystems.taxonomy.pins import DEFAULT_FRAMEWORK

COA_MAPPING_BLOCK_TYPE = "coa_mapping"
MAPPING_TAXONOMY_TYPE = "mapping"

BOOK_FRAMEWORK = DEFAULT_FRAMEWORK.partition("@")[0]
"""The framework a chart's book mapping targets, until the pin names roles."""


class FrameworkNotInLibraryError(LookupError):
  def __init__(self, framework: str) -> None:
    super().__init__(
      f"Framework {framework!r} is not in this graph's library; "
      "a chart cannot map into it."
    )
    self.framework = framework


class MappingAlreadyExistsError(ValueError):
  def __init__(self, chart_id: str, framework: str) -> None:
    super().__init__(
      f"Chart {chart_id!r} already has a mapping into {framework!r}; "
      "a chart holds one mapping per framework."
    )
    self.chart_id = chart_id
    self.framework = framework


def owned_mapping_taxonomy_ids(chart_id: str):
  """Select the ids of the mapping taxonomies sourced from ``chart_id``."""
  return select(Taxonomy.id).where(
    Taxonomy.taxonomy_type == MAPPING_TAXONOMY_TYPE,
    Taxonomy.source_taxonomy_id == chart_id,
  )


def in_block(taxonomy_id: str) -> ColumnElement[bool]:
  """Structures in the block rooted at ``taxonomy_id``: its own, plus those of
  any mapping taxonomy it owns (only a chart owns any)."""
  return or_(
    Structure.taxonomy_id == taxonomy_id,
    Structure.taxonomy_id.in_(owned_mapping_taxonomy_ids(taxonomy_id)),
  )


def framework_taxonomy_id(session: Session, framework: str) -> str | None:
  """The graph's ``reporting_standard`` taxonomy for ``framework``, newest
  version first."""
  return session.execute(
    select(Taxonomy.id)
    .where(
      Taxonomy.standard == framework,
      Taxonomy.taxonomy_type == "reporting_standard",
    )
    .order_by(Taxonomy.version.desc())
    .limit(1)
  ).scalar_one_or_none()


def find_mapping_structure(
  session: Session,
  framework: str = BOOK_FRAMEWORK,
  *,
  chart_id: str | None = None,
) -> Structure | None:
  """The active mapping into ``framework``: of ``chart_id`` when given,
  otherwise of the graph's earliest chart that has one."""
  source = Taxonomy.__table__.alias("mapping_source")
  target = Taxonomy.__table__.alias("mapping_target")
  query = (
    select(Structure)
    .join(Taxonomy, Structure.taxonomy_id == Taxonomy.id)
    .join(source, Taxonomy.source_taxonomy_id == source.c.id)
    .join(target, Taxonomy.target_taxonomy_id == target.c.id)
    .where(
      Structure.block_type == COA_MAPPING_BLOCK_TYPE,
      Structure.is_active.is_(True),
      Taxonomy.taxonomy_type == MAPPING_TAXONOMY_TYPE,
      Taxonomy.is_active.is_(True),
      target.c.standard == framework,
    )
    .order_by(source.c.created_at, Structure.created_at)
    .limit(1)
  )
  if chart_id is not None:
    query = query.where(Taxonomy.source_taxonomy_id == chart_id)
  return session.execute(query).scalars().first()


def mapping_frameworks(session: Session, structure_ids: list[str]) -> dict[str, str]:
  """``{structure_id: framework}`` for the given mapping structures."""
  if not structure_ids:
    return {}
  target = Taxonomy.__table__.alias("mapping_target")
  rows = session.execute(
    select(Structure.id, target.c.standard)
    .join(Taxonomy, Structure.taxonomy_id == Taxonomy.id)
    .join(target, Taxonomy.target_taxonomy_id == target.c.id)
    .where(
      Structure.id.in_(structure_ids),
      Taxonomy.taxonomy_type == MAPPING_TAXONOMY_TYPE,
    )
  ).all()
  return {str(sid): str(standard) for sid, standard in rows if standard}


def create_mapping_structure(
  session: Session,
  *,
  chart_id: str,
  framework: str,
  name: str,
  created_by: str,
  description: str | None = None,
  concept_arrangement: str | None = None,
  metadata: dict | None = None,
) -> Structure:
  """Create the chart's mapping into ``framework``: its mapping taxonomy and
  the ``coa_mapping`` structure anchored to it. Flushes both."""
  target_id = framework_taxonomy_id(session, framework)
  if target_id is None:
    raise FrameworkNotInLibraryError(framework)
  if find_mapping_structure(session, framework, chart_id=chart_id) is not None:
    raise MappingAlreadyExistsError(chart_id, framework)

  mapping_taxonomy = Taxonomy(
    name=name,
    description=description,
    taxonomy_type=MAPPING_TAXONOMY_TYPE,
    source_taxonomy_id=chart_id,
    target_taxonomy_id=target_id,
    is_shared=False,
    is_active=True,
    is_locked=False,
    metadata_={},
    created_by=created_by,
  )
  session.add(mapping_taxonomy)
  session.flush()

  structure = Structure(
    name=name,
    description=description,
    block_type=COA_MAPPING_BLOCK_TYPE,
    concept_arrangement=concept_arrangement,
    taxonomy_id=mapping_taxonomy.id,
    is_active=True,
    metadata_=dict(metadata or {}),
    created_by=created_by,
  )
  session.add(structure)
  session.flush()
  return structure


def ensure_mapping_structure(
  session: Session,
  *,
  chart_id: str,
  framework: str,
  name: str,
  created_by: str,
  description: str | None = None,
) -> Structure:
  """The chart's mapping into ``framework``, created if it has none."""
  existing = find_mapping_structure(session, framework, chart_id=chart_id)
  if existing is not None:
    return existing
  return create_mapping_structure(
    session,
    chart_id=chart_id,
    framework=framework,
    name=name,
    description=description,
    created_by=created_by,
  )


def prune_empty_mapping_taxonomies(session: Session, chart_id: str) -> None:
  """Delete the chart's mapping taxonomies that no longer hold a structure."""
  session.flush()
  session.execute(
    delete(Taxonomy).where(
      Taxonomy.id.in_(owned_mapping_taxonomy_ids(chart_id)),
      ~Taxonomy.id.in_(select(Structure.taxonomy_id)),
    )
  )


def is_chart_mapping(taxonomy: Taxonomy, block_type: str) -> bool:
  """Whether a structure request on ``taxonomy`` is one of its mappings."""
  return (
    str(taxonomy.taxonomy_type) == "chart_of_accounts"
    and block_type == COA_MAPPING_BLOCK_TYPE
  )


__all__ = [
  "BOOK_FRAMEWORK",
  "COA_MAPPING_BLOCK_TYPE",
  "FrameworkNotInLibraryError",
  "MappingAlreadyExistsError",
  "create_mapping_structure",
  "ensure_mapping_structure",
  "find_mapping_structure",
  "framework_taxonomy_id",
  "in_block",
  "is_chart_mapping",
  "mapping_frameworks",
  "owned_mapping_taxonomy_ids",
  "prune_empty_mapping_taxonomies",
]
