"""Copy library content from ``public`` into a tenant schema.

Bulk ``INSERT ... SELECT`` in FK order, keeping the deterministic UUID5 ids
so re-running is idempotent. Library rows carry ``created_by =
'library-seeder'``, which the tenant immutability triggers key on.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache

from sqlalchemy import text
from sqlalchemy.engine import Connection

from robosystems.taxonomy.discovery import FRAMEWORKS_DIR
from robosystems.taxonomy.pins import DEFAULT_TAXONOMY_PIN

_TENANT_EXCLUDE_PATH = FRAMEWORKS_DIR / "rs-gaap" / "tenant-exclude" / "v1.json"


@lru_cache(maxsize=1)
def _tenant_exclude_qnames() -> tuple[str, ...]:
  """rs-gaap qnames kept in ``public`` but not copied to tenants (industry
  verticals, dimension members, concepts the renderer never reaches); from
  ``scripts/generate_tenant_exclude.py``. Empty when the artifact is absent.
  """
  try:
    data = json.loads(_TENANT_EXCLUDE_PATH.read_text())
  except FileNotFoundError:
    return ()
  return tuple(data.get("excluded_qnames", ()))


_TAXONOMY_COLS = (
  "id, name, description, taxonomy_type, version, standard, namespace_uri, "
  "is_shared, source_taxonomy_id, target_taxonomy_id, parent_taxonomy_id, "
  "extension_type, effective_date, is_active, is_locked, metadata, "
  "created_at, updated_at, created_by"
)

_ELEMENT_COLS = (
  "id, code, name, description, qname, namespace, uri, "
  "balance_type, period_type, substitution_group, is_abstract, is_monetary, "
  "element_type, item_type, parent_id, depth, path, taxonomy_id, source, currency, "
  "is_active, is_placeholder, external_id, external_source, "
  "agent_id, aliases, embedding, "
  "metadata, version, created_at, updated_at, created_by"
)

_ELEMENT_LABEL_COLS = "id, element_id, role, language, text, created_at, created_by"

_ELEMENT_REFERENCE_COLS = (
  "id, element_id, ref_type, citation, uri, attributes, created_at, created_by"
)

_STRUCTURE_COLS = (
  "id, name, description, block_type, taxonomy_id, graph_structure_id, "
  "is_active, concept_arrangement, member_arrangement, artifact_mechanics, "
  "renderer_note, template_id, metadata, created_at, updated_at, created_by"
)

_ASSOCIATION_COLS = (
  "id, structure_id, from_element_id, to_element_id, association_type, "
  "arcrole, order_value, weight, confidence, suggested_by, approved_by, "
  "approved_at, metadata, created_at, updated_at, created_by"
)

_TRAIT_COLS = (
  "id, category, identifier, type, name, description, confidence, source, "
  "metadata, created_at, updated_at, created_by"
)

_ELEMENT_TRAIT_COLS = (
  "element_id, trait_id, is_primary, confidence, source, "
  "created_at, updated_at, created_by"
)

_CLASSIFICATION_COLS = (
  "id, category, identifier, type, name, description, confidence, source, "
  "metadata, created_at, updated_at, created_by"
)

_ASSOC_CLASSIFICATION_COLS = (
  "association_id, classification_id, is_primary, confidence, source, "
  "created_at, updated_at, created_by"
)

_RULE_COLS = (
  "id, taxonomy_id, rule_category, rule_pattern, rule_check_kind, "
  "rule_expression, rule_message, rule_severity, rule_origin, target_kind, "
  "target_structure_id, target_element_id, target_association_id, "
  "target_taxonomy_id, rule_variables, metadata, created_at, updated_at, created_by"
)

_REPORTING_STYLE_NETWORK_COLS = (
  "reporting_style_id, statement_type, network_id, created_at, created_by"
)


@dataclass(frozen=True)
class CopyStats:
  """Row counts inserted per table during a library → tenant copy."""

  taxonomies: int
  elements: int
  element_labels: int
  element_references: int
  structures: int
  associations: int
  traits: int
  element_traits: int
  classifications: int
  association_classifications: int
  rules: int
  reporting_style_networks: int

  @property
  def total(self) -> int:
    return (
      self.taxonomies
      + self.elements
      + self.element_labels
      + self.element_references
      + self.structures
      + self.associations
      + self.traits
      + self.element_traits
      + self.classifications
      + self.association_classifications
      + self.rules
      + self.reporting_style_networks
    )


# Transaction-scoped GUC the immutability triggers consult; a re-sync caller
# runs SET_LIBRARY_RESYNC in the same transaction first.
LIBRARY_RESYNC_GUC = "robosystems.library_resync"
SET_LIBRARY_RESYNC = f"SET LOCAL {LIBRARY_RESYNC_GUC} = 'on'"


def _build_pin_clause(resolved_pin: dict[str, str]) -> tuple[str, dict[str, str]]:
  """``{"fac":"v1","rs-gaap":"v1"}`` → ``"(:s0, :v0), (:s1, :v1)"`` + params."""
  pin_values_sql = ", ".join(f"(:s{i}, :v{i})" for i in range(len(resolved_pin)))
  pin_params: dict[str, str] = {}
  for i, (std, ver) in enumerate(resolved_pin.items()):
    pin_params[f"s{i}"] = std
    pin_params[f"v{i}"] = ver
  return pin_values_sql, pin_params


def _assoc_endpoints_present(schema: str) -> str:
  """SQL fragment skipping an arc unless both endpoint elements are already in
  the tenant: a copied package's arc may point into a package that is not
  copied, which would violate the NOT NULL FKs. Relies on elements being
  inserted first; the source row must be aliased ``a``.
  """
  return (
    f" AND EXISTS (SELECT 1 FROM {schema}.elements e WHERE e.id = a.from_element_id)"
    f" AND EXISTS (SELECT 1 FROM {schema}.elements e WHERE e.id = a.to_element_id)"
  )


def copy_library_into_tenant(
  connection: Connection,
  schema: str,
  pin: dict[str, str] | None = None,
) -> CopyStats:
  """Bulk-copy pinned library taxonomies from ``public.*`` into ``{schema}.*``.

  Copies every library table in FK order with ``ON CONFLICT DO NOTHING``,
  so re-running never duplicates or updates. Does not commit. ``pin`` is
  ``{standard: version}``, defaulting to :data:`DEFAULT_TAXONOMY_PIN`.
  """
  resolved_pin = pin if pin is not None else DEFAULT_TAXONOMY_PIN
  if not resolved_pin:
    return CopyStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  pin_values_sql, pin_params = _build_pin_clause(resolved_pin)

  tax_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.taxonomies ({_TAXONOMY_COLS})
      SELECT {_TAXONOMY_COLS} FROM public.taxonomies
      WHERE (standard, version) IN (VALUES {pin_values_sql})
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Dependent rows below key off what landed in {schema}.elements, so the
  # exclusion set drops their dependents too.
  exclude_qnames = _tenant_exclude_qnames()
  exclude_clause = "AND e.qname != ALL(:exclude_qnames)" if exclude_qnames else ""
  elem_params = (
    {**pin_params, "exclude_qnames": list(exclude_qnames)}
    if exclude_qnames
    else pin_params
  )
  elem_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.elements ({_ELEMENT_COLS})
      SELECT {_ELEMENT_COLS} FROM public.elements e
      WHERE e.taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      {exclude_clause}
      ON CONFLICT (id) DO NOTHING
    """),
    elem_params,
  )

  label_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_labels ({_ELEMENT_LABEL_COLS})
      SELECT {_ELEMENT_LABEL_COLS} FROM public.element_labels
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      ON CONFLICT (id) DO NOTHING
    """),
  )

  ref_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_references ({_ELEMENT_REFERENCE_COLS})
      SELECT {_ELEMENT_REFERENCE_COLS} FROM public.element_references
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      ON CONFLICT (id) DO NOTHING
    """),
  )

  struct_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.structures ({_STRUCTURE_COLS})
      SELECT {_STRUCTURE_COLS} FROM public.structures
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  assoc_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.associations ({_ASSOCIATION_COLS})
      SELECT {_ASSOCIATION_COLS} FROM public.associations a
      WHERE a.structure_id IN (
        SELECT id FROM public.structures
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      {_assoc_endpoints_present(schema)}
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Traits and classifications are pin-independent: every tenant gets all.
  trait_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.traits ({_TRAIT_COLS})
      SELECT {_TRAIT_COLS} FROM public.traits
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  et_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_traits ({_ELEMENT_TRAIT_COLS})
      SELECT {_ELEMENT_TRAIT_COLS} FROM public.element_traits
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      ON CONFLICT (element_id, trait_id) DO NOTHING
    """),
  )

  cls_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.classifications ({_CLASSIFICATION_COLS})
      SELECT {_CLASSIFICATION_COLS} FROM public.classifications
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  ac_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.association_classifications ({_ASSOC_CLASSIFICATION_COLS})
      SELECT {_ASSOC_CLASSIFICATION_COLS} FROM public.association_classifications
      WHERE association_id IN (SELECT id FROM {schema}.associations)
      ON CONFLICT (association_id, classification_id) DO NOTHING
    """),
  )

  # Rules reference structures / elements / associations polymorphically.
  rule_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.rules ({_RULE_COLS})
      SELECT {_RULE_COLS} FROM public.rules
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Only rows whose Style and Network structures both landed in the tenant.
  rsn_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.reporting_style_networks ({_REPORTING_STYLE_NETWORK_COLS})
      SELECT {_REPORTING_STYLE_NETWORK_COLS} FROM public.reporting_style_networks rsn
      WHERE EXISTS (SELECT 1 FROM {schema}.structures s WHERE s.id = rsn.reporting_style_id)
        AND EXISTS (SELECT 1 FROM {schema}.structures s WHERE s.id = rsn.network_id)
      ON CONFLICT (reporting_style_id, statement_type) DO NOTHING
    """),
  )

  return CopyStats(
    taxonomies=tax_result.rowcount or 0,
    elements=elem_result.rowcount or 0,
    element_labels=label_result.rowcount or 0,
    element_references=ref_result.rowcount or 0,
    structures=struct_result.rowcount or 0,
    associations=assoc_result.rowcount or 0,
    traits=trait_result.rowcount or 0,
    element_traits=et_result.rowcount or 0,
    classifications=cls_result.rowcount or 0,
    association_classifications=ac_result.rowcount or 0,
    rules=rule_result.rowcount or 0,
    reporting_style_networks=rsn_result.rowcount or 0,
  )


# Re-sync ON CONFLICT clauses. The updatable columns must match
# operations/taxonomy_block/library_creator.py exactly; frozen tables stay DO
# NOTHING. The statements must stay in lockstep with copy_library_into_tenant
# (same tables, FK order and WHERE clauses); only the conflict action differs.
_RESYNC_TAXONOMY_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "taxonomy_type = EXCLUDED.taxonomy_type, description = EXCLUDED.description"
)
_RESYNC_ELEMENT_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "balance_type = EXCLUDED.balance_type, period_type = EXCLUDED.period_type, "
  "substitution_group = EXCLUDED.substitution_group, "
  "is_abstract = EXCLUDED.is_abstract, is_monetary = EXCLUDED.is_monetary, "
  "element_type = EXCLUDED.element_type, item_type = EXCLUDED.item_type"
)
_RESYNC_TRAIT_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "name = EXCLUDED.name, description = EXCLUDED.description"
)
_RESYNC_ELEMENT_TRAIT_CONFLICT = (
  "ON CONFLICT (element_id, trait_id) DO UPDATE SET "
  "is_primary = EXCLUDED.is_primary, confidence = EXCLUDED.confidence, "
  "source = EXCLUDED.source"
)
_RESYNC_RULE_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "rule_expression = EXCLUDED.rule_expression, "
  "rule_message = EXCLUDED.rule_message, "
  "rule_severity = EXCLUDED.rule_severity, "
  "rule_variables = EXCLUDED.rule_variables"
)
# The id is uuid5(structure:from:to:type), so value fixes update in place;
# changing from/to/type mints a new arc and the stale one lingers.
_RESYNC_ASSOCIATION_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "weight = EXCLUDED.weight, order_value = EXCLUDED.order_value, "
  "arcrole = EXCLUDED.arcrole, confidence = EXCLUDED.confidence, "
  "metadata = EXCLUDED.metadata"
)


def resync_library_into_tenant(
  connection: Connection,
  schema: str,
  pin: dict[str, str] | None = None,
) -> CopyStats:
  """Re-sync pinned library taxonomies from ``public.*`` into ``{schema}.*``.

  The ``DO UPDATE`` sibling of :func:`copy_library_into_tenant`: library
  fixes reach provisioned tenants. Mutable columns update in place; labels,
  references, structures, classifications and style networks are additive
  only; **nothing is ever deleted**.

  Does not commit. **Must run in a transaction that has executed**
  :data:`SET_LIBRARY_RESYNC`, or the immutability triggers reject the update
  (asserted up front). CopyStats counts inserts plus updates for DO UPDATE
  tables, inserts only for the rest.
  """
  resolved_pin = pin if pin is not None else DEFAULT_TAXONOMY_PIN
  if not resolved_pin:
    return CopyStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  if (
    connection.execute(
      text("SELECT current_setting(:guc, true)"), {"guc": LIBRARY_RESYNC_GUC}
    ).scalar()
    != "on"
  ):
    raise RuntimeError(
      "resync_library_into_tenant must run inside a transaction that has "
      f"executed `{SET_LIBRARY_RESYNC}`; the immutability triggers reject the "
      "DO UPDATE otherwise. Resync with a resync_library_into_tenant migration."
    )

  pin_values_sql, pin_params = _build_pin_clause(resolved_pin)

  tax_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.taxonomies ({_TAXONOMY_COLS})
      SELECT {_TAXONOMY_COLS} FROM public.taxonomies
      WHERE (standard, version) IN (VALUES {pin_values_sql})
      {_RESYNC_TAXONOMY_CONFLICT}
    """),
    pin_params,
  )

  # Same exclusion set, so a re-sync never re-introduces a curated-out concept.
  exclude_qnames = _tenant_exclude_qnames()
  exclude_clause = "AND e.qname != ALL(:exclude_qnames)" if exclude_qnames else ""
  elem_params = (
    {**pin_params, "exclude_qnames": list(exclude_qnames)}
    if exclude_qnames
    else pin_params
  )
  elem_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.elements ({_ELEMENT_COLS})
      SELECT {_ELEMENT_COLS} FROM public.elements e
      WHERE e.taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      {exclude_clause}
      {_RESYNC_ELEMENT_CONFLICT}
    """),
    elem_params,
  )

  label_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_labels ({_ELEMENT_LABEL_COLS})
      SELECT {_ELEMENT_LABEL_COLS} FROM public.element_labels
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      ON CONFLICT (id) DO NOTHING
    """),
  )

  ref_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_references ({_ELEMENT_REFERENCE_COLS})
      SELECT {_ELEMENT_REFERENCE_COLS} FROM public.element_references
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      ON CONFLICT (id) DO NOTHING
    """),
  )

  struct_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.structures ({_STRUCTURE_COLS})
      SELECT {_STRUCTURE_COLS} FROM public.structures
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  assoc_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.associations ({_ASSOCIATION_COLS})
      SELECT {_ASSOCIATION_COLS} FROM public.associations a
      WHERE a.structure_id IN (
        SELECT id FROM public.structures
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      {_assoc_endpoints_present(schema)}
      {_RESYNC_ASSOCIATION_CONFLICT}
    """),
    pin_params,
  )

  trait_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.traits ({_TRAIT_COLS})
      SELECT {_TRAIT_COLS} FROM public.traits
      WHERE created_by = 'library-seeder'
      {_RESYNC_TRAIT_CONFLICT}
    """),
  )

  et_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_traits ({_ELEMENT_TRAIT_COLS})
      SELECT {_ELEMENT_TRAIT_COLS} FROM public.element_traits
      WHERE element_id IN (SELECT id FROM {schema}.elements)
      {_RESYNC_ELEMENT_TRAIT_CONFLICT}
    """),
  )

  cls_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.classifications ({_CLASSIFICATION_COLS})
      SELECT {_CLASSIFICATION_COLS} FROM public.classifications
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  ac_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.association_classifications ({_ASSOC_CLASSIFICATION_COLS})
      SELECT {_ASSOC_CLASSIFICATION_COLS} FROM public.association_classifications
      WHERE association_id IN (SELECT id FROM {schema}.associations)
      ON CONFLICT (association_id, classification_id) DO NOTHING
    """),
  )

  rule_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.rules ({_RULE_COLS})
      SELECT {_RULE_COLS} FROM public.rules
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      {_RESYNC_RULE_CONFLICT}
    """),
    pin_params,
  )

  rsn_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.reporting_style_networks ({_REPORTING_STYLE_NETWORK_COLS})
      SELECT {_REPORTING_STYLE_NETWORK_COLS} FROM public.reporting_style_networks rsn
      WHERE EXISTS (SELECT 1 FROM {schema}.structures s WHERE s.id = rsn.reporting_style_id)
        AND EXISTS (SELECT 1 FROM {schema}.structures s WHERE s.id = rsn.network_id)
      ON CONFLICT (reporting_style_id, statement_type) DO NOTHING
    """),
  )

  return CopyStats(
    taxonomies=tax_result.rowcount or 0,
    elements=elem_result.rowcount or 0,
    element_labels=label_result.rowcount or 0,
    element_references=ref_result.rowcount or 0,
    structures=struct_result.rowcount or 0,
    associations=assoc_result.rowcount or 0,
    traits=trait_result.rowcount or 0,
    element_traits=et_result.rowcount or 0,
    classifications=cls_result.rowcount or 0,
    association_classifications=ac_result.rowcount or 0,
    rules=rule_result.rowcount or 0,
    reporting_style_networks=rsn_result.rowcount or 0,
  )
