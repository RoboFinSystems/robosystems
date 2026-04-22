"""Copy library content from the ``public`` schema into a tenant schema.

Called at entity-graph provisioning time (via ``provision_tenant_schema``)
and by the backfill migration that seeds library rows into existing tenant
schemas. The pattern is bulk ``INSERT ... SELECT`` in FK order, preserving
the library rows' deterministic UUID5 ids so re-running is idempotent.

Library-origin rows are distinguished by ``created_by = 'library-seeder'``
(applied in ``library_writer.py``). After the copy, tenant-schema
immutability triggers (installed separately) key on that same audit field
to raise on any UPDATE/DELETE against a library-seeded row.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

from robosystems.taxonomy.pins import DEFAULT_TAXONOMY_PIN

_TAXONOMY_COLS = (
  "id, name, description, taxonomy_type, version, standard, namespace_uri, "
  "is_shared, source_taxonomy_id, target_taxonomy_id, parent_taxonomy_id, "
  "extension_type, effective_date, is_active, is_locked, metadata, "
  "created_at, updated_at, created_by"
)

_ELEMENT_COLS = (
  "id, code, name, description, qname, namespace, uri, "
  "balance_type, period_type, substitution_group, is_abstract, is_monetary, "
  "element_type, parent_id, depth, path, taxonomy_id, source, currency, "
  "is_active, is_placeholder, external_id, external_source, metadata, "
  "version, created_at, updated_at, created_by"
)

_ELEMENT_LABEL_COLS = "id, element_id, role, language, text, created_at, created_by"

_ELEMENT_REFERENCE_COLS = (
  "id, element_id, ref_type, citation, uri, attributes, created_at, created_by"
)

_STRUCTURE_COLS = (
  "id, name, description, structure_type, taxonomy_id, graph_structure_id, "
  "is_active, concept_arrangement, member_arrangement, artifact_mechanics, "
  "parenthetical_note, template_id, metadata, created_at, updated_at, created_by"
)

_ASSOCIATION_COLS = (
  "id, structure_id, from_element_id, to_element_id, association_type, "
  "arcrole, order_value, weight, confidence, suggested_by, approved_by, "
  "approved_at, metadata, created_at, updated_at, created_by"
)

_CLASSIFICATION_COLS = (
  "id, category, identifier, type, name, description, confidence, source, "
  "metadata, created_at, updated_at, created_by"
)

_ELEMENT_CLASSIFICATION_COLS = (
  "element_id, classification_id, is_primary, confidence, source, "
  "created_at, updated_at, created_by"
)

_RULE_COLS = (
  "id, taxonomy_id, rule_category, rule_pattern, rule_expression, "
  "rule_message, rule_severity, rule_origin, target_kind, "
  "target_structure_id, target_element_id, target_association_id, "
  "rule_variables, metadata, created_at, updated_at, created_by"
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
  classifications: int
  element_classifications: int
  rules: int

  @property
  def total(self) -> int:
    return (
      self.taxonomies
      + self.elements
      + self.element_labels
      + self.element_references
      + self.structures
      + self.associations
      + self.classifications
      + self.element_classifications
      + self.rules
    )


def copy_library_into_tenant(
  connection: Connection,
  schema: str,
  pin: dict[str, str] | None = None,
) -> CopyStats:
  """Bulk-copy pinned library taxonomies from ``public.*`` into ``{schema}.*``.

  Copies six tables in FK order (taxonomies → elements → element_labels /
  element_references → structures → associations). Every statement uses
  ``ON CONFLICT (id) DO NOTHING`` so the call is idempotent: library rows
  keep their deterministic UUID5 ids, and re-running never produces
  duplicates or updates.

  Args:
      connection: A SQLAlchemy connection bound to the extensions database.
          Caller owns transaction management (this function only issues
          statements; it does not commit).
      schema: The tenant schema name (validated upstream).
      pin: ``{standard: version}`` naming which library taxonomies to
          copy. Defaults to :data:`DEFAULT_TAXONOMY_PIN` when None.

  Returns:
      :class:`CopyStats` with per-table insert counts (from ``result.rowcount``).
  """
  resolved_pin = pin if pin is not None else DEFAULT_TAXONOMY_PIN
  if not resolved_pin:
    return CopyStats(0, 0, 0, 0, 0, 0, 0, 0, 0)

  # Flatten the pin into a parameterized IN clause via VALUES.
  # E.g., ("sfac6", "v1"), ("fac", "v1"), …
  pin_values_sql = ", ".join(f"(:s{i}, :v{i})" for i in range(len(resolved_pin)))
  pin_params: dict[str, str] = {}
  for i, (std, ver) in enumerate(resolved_pin.items()):
    pin_params[f"s{i}"] = std
    pin_params[f"v{i}"] = ver

  # Taxonomies — only the pinned (standard, version) pairs.
  tax_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.taxonomies ({_TAXONOMY_COLS})
      SELECT {_TAXONOMY_COLS} FROM public.taxonomies
      WHERE (standard, version) IN (VALUES {pin_values_sql})
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Elements — by taxonomy_id. Also include taxonomy-less elements whose
  # source maps into the pinned set (defensive; seeds generally set
  # taxonomy_id, but a few historical rows may be orphan-anchored).
  elem_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.elements ({_ELEMENT_COLS})
      SELECT {_ELEMENT_COLS} FROM public.elements
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Labels + references — element_id in the copied element set.
  label_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_labels ({_ELEMENT_LABEL_COLS})
      SELECT {_ELEMENT_LABEL_COLS} FROM public.element_labels
      WHERE element_id IN (
        SELECT id FROM public.elements
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  ref_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_references ({_ELEMENT_REFERENCE_COLS})
      SELECT {_ELEMENT_REFERENCE_COLS} FROM public.element_references
      WHERE element_id IN (
        SELECT id FROM public.elements
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Structures + associations — by taxonomy_id (structures), then by
  # structure_id (associations).
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
      SELECT {_ASSOCIATION_COLS} FROM public.associations
      WHERE structure_id IN (
        SELECT id FROM public.structures
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      ON CONFLICT (id) DO NOTHING
    """),
    pin_params,
  )

  # Classifications — copy the full us-gaap-metamodel vocabulary (all
  # rows with created_by='library-seeder'). Pin-independent: every
  # tenant gets the same classification catalog.
  cls_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.classifications ({_CLASSIFICATION_COLS})
      SELECT {_CLASSIFICATION_COLS} FROM public.classifications
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  # Element classifications — junction rows linking elements copied
  # above to the classification catalog.
  ec_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_classifications ({_ELEMENT_CLASSIFICATION_COLS})
      SELECT {_ELEMENT_CLASSIFICATION_COLS} FROM public.element_classifications
      WHERE element_id IN (
        SELECT id FROM public.elements
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      ON CONFLICT (element_id, classification_id) DO NOTHING
    """),
    pin_params,
  )

  # Rules — by taxonomy_id. Depends on the copied structures / elements /
  # associations above since rule rows FK them polymorphically.
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

  return CopyStats(
    taxonomies=tax_result.rowcount or 0,
    elements=elem_result.rowcount or 0,
    element_labels=label_result.rowcount or 0,
    element_references=ref_result.rowcount or 0,
    structures=struct_result.rowcount or 0,
    associations=assoc_result.rowcount or 0,
    classifications=cls_result.rowcount or 0,
    element_classifications=ec_result.rowcount or 0,
    rules=rule_result.rowcount or 0,
  )
