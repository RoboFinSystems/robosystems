"""Copy library content from the ``public`` schema into a tenant schema.

Called at entity-graph provisioning time (via ``provision_tenant_schema``)
and by the backfill migration that seeds library rows into existing tenant
schemas. The pattern is bulk ``INSERT ... SELECT`` in FK order, preserving
the library rows' deterministic UUID5 ids so re-running is idempotent.

Library-origin rows are distinguished by ``created_by = 'library-seeder'``
(applied by ``operations/taxonomy_block/library_creator.py``). After the copy,
tenant-schema immutability triggers (installed separately) key on that same
audit field to raise on any UPDATE/DELETE against a library-seeded row.
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


# The transaction-scoped GUC that opts a transaction into a library re-sync.
# resync_library_into_tenant requires it (the immutability triggers consult it),
# and callers run SET_LIBRARY_RESYNC in the same transaction first. Defined at
# this layer so the writer's guard and every caller share one source of truth.
LIBRARY_RESYNC_GUC = "robosystems.library_resync"
SET_LIBRARY_RESYNC = f"SET LOCAL {LIBRARY_RESYNC_GUC} = 'on'"


def _build_pin_clause(resolved_pin: dict[str, str]) -> tuple[str, dict[str, str]]:
  """Flatten a resolved pin into a parameterized ``VALUES`` clause + params.

  E.g. ``{"fac":"v1","rs-gaap":"v1"}`` → ``"(:s0, :v0), (:s1, :v1)"`` plus the
  bound params. Shared by :func:`copy_library_into_tenant` and
  :func:`resync_library_into_tenant` so the two paths cannot drift in how they
  bind the pinned ``(standard, version)`` pairs.
  """
  pin_values_sql = ", ".join(f"(:s{i}, :v{i})" for i in range(len(resolved_pin)))
  pin_params: dict[str, str] = {}
  for i, (std, ver) in enumerate(resolved_pin.items()):
    pin_params[f"s{i}"] = std
    pin_params[f"v{i}"] = ver
  return pin_values_sql, pin_params


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
    return CopyStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  pin_values_sql, pin_params = _build_pin_clause(resolved_pin)

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

  # Traits — copy the full fac-traits vocabulary (all rows with
  # created_by='library-seeder'). Pin-independent: every tenant gets the
  # same trait catalog.
  trait_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.traits ({_TRAIT_COLS})
      SELECT {_TRAIT_COLS} FROM public.traits
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  # Element traits — junction rows linking elements copied above to the
  # trait catalog.
  et_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.element_traits ({_ELEMENT_TRAIT_COLS})
      SELECT {_ELEMENT_TRAIT_COLS} FROM public.element_traits
      WHERE element_id IN (
        SELECT id FROM public.elements
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      ON CONFLICT (element_id, trait_id) DO NOTHING
    """),
    pin_params,
  )

  # Classifications — copy association-side structural pattern vocabulary
  # (all rows with created_by='library-seeder'). Pin-independent.
  cls_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.classifications ({_CLASSIFICATION_COLS})
      SELECT {_CLASSIFICATION_COLS} FROM public.classifications
      WHERE created_by = 'library-seeder'
      ON CONFLICT (id) DO NOTHING
    """),
  )

  # Association classifications — junction rows linking
  # associations copied above to the classification catalog.
  ac_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.association_classifications ({_ASSOC_CLASSIFICATION_COLS})
      SELECT {_ASSOC_CLASSIFICATION_COLS} FROM public.association_classifications
      WHERE association_id IN (
        SELECT id FROM public.associations
        WHERE structure_id IN (
          SELECT id FROM public.structures
          WHERE taxonomy_id IN (
            SELECT id FROM public.taxonomies
            WHERE (standard, version) IN (VALUES {pin_values_sql})
          )
        )
      )
      ON CONFLICT (association_id, classification_id) DO NOTHING
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

  # Reporting Style composition (Phase 1 of §3.2). Each row points at a
  # Style Structure + a Network Structure that have both been copied into
  # this tenant — gate on both structure_ids existing locally so an
  # unpinned package doesn't leave dangling composition references.
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


# Per-table ON CONFLICT clauses for the re-sync path. The updatable column sets
# mirror ``operations/taxonomy_block/library_creator.py`` EXACTLY — public and
# tenant must agree on what is mutable. Frozen tables stay DO NOTHING (additive:
# new rows insert, existing rows are left alone).
#
# IMPORTANT: keep these statements in lockstep with ``copy_library_into_tenant``
# above (same tables, same FK order, same WHERE clauses) — only the trailing
# conflict action differs.
_RESYNC_TAXONOMY_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "taxonomy_type = EXCLUDED.taxonomy_type, description = EXCLUDED.description"
)
_RESYNC_ELEMENT_CONFLICT = (
  "ON CONFLICT (id) DO UPDATE SET "
  "balance_type = EXCLUDED.balance_type, period_type = EXCLUDED.period_type, "
  "substitution_group = EXCLUDED.substitution_group, "
  "is_abstract = EXCLUDED.is_abstract, is_monetary = EXCLUDED.is_monetary, "
  "element_type = EXCLUDED.element_type"
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
# Gap B — the association id (uuid5(structure:from:to:type)) is preserved when
# only these value columns change, so calc-weight / presentation-order / arcrole
# fixes update cleanly. Changing from/to/type mints a new arc (additive; the
# stale arc lingers until a deletion pass exists). Mirrors library_creator.
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

  The ``DO UPDATE`` sibling of :func:`copy_library_into_tenant`: same 12
  statements in the same FK order, but in-place library fixes (rule logic,
  calc-irrelevant element attributes, trait names/bindings) flow into an
  already-provisioned tenant instead of being skipped. New rows still insert;
  existing rows update for the mutable columns only; **nothing is ever
  deleted** (retirement propagation is intentionally out of scope).

  The updatable column sets mirror ``library_creator`` exactly: taxonomies,
  elements, traits, element_traits, rules, and **associations** (calc/
  presentation value columns — Gap B) update in place. Frozen (additive only):
  labels, references, structures, classifications, association_classifications,
  reporting_style_networks.

  Caller contract (identical to :func:`copy_library_into_tenant`): this function
  only issues statements and does not commit. **The caller MUST run inside a
  transaction that has executed** :data:`SET_LIBRARY_RESYNC`
  (``SET LOCAL robosystems.library_resync = 'on'``) — otherwise the immutability
  triggers (migration 0016) reject the ``DO UPDATE``. This is asserted up front:
  a missing GUC raises a clear ``RuntimeError`` rather than an opaque trigger
  exception mid-fan-out.

  Returns:
      :class:`CopyStats`. For ``DO UPDATE`` tables ``rowcount`` counts inserts
      **and** updates (the per-table delta touched by this re-sync); for frozen
      tables it counts inserts only.
  """
  resolved_pin = pin if pin is not None else DEFAULT_TAXONOMY_PIN
  if not resolved_pin:
    return CopyStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  # Fail loudly and early if the caller forgot the bypass GUC — otherwise the
  # first DO UPDATE raises an opaque PL/pgSQL trigger exception mid-fan-out.
  if (
    connection.execute(
      text("SELECT current_setting(:guc, true)"), {"guc": LIBRARY_RESYNC_GUC}
    ).scalar()
    != "on"
  ):
    raise RuntimeError(
      "resync_library_into_tenant must run inside a transaction that has "
      f"executed `{SET_LIBRARY_RESYNC}`; the immutability triggers reject the "
      "DO UPDATE otherwise. Use operations/taxonomy_block/resync.py."
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

  elem_result = connection.execute(
    text(f"""
      INSERT INTO {schema}.elements ({_ELEMENT_COLS})
      SELECT {_ELEMENT_COLS} FROM public.elements
      WHERE taxonomy_id IN (
        SELECT id FROM public.taxonomies
        WHERE (standard, version) IN (VALUES {pin_values_sql})
      )
      {_RESYNC_ELEMENT_CONFLICT}
    """),
    pin_params,
  )

  # Labels + references — frozen (additive only).
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

  # Structures — frozen (additive only).
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

  # Associations — DO UPDATE on value columns (Gap B): calc weights and
  # presentation order propagate in place; from/to/type changes mint new arcs.
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
      WHERE element_id IN (
        SELECT id FROM public.elements
        WHERE taxonomy_id IN (
          SELECT id FROM public.taxonomies
          WHERE (standard, version) IN (VALUES {pin_values_sql})
        )
      )
      {_RESYNC_ELEMENT_TRAIT_CONFLICT}
    """),
    pin_params,
  )

  # Classifications — frozen (additive only).
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
      WHERE association_id IN (
        SELECT id FROM public.associations
        WHERE structure_id IN (
          SELECT id FROM public.structures
          WHERE taxonomy_id IN (
            SELECT id FROM public.taxonomies
            WHERE (standard, version) IN (VALUES {pin_values_sql})
          )
        )
      )
      ON CONFLICT (association_id, classification_id) DO NOTHING
    """),
    pin_params,
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

  # Reporting Style composition — frozen (additive only).
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
