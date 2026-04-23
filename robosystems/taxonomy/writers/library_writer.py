"""TaxonomyPackage → SQL INSERTs into the public schema library.

Idempotent writer: existing rows (keyed by deterministic UUID5) are
skipped via ON CONFLICT DO NOTHING. Safe to run multiple times; safe
to run across taxonomy versions (each version's rows get unique IDs
because they're keyed on qname + namespace_uri + version).
"""

from __future__ import annotations

import json

from sqlalchemy import text
from sqlalchemy.engine import Connection

from robosystems.logger import logger
from robosystems.taxonomy.model import (
  AssociationSpec,
  ClassificationAssignmentSpec,
  ClassificationSpec,
  ElementSpec,
  RuleSpec,
  StructureSpec,
  TaxonomyPackage,
)
from robosystems.utils.uuid import generate_deterministic_uuid

# Sources allowed by the elements.source CHECK constraint. Elements from
# any other source (country codes, conceptual-model infrastructure, SEC
# reference namespaces, etc) are skipped during library load — they're
# not meaningful reporting concepts and would fail the CHECK anyway.
_ALLOWED_SOURCES = frozenset(
  {
    "fac",
    "rs-gaap",
    "us-gaap",
    "ifrs",
    "quickbooks",
    "xero",
    "plaid",
    "native",
    "import",
    "system",
  }
)


def _taxonomy_id(standard: str, version: str) -> str:
  return generate_deterministic_uuid(f"{standard}:{version}", namespace="taxonomy")


def _element_id(namespace_uri: str, qname: str) -> str:
  return generate_deterministic_uuid(f"{namespace_uri}#{qname}", namespace="element")


def _label_id(element_id: str, role: str, language: str) -> str:
  return generate_deterministic_uuid(
    f"{element_id}:{role}:{language}", namespace="label"
  )


def _reference_id(element_id: str, citation: str) -> str:
  return generate_deterministic_uuid(f"{element_id}:{citation}", namespace="reference")


def _classification_id(category: str, identifier: str, type_: str = "system") -> str:
  """Deterministic ID for a (category, identifier, type) classification triple.

  Keyed so the same 'elementsOfFinancialStatements/asset' row is found
  regardless of how many taxonomies reference it. Shared across all
  elements that carry this classification.
  """
  return generate_deterministic_uuid(
    f"{category}:{identifier}:{type_}", namespace="classification"
  )


def _element_classification_id(element_id: str, classification_id: str) -> str:
  """Composite key for element_classifications junction rows."""
  return f"{element_id}:{classification_id}"


def _write_classification(conn: Connection, cls: ClassificationSpec) -> str:
  """Insert-or-get a classification row, returning its id."""
  type_ = cls.source
  cls_id = _classification_id(cls.category, cls.identifier, type_)
  conn.execute(
    text(
      """
      INSERT INTO public.classifications (
        id, category, identifier, type, name, description,
        metadata, created_at, updated_at, created_by
      ) VALUES (
        :id, :category, :identifier, :type, :name, :description,
        '{}'::jsonb, now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO UPDATE SET
        name = COALESCE(EXCLUDED.name, public.classifications.name),
        description = COALESCE(EXCLUDED.description, public.classifications.description),
        updated_at = now()
      """
    ),
    {
      "id": cls_id,
      "category": cls.category,
      "identifier": cls.identifier,
      "type": type_,
      "name": cls.name,
      "description": cls.description,
    },
  )
  return cls_id


def _write_classification_assignment(
  conn: Connection,
  *,
  element_id: str,
  classification_id: str,
  is_primary: bool,
  confidence: float | None,
  source: str,
) -> None:
  """Insert an element_classifications junction row."""
  conn.execute(
    text(
      """
      INSERT INTO public.element_classifications (
        element_id, classification_id, is_primary, confidence, source,
        created_at, updated_at, created_by
      ) VALUES (
        :element_id, :classification_id, :is_primary, :confidence,
        :source, now(), now(), 'library-seeder'
      )
      ON CONFLICT (element_id, classification_id) DO UPDATE SET
        is_primary = EXCLUDED.is_primary,
        confidence = EXCLUDED.confidence,
        source = EXCLUDED.source,
        updated_at = now()
      """
    ),
    {
      "element_id": element_id,
      "classification_id": classification_id,
      "is_primary": is_primary,
      "confidence": confidence,
      "source": source,
    },
  )


def _structure_id(role_uri: str) -> str:
  return generate_deterministic_uuid(role_uri, namespace="structure")


def _association_id(
  structure_id: str,
  from_element_id: str,
  to_element_id: str,
  association_type: str,
) -> str:
  return generate_deterministic_uuid(
    f"{structure_id}:{from_element_id}:{to_element_id}:{association_type}",
    namespace="association",
  )


def _rule_id(standard: str, local_id: str) -> str:
  """Deterministic UUID5 for a rule row keyed on (standard, local id).

  The local id is the blank-node local name from the seed (e.g.
  'fac-rule-bs-identity'). Including ``standard`` in the seed avoids
  collision if two rule packs happen to pick the same local id.
  """
  return generate_deterministic_uuid(f"{standard}:{local_id}", namespace="rule")


def _write_taxonomy_row(conn: Connection, package: TaxonomyPackage) -> str:
  """INSERT the Taxonomy row and return its id."""
  tax_id = _taxonomy_id(package.standard, package.version)

  conn.execute(
    text(
      """
      INSERT INTO public.taxonomies (
        id, name, description, taxonomy_type, version, standard, namespace_uri,
        is_shared, is_active, is_locked, metadata, created_at, updated_at, created_by
      ) VALUES (
        :id, :name, :description, :taxonomy_type, :version, :standard, :namespace_uri,
        :is_shared, true, true, '{}'::jsonb, now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO UPDATE SET
        taxonomy_type = EXCLUDED.taxonomy_type,
        description = EXCLUDED.description,
        updated_at = now()
      """
    ),
    {
      "id": tax_id,
      "name": package.name,
      "description": package.description,
      "taxonomy_type": package.taxonomy_type,
      "version": package.version,
      "standard": package.standard,
      "namespace_uri": package.namespace_uri,
      "is_shared": package.is_shared,
    },
  )
  return tax_id


def _write_element(conn: Connection, element: ElementSpec, taxonomy_id: str) -> str:
  """INSERT an Element row and return its id."""
  elem_id = _element_id(element.namespace_uri, element.qname)

  conn.execute(
    text(
      """
      INSERT INTO public.elements (
        id, code, name, description, qname, namespace, uri,
        balance_type, period_type, substitution_group,
        is_abstract, is_monetary, element_type,
        parent_id, depth, path, taxonomy_id, source, currency,
        is_active, is_placeholder, metadata, version,
        external_id, external_source,
        created_at, updated_at, created_by
      ) VALUES (
        :id, :code, :name, :description, :qname, :namespace, :uri,
        :balance_type, :period_type, :substitution_group,
        :is_abstract, :is_monetary, :element_type,
        NULL, 0, '', :taxonomy_id, :source, 'USD',
        true, false, '{}'::jsonb, 1,
        NULL, NULL,
        now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO UPDATE SET
        balance_type = EXCLUDED.balance_type,
        period_type = EXCLUDED.period_type,
        substitution_group = EXCLUDED.substitution_group,
        is_abstract = EXCLUDED.is_abstract,
        is_monetary = EXCLUDED.is_monetary,
        element_type = EXCLUDED.element_type
      """
    ),
    {
      "id": elem_id,
      "code": element.qname,
      "name": element.name,
      "description": None,
      "qname": element.qname,
      "namespace": element.namespace,
      "uri": f"{element.namespace_uri}{element.qname.split(':')[-1]}",
      "balance_type": element.balance_type,
      "period_type": element.period_type,
      "substitution_group": element.substitution_group,
      "is_abstract": element.is_abstract,
      "is_monetary": element.is_monetary,
      "element_type": element.element_type,
      "taxonomy_id": taxonomy_id,
      "source": element.source,
    },
  )

  # Labels
  for label in element.labels:
    conn.execute(
      text(
        """
        INSERT INTO public.element_labels (
          id, element_id, role, language, text, created_at, created_by
        ) VALUES (
          :id, :element_id, :role, :language, :text, now(), 'library-seeder'
        )
        ON CONFLICT (id) DO NOTHING
        """
      ),
      {
        "id": _label_id(elem_id, label.role, label.language),
        "element_id": elem_id,
        "role": label.role,
        "language": label.language,
        "text": label.text,
      },
    )

  # References
  for ref in element.references:
    conn.execute(
      text(
        """
        INSERT INTO public.element_references (
          id, element_id, ref_type, citation, uri, attributes,
          created_at, created_by
        ) VALUES (
          :id, :element_id, :ref_type, :citation, :uri, :attributes,
          now(), 'library-seeder'
        )
        ON CONFLICT (id) DO NOTHING
        """
      ),
      {
        "id": _reference_id(elem_id, ref.citation),
        "element_id": elem_id,
        "ref_type": ref.ref_type,
        "citation": ref.citation,
        "uri": ref.uri,
        "attributes": ref.attributes,
      },
    )

  return elem_id


def _write_structure(
  conn: Connection, structure: StructureSpec, taxonomy_id: str
) -> str:
  """INSERT a Structure row and return its id."""
  struct_id = _structure_id(structure.role_uri)
  conn.execute(
    text(
      """
      INSERT INTO public.structures (
        id, name, description, structure_type, taxonomy_id, graph_structure_id,
        is_active, metadata, created_at, updated_at, created_by
      ) VALUES (
        :id, :name, NULL, :structure_type, :taxonomy_id, NULL,
        true, :metadata, now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO NOTHING
      """
    ),
    {
      "id": struct_id,
      "name": structure.name,
      "structure_type": structure.structure_type,
      "taxonomy_id": taxonomy_id,
      "metadata": f'{{"role_uri": "{structure.role_uri}"}}',
    },
  )
  return struct_id


def _write_association(
  conn: Connection,
  assoc: AssociationSpec,
  structure_id: str,
  from_element_id: str,
  to_element_id: str,
) -> None:
  """INSERT an Association row."""
  assoc_id = _association_id(
    structure_id, from_element_id, to_element_id, assoc.association_type
  )

  conn.execute(
    text(
      """
      INSERT INTO public.associations (
        id, structure_id, from_element_id, to_element_id,
        association_type, arcrole, order_value, weight,
        confidence, suggested_by, approved_by, approved_at,
        metadata, created_at, updated_at, created_by
      ) VALUES (
        :id, :structure_id, :from_element_id, :to_element_id,
        :association_type, :arcrole, :order_value, :weight,
        1.0, 'library', 'library', now(),
        '{}'::jsonb, now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO NOTHING
      """
    ),
    {
      "id": assoc_id,
      "structure_id": structure_id,
      "from_element_id": from_element_id,
      "to_element_id": to_element_id,
      "association_type": assoc.association_type,
      "arcrole": assoc.arcrole,
      "order_value": assoc.order,
      "weight": assoc.weight,
    },
  )


def _default_role_uri(package: TaxonomyPackage) -> str:
  """Deterministic role URI for the package's catch-all default structure."""
  return f"{package.namespace_uri}default"


def _get_or_create_default_structure(
  conn: Connection, taxonomy_id: str, package: TaxonomyPackage
) -> str:
  """A catch-all structure for arcs without an extended link role.

  Associations carry a role URI when they're scoped to one; for arcs
  without a role we bucket them under a package-level default
  structure so the `structure_id` FK is always populated.
  """
  default_role = _default_role_uri(package)
  struct_id = _structure_id(default_role)
  conn.execute(
    text(
      """
      INSERT INTO public.structures (
        id, name, structure_type, taxonomy_id, is_active, metadata,
        created_at, updated_at, created_by
      ) VALUES (
        :id, :name, 'custom', :taxonomy_id, true, :metadata,
        now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO NOTHING
      """
    ),
    {
      "id": struct_id,
      "name": f"{package.name} — default structure",
      "taxonomy_id": taxonomy_id,
      "metadata": f'{{"role_uri": "{default_role}"}}',
    },
  )
  return struct_id


def _resolve_qname_db(conn: Connection, qname: str) -> str | None:
  row = conn.execute(
    text("SELECT id FROM public.elements WHERE qname = :qname LIMIT 1"),
    {"qname": qname},
  ).fetchone()
  return row[0] if row else None


def _resolve_classification_db(
  conn: Connection, category: str, identifier: str
) -> str | None:
  row = conn.execute(
    text(
      """
      SELECT id FROM public.classifications
      WHERE category = :category AND identifier = :identifier
      LIMIT 1
      """
    ),
    {"category": category, "identifier": identifier},
  ).fetchone()
  return row[0] if row else None


def write_taxonomy_elements(
  conn: Connection, package: TaxonomyPackage
) -> dict[str, int]:
  """Phase 1 writer: taxonomy row, elements, labels, references, structures,
  and classification vocabulary.

  Must be called for every package before ``write_taxonomy_arcs`` runs for
  any of them — phase 2 resolves cross-package qname/classification
  references via DB lookups, which requires the full element + vocabulary
  universe to already be persisted.
  """
  counts = {
    "taxonomies": 0,
    "elements": 0,
    "labels": 0,
    "references": 0,
    "structures": 0,
    "classifications": 0,
  }

  taxonomy_id = _write_taxonomy_row(conn, package)
  counts["taxonomies"] = 1

  skipped_sources: dict[str, int] = {}
  for element in package.elements:
    if element.source not in _ALLOWED_SOURCES:
      skipped_sources[element.source] = skipped_sources.get(element.source, 0) + 1
      continue
    _write_element(conn, element, taxonomy_id)
    counts["elements"] += 1
    counts["labels"] += len(element.labels)
    counts["references"] += len(element.references)
  if skipped_sources:
    logger.info(f"Skipped elements by source (non-reporting): {skipped_sources}")

  for structure in package.structures:
    _write_structure(conn, structure, taxonomy_id)
    counts["structures"] += 1

  # Reserve the default structure row up-front — arcs without a role in
  # phase 2 look it up via _default_structure_id(package) deterministically.
  _get_or_create_default_structure(conn, taxonomy_id, package)

  for cls in package.classifications:
    _write_classification(conn, cls)
    counts["classifications"] += 1

  return counts


def write_taxonomy_arcs(conn: Connection, package: TaxonomyPackage) -> dict[str, int]:
  """Phase 2 writer: associations + classification assignments.

  Assumes ``write_taxonomy_elements`` has already run for *every* package,
  so any qname or classification referenced by this package's arcs is
  resolvable via a DB lookup regardless of which seed defines it.
  """
  counts = {
    "associations": 0,
    "associations_skipped": 0,
    "classification_assignments": 0,
    "classification_assignments_skipped": 0,
  }

  # Structure id resolution — structures were written in phase 1 with
  # deterministic ids keyed on role_uri, so we can reconstruct the lookup
  # without carrying state between phases.
  default_struct_id = _structure_id(_default_role_uri(package))

  unresolved: list[tuple[str, str, str]] = []
  for assoc in package.associations:
    from_id = _resolve_qname_db(conn, assoc.from_qname)
    to_id = _resolve_qname_db(conn, assoc.to_qname)
    if from_id is None or to_id is None:
      unresolved.append((assoc.from_qname, assoc.to_qname, assoc.association_type))
      continue
    structure_id = _structure_id(assoc.role) if assoc.role else default_struct_id
    _write_association(conn, assoc, structure_id, from_id, to_id)
    counts["associations"] += 1

  counts["associations_skipped"] = len(unresolved)
  if unresolved:
    sample = unresolved[:5]
    logger.warning(
      "[%s] %d association arc(s) skipped — from/to qname not in library. Sample: %s%s",
      package.name,
      len(unresolved),
      ", ".join(f"{f} --{t_}--> {t}" for f, t, t_ in sample),
      "" if len(unresolved) <= 5 else f" (+{len(unresolved) - 5} more)",
    )

  skipped_assignments: list[ClassificationAssignmentSpec] = []
  for asn in package.classification_assignments:
    elem_id = _resolve_qname_db(conn, asn.element_qname)
    if elem_id is None:
      skipped_assignments.append(asn)
      continue
    cls_id = _resolve_classification_db(conn, asn.category, asn.identifier)
    if cls_id is None:
      skipped_assignments.append(asn)
      continue
    _write_classification_assignment(
      conn,
      element_id=elem_id,
      classification_id=cls_id,
      is_primary=asn.is_primary,
      confidence=asn.confidence,
      source=asn.source,
    )
    counts["classification_assignments"] += 1

  counts["classification_assignments_skipped"] = len(skipped_assignments)
  if skipped_assignments:
    sample = skipped_assignments[:5]
    logger.warning(
      "[%s] %d classification assignment(s) skipped — element/classification not in library. Sample: %s%s",
      package.name,
      len(skipped_assignments),
      ", ".join(f"{a.element_qname} --{a.category}--> {a.identifier}" for a in sample),
      ""
      if len(skipped_assignments) <= 5
      else f" (+{len(skipped_assignments) - 5} more)",
    )

  return counts


def _resolve_structure_id_by_role(conn: Connection, role_uri: str) -> str | None:
  """Confirm a structure with this role_uri exists; return its id.

  Structure ids are deterministic UUID5(role_uri), so we could compute
  the id without a round-trip — but the lookup catches typos in the
  seed where the role URI doesn't match a previously-loaded structure
  (FK would otherwise fail at INSERT with a less obvious error).
  """
  expected_id = _structure_id(role_uri)
  row = conn.execute(
    text("SELECT id FROM public.structures WHERE id = :id LIMIT 1"),
    {"id": expected_id},
  ).fetchone()
  return row[0] if row else None


def _write_rule(
  conn: Connection,
  rule: RuleSpec,
  taxonomy_id: str,
  package: TaxonomyPackage,
) -> bool:
  """Insert one rule row. Returns True on write, False when skipped.

  Skips (with a warning) when the polymorphic target can't be
  resolved — the most common failure mode is a target_ref in the seed
  pointing at a structure/element that hasn't been loaded yet. Better
  to log the miss than raise and abort the whole taxonomy load.
  """
  rule_uuid = _rule_id(package.standard, rule.id)
  target_kind: str | None = None
  target_structure_id: str | None = None
  target_element_id: str | None = None
  target_association_id: str | None = None

  if rule.rule_target is not None:
    target_kind = rule.rule_target.target_kind
    ref = rule.rule_target.target_ref
    if target_kind == "structure":
      target_structure_id = _resolve_structure_id_by_role(conn, ref)
      if target_structure_id is None:
        logger.warning(
          "Rule %s: target structure role_uri %r not in library — skipping",
          rule.id,
          ref,
        )
        return False
    elif target_kind == "element":
      target_element_id = _resolve_qname_db(conn, ref)
      if target_element_id is None:
        logger.warning(
          "Rule %s: target element qname %r not in library — skipping",
          rule.id,
          ref,
        )
        return False
    elif target_kind == "association":
      # Associations carry opaque composite ids; we don't have a
      # natural-key resolver yet. Phase δ.3 will add one alongside the
      # arc-level rule harvest; until then association-targeted rules
      # are not seedable.
      logger.warning(
        "Rule %s: association-targeted rules not yet supported — skipping",
        rule.id,
      )
      return False

  variables_json = json.dumps(
    [
      {"variable_name": v.variable_name, "variable_qname": v.variable_qname}
      for v in rule.rule_variables
    ]
  )

  conn.execute(
    text(
      """
      INSERT INTO public.rules (
        id, taxonomy_id, rule_category, rule_pattern, rule_expression,
        rule_message, rule_severity, rule_origin,
        target_kind, target_structure_id, target_element_id,
        target_association_id,
        rule_variables, metadata, created_at, updated_at, created_by
      ) VALUES (
        :id, :taxonomy_id, :rule_category, :rule_pattern, :rule_expression,
        :rule_message, :rule_severity, :rule_origin,
        :target_kind, :target_structure_id, :target_element_id,
        :target_association_id,
        CAST(:rule_variables AS jsonb), '{}'::jsonb, now(), now(), 'library-seeder'
      )
      ON CONFLICT (id) DO UPDATE SET
        rule_expression = EXCLUDED.rule_expression,
        rule_message = EXCLUDED.rule_message,
        rule_severity = EXCLUDED.rule_severity,
        rule_variables = EXCLUDED.rule_variables,
        updated_at = now()
      """
    ),
    {
      "id": rule_uuid,
      "taxonomy_id": taxonomy_id,
      "rule_category": rule.rule_category,
      "rule_pattern": rule.rule_pattern,
      "rule_expression": rule.rule_expression,
      "rule_message": rule.rule_message,
      "rule_severity": rule.rule_severity,
      "rule_origin": rule.rule_origin,
      "target_kind": target_kind,
      "target_structure_id": target_structure_id,
      "target_element_id": target_element_id,
      "target_association_id": target_association_id,
      "rule_variables": variables_json,
    },
  )
  return True


def write_taxonomy_rules(conn: Connection, package: TaxonomyPackage) -> dict[str, int]:
  """Phase 3 writer: rules rows.

  Depends on elements + structures already persisted (phase 1) so
  polymorphic target references resolve. Runs after
  ``write_taxonomy_arcs`` for every package so association-targeted
  rules (Phase δ.3) can resolve their targets too.
  """
  counts = {"rules": 0, "rules_skipped": 0}
  taxonomy_id = _taxonomy_id(package.standard, package.version)
  for rule in package.rules:
    if _write_rule(conn, rule, taxonomy_id, package):
      counts["rules"] += 1
    else:
      counts["rules_skipped"] += 1
  return counts


def write_taxonomy_package(
  conn: Connection, package: TaxonomyPackage
) -> dict[str, int]:
  """Single-package write — phase 1 + phase 2 + phase 3 in one call.

  Safe only when every qname/classification this package references is
  already in the DB (or defined in this package itself). For multi-seed
  loads where arcs span packages, call ``write_taxonomy_elements`` for
  every package first, then ``write_taxonomy_arcs`` for every package,
  then ``write_taxonomy_rules`` last.
  """
  elem_counts = write_taxonomy_elements(conn, package)
  arc_counts = write_taxonomy_arcs(conn, package)
  rule_counts = write_taxonomy_rules(conn, package)
  counts = {**elem_counts, **arc_counts, **rule_counts}
  logger.info(f"Wrote {package.name}: {counts}")
  return counts
