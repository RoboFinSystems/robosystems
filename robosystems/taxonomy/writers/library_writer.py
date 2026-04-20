"""TaxonomyPackage → SQL INSERTs into the public schema library.

Idempotent writer: existing rows (keyed by deterministic UUID5) are
skipped via ON CONFLICT DO NOTHING. Safe to run multiple times; safe
to run across taxonomy versions (each version's rows get unique IDs
because they're keyed on qname + namespace_uri + version).
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Connection

from robosystems.logger import logger
from robosystems.taxonomy.model import (
  AssociationSpec,
  ClassificationAssignmentSpec,
  ClassificationSpec,
  ElementSpec,
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
    "sfac6",
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
        'us-gaap-metamodel', now(), now(), 'library-seeder'
      )
      ON CONFLICT (element_id, classification_id) DO UPDATE SET
        is_primary = EXCLUDED.is_primary,
        confidence = EXCLUDED.confidence,
        updated_at = now()
      """
    ),
    {
      "element_id": element_id,
      "classification_id": classification_id,
      "is_primary": is_primary,
      "confidence": confidence,
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


def _get_or_create_default_structure(
  conn: Connection, taxonomy_id: str, package: TaxonomyPackage
) -> str:
  """A catch-all structure for arcs without an extended link role.

  Associations carry a role URI when they're scoped to one; for arcs
  without a role we bucket them under a package-level default
  structure so the `structure_id` FK is always populated.
  """
  default_role = f"{package.namespace_uri}default"
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


def write_taxonomy_package(
  conn: Connection, package: TaxonomyPackage
) -> dict[str, int]:
  """Persist a TaxonomyPackage to the public schema library.

  Args:
      conn: SQLAlchemy Connection bound to extensions DB (public schema).
      package: Loaded TaxonomyPackage.

  Returns:
      Dict with counts per row type (for logging / verification).
  """
  counts = {
    "taxonomies": 0,
    "elements": 0,
    "labels": 0,
    "references": 0,
    "structures": 0,
    "associations": 0,
    "classifications": 0,
    "classification_assignments": 0,
  }

  # 1. Taxonomy row
  taxonomy_id = _write_taxonomy_row(conn, package)
  counts["taxonomies"] = 1

  # 2. Elements (with their labels + references)
  qname_to_element_id: dict[str, str] = {}
  skipped_sources: dict[str, int] = {}
  for element in package.elements:
    if element.source not in _ALLOWED_SOURCES:
      # Infrastructure concept from a namespace we don't track — skip.
      # Tracked per-source for diagnostics only.
      skipped_sources[element.source] = skipped_sources.get(element.source, 0) + 1
      continue
    elem_id = _write_element(conn, element, taxonomy_id)
    qname_to_element_id[element.qname] = elem_id
    counts["elements"] += 1
    counts["labels"] += len(element.labels)
    counts["references"] += len(element.references)
  if skipped_sources:
    logger.info(f"Skipped elements by source (non-reporting): {skipped_sources}")

  # 3. Structures (extended link roles)
  role_to_structure_id: dict[str, str] = {}
  for structure in package.structures:
    struct_id = _write_structure(conn, structure, taxonomy_id)
    role_to_structure_id[structure.role_uri] = struct_id
    counts["structures"] += 1

  # Default structure for arcs without a role
  default_struct_id = _get_or_create_default_structure(conn, taxonomy_id, package)

  # 4. Associations — resolve qnames within this package first, then fall
  # back to a DB lookup so cross-package arcs work when the referenced
  # package has already been loaded by an earlier migration step.
  def _resolve_qname(qname: str) -> str | None:
    local = qname_to_element_id.get(qname)
    if local is not None:
      return local
    row = conn.execute(
      text("SELECT id FROM public.elements WHERE qname = :qname LIMIT 1"),
      {"qname": qname},
    ).fetchone()
    return row[0] if row else None

  unresolved: list[tuple[str, str, str]] = []
  for assoc in package.associations:
    from_id = _resolve_qname(assoc.from_qname)
    to_id = _resolve_qname(assoc.to_qname)
    if from_id is None or to_id is None:
      # Association target not loaded in any package yet — skip; will
      # be re-attemptable when the dependent package loads.
      unresolved.append((assoc.from_qname, assoc.to_qname, assoc.association_type))
      continue

    structure_id = (
      role_to_structure_id.get(assoc.role, default_struct_id)
      if assoc.role
      else default_struct_id
    )
    _write_association(conn, assoc, structure_id, from_id, to_id)
    counts["associations"] += 1

  # For cross-taxonomy mapping seeds (sfac6-to-fac, fac-to-rs-gaap) the
  # association arcs ARE the primary data. Dropping them silently would
  # mean silent mapping holes — surface a count + representative sample
  # so seed-curation drift is observable.
  if unresolved:
    sample = unresolved[:5]
    logger.warning(
      "[%s] %d association arc(s) skipped — from/to qname not in library. Sample: %s%s",
      package.name,
      len(unresolved),
      ", ".join(f"{f} --{t_}--> {t}" for f, t, t_ in sample),
      "" if len(unresolved) <= 5 else f" (+{len(unresolved) - 5} more)",
    )

  # 5. Classification vocabulary rows (from classification-vocabulary seeds
  #    like us-gaap-metamodel/v1)
  cls_key_to_id: dict[tuple[str, str, str], str] = {}
  for cls in package.classifications:
    cls_id = _write_classification(conn, cls)
    cls_key_to_id[(cls.category, cls.identifier, cls.source)] = cls_id
    counts["classifications"] += 1

  # 6. Classification assignments (from classification-assignment seeds
  #    like rs-gaap-to-metamodel/v1)
  skipped_assignments: list[ClassificationAssignmentSpec] = []
  for asn in package.classification_assignments:
    elem_id = _resolve_qname(asn.element_qname)
    if elem_id is None:
      skipped_assignments.append(asn)
      continue
    # Resolve classification id; fall back to DB lookup for cross-package
    # references (assignments seed loaded after vocabulary seed).
    key = (asn.category, asn.identifier, "us-gaap-metamodel")
    cls_id = cls_key_to_id.get(key)
    if cls_id is None:
      row = conn.execute(
        text(
          """
          SELECT id FROM public.classifications
          WHERE category = :category AND identifier = :identifier
          LIMIT 1
          """
        ),
        {"category": asn.category, "identifier": asn.identifier},
      ).fetchone()
      if row is None:
        skipped_assignments.append(asn)
        continue
      cls_id = row[0]
    _write_classification_assignment(
      conn,
      element_id=elem_id,
      classification_id=cls_id,
      is_primary=asn.is_primary,
      confidence=asn.confidence,
    )
    counts["classification_assignments"] += 1

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

  logger.info(f"Wrote {package.name}: {counts}")
  return counts
