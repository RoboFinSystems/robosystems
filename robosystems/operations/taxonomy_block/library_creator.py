"""TaxonomyPackage → ORM-session inserts for library-origin taxonomies.

Admin-only. Three-pass flow so cross-package arcs resolve via DB lookup
regardless of seed load order:

  ``create_library_taxonomy_elements`` — Taxonomy + Elements + Labels +
    References + Structures + Traits per package.
  ``create_library_arcs`` — Associations + TraitAssignments per
    package (needs every package's elements in the DB first).
  ``create_library_rules`` — Rules with polymorphic target resolution
    (needs structures + elements from all packages).

Used by migration 0002 and any future migration that adds or updates library
seeds. Not routed through the public envelope or registry — takes TaxonomyPackage
directly because the envelope model (TaxonomyBlockElementRequest) is missing
library-specific fields (source, labels, references, classifications).

Key derivation must remain stable across re-seeds: same input → same UUID5,
so re-runs hit ON CONFLICT rather than inserting duplicates and existing
seeded DBs are not disturbed.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.extensions import (
  Association,
  Element,
  ElementLabel,
  ElementReference,
  ElementTrait,
  Rule,
  Structure,
  Taxonomy,
  Trait,
)
from robosystems.taxonomy.model import (
  TaxonomyPackage,
  TraitAssignmentSpec,
)
from robosystems.utils.uuid import generate_deterministic_uuid

# Sources allowed by the elements.source CHECK constraint. Elements from
# any other source are skipped during library load.
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
    # rs-gaap-base framework extension packages — each declares a
    # sibling namespace anchored to the rs-gaap framework. Elements
    # carry the namespace prefix as their source value.
    "disclosures",
    "checklist",
    "styles",
  }
)


# ── Deterministic ID helpers ──────────────────────────────────────────────────
# Key derivation must remain stable across re-seeds: same input → same UUID5
# → re-runs hit ON CONFLICT rather than inserting duplicates.


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


def _trait_id(category: str, identifier: str, type_: str = "system") -> str:
  return generate_deterministic_uuid(
    f"{category}:{identifier}:{type_}", namespace="trait"
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
  return generate_deterministic_uuid(f"{standard}:{local_id}", namespace="rule")


def _default_role_uri(package: TaxonomyPackage) -> str:
  return f"{package.namespace_uri}default"


# ── Taxonomy & elements ───────────────────────────────────────────────────────


def create_library_taxonomy_elements(
  session: Session,
  package: TaxonomyPackage,
  created_by: str = "library-seeder",
) -> tuple[str, dict[str, int]]:
  """Insert Taxonomy + Elements + Labels + References + Structures +
  Traits + default catch-all structure for one package.

  Must be called for every package before ``create_library_arcs`` runs for
  any of them — the arcs pass resolves cross-package qname/classification
  references via DB lookups, which requires the full element + vocabulary
  universe to already be persisted.

  Returns (taxonomy_id, counts).
  """
  counts: dict[str, int] = {
    "taxonomies": 0,
    "elements": 0,
    "labels": 0,
    "references": 0,
    "structures": 0,
    "traits": 0,
    "classifications": 0,
  }

  taxonomy_id = _taxonomy_id(package.standard, package.version)

  session.execute(
    pg_insert(Taxonomy.__table__)
    .values(
      id=taxonomy_id,
      name=package.name,
      description=package.description,
      taxonomy_type=package.taxonomy_type,
      version=package.version,
      standard=package.standard,
      namespace_uri=package.namespace_uri,
      is_shared=package.is_shared,
      is_active=True,
      is_locked=True,
      metadata={},
      created_by=created_by,
    )
    .on_conflict_do_update(
      index_elements=["id"],
      set_={
        "taxonomy_type": pg_insert(Taxonomy.__table__).excluded.taxonomy_type,
        "description": pg_insert(Taxonomy.__table__).excluded.description,
      },
    )
  )
  counts["taxonomies"] = 1

  skipped_sources: dict[str, int] = {}
  for element in package.elements:
    if element.source not in _ALLOWED_SOURCES:
      skipped_sources[element.source] = skipped_sources.get(element.source, 0) + 1
      continue

    elem_id = _element_id(element.namespace_uri, element.qname)
    session.execute(
      pg_insert(Element.__table__)
      .values(
        id=elem_id,
        code=element.qname,
        name=element.name,
        description=None,
        qname=element.qname,
        namespace=element.namespace,
        uri=f"{element.namespace_uri}{element.qname.split(':')[-1]}",
        balance_type=element.balance_type,
        period_type=element.period_type,
        substitution_group=element.substitution_group,
        is_abstract=element.is_abstract,
        is_monetary=element.is_monetary,
        element_type=element.element_type,
        parent_id=None,
        depth=0,
        path="",
        taxonomy_id=taxonomy_id,
        source=element.source,
        currency="USD",
        is_active=True,
        is_placeholder=False,
        metadata={},
        version=1,
        created_by=created_by,
      )
      .on_conflict_do_update(
        index_elements=["id"],
        set_={
          "balance_type": pg_insert(Element.__table__).excluded.balance_type,
          "period_type": pg_insert(Element.__table__).excluded.period_type,
          "substitution_group": pg_insert(
            Element.__table__
          ).excluded.substitution_group,
          "is_abstract": pg_insert(Element.__table__).excluded.is_abstract,
          "is_monetary": pg_insert(Element.__table__).excluded.is_monetary,
          "element_type": pg_insert(Element.__table__).excluded.element_type,
        },
      )
    )
    counts["elements"] += 1

    for label in element.labels:
      session.execute(
        pg_insert(ElementLabel.__table__)
        .values(
          id=_label_id(elem_id, label.role, label.language),
          element_id=elem_id,
          role=label.role,
          language=label.language,
          text=label.text,
          created_by=created_by,
        )
        .on_conflict_do_nothing(index_elements=["id"])
      )
      counts["labels"] += 1

    for ref in element.references:
      session.execute(
        pg_insert(ElementReference.__table__)
        .values(
          id=_reference_id(elem_id, ref.citation),
          element_id=elem_id,
          ref_type=ref.ref_type,
          citation=ref.citation,
          uri=ref.uri,
          attributes=ref.attributes,
          created_by=created_by,
        )
        .on_conflict_do_nothing(index_elements=["id"])
      )
      counts["references"] += 1

  if skipped_sources:
    logger.info("Skipped elements by source (non-reporting): %s", skipped_sources)

  for structure in package.structures:
    struct_id = _structure_id(structure.role_uri)
    session.execute(
      pg_insert(Structure.__table__)
      .values(
        id=struct_id,
        name=structure.name,
        description=None,
        block_type=structure.block_type,
        concept_arrangement=structure.concept_arrangement,
        taxonomy_id=taxonomy_id,
        is_active=True,
        metadata={"role_uri": structure.role_uri},
        created_by=created_by,
      )
      .on_conflict_do_nothing(index_elements=["id"])
    )
    counts["structures"] += 1

  default_role = _default_role_uri(package)
  default_struct_id = _structure_id(default_role)
  session.execute(
    pg_insert(Structure.__table__)
    .values(
      id=default_struct_id,
      name=f"{package.name} — default structure",
      description=None,
      block_type="custom",
      taxonomy_id=taxonomy_id,
      is_active=True,
      metadata={"role_uri": default_role},
      created_by=created_by,
    )
    .on_conflict_do_nothing(index_elements=["id"])
  )

  for trait in package.traits:
    trt_id = _trait_id(trait.category, trait.identifier, trait.source)
    session.execute(
      pg_insert(Trait.__table__)
      .values(
        id=trt_id,
        category=trait.category,
        identifier=trait.identifier,
        type=trait.source,
        name=trait.name,
        description=trait.description,
        metadata={},
        created_by=created_by,
      )
      .on_conflict_do_update(
        index_elements=["id"],
        set_={
          "name": pg_insert(Trait.__table__).excluded.name,
          "description": pg_insert(Trait.__table__).excluded.description,
        },
      )
    )
    counts["traits"] += 1

  return taxonomy_id, counts


# ── Cross-package arcs ────────────────────────────────────────────────────────


def _resolve_element_id(session: Session, qname: str) -> str | None:
  row = session.execute(
    select(Element.id).where(Element.qname == qname).limit(1)
  ).scalar_one_or_none()
  return row


def _bulk_resolve_element_ids(session: Session, qnames: set[str]) -> dict[str, str]:
  """Single query resolving all qnames at once. Returns {qname: element_id}."""
  if not qnames:
    return {}
  rows = session.execute(
    select(Element.qname, Element.id).where(Element.qname.in_(qnames))
  ).all()
  return {row[0]: row[1] for row in rows}


def _bulk_resolve_element_period_types(
  session: Session, qnames: set[str]
) -> dict[str, str]:
  """Resolve qname → period_type ('instant' | 'duration') for routing
  arcs by the parent element's period type. Returns {} when ``qnames``
  is empty.
  """
  if not qnames:
    return {}
  rows = session.execute(
    select(Element.qname, Element.period_type).where(Element.qname.in_(qnames))
  ).all()
  return {row[0]: row[1] for row in rows if row[1] is not None}


# rs-gaap qname prefixes that identify cash-flow-statement concepts.
# Used to disambiguate duration concepts (IS vs CF) when a presentation
# package declares both block_types but no per-arc role.
_CF_QNAME_PATTERNS: tuple[str, ...] = (
  "CashAndCashEquivalents",
  "NetCashProvided",
  "NetCashUsed",
  "PaymentsFor",
  "PaymentsTo",
  "PaymentsOf",
  "ProceedsFrom",
  "IncreaseDecreaseIn",
  "RepaymentsOf",
  "AdjustmentsToReconcile",
)


def _build_arc_router(
  block_type_to_id: dict[str, str],
):
  """Return a callable that routes a parent arc to a structure_id by
  the parent element's period_type + qname when the source seed lacks
  per-arc role qualifiers.

  Routing rule (only applies when the package declares both BS and IS
  structure types):

  - ``period_type='instant'`` → balance_sheet
  - ``period_type='duration'`` + qname matches a known CF prefix
    → cash_flow_statement (when present)
  - ``period_type='duration'`` (else) → income_statement

  Returns ``None`` when the package's structure set doesn't fit the
  BS/IS/CF shape — caller falls back to the default structure.

  This is needed for ``rs-gaap-presentation``, whose seed file uses
  flat ``rs:parent`` triples and declares 3 statement structures
  without per-arc role qualifiers. Without this routing, all 1890
  parent relationships land on the catchall "default structure" and
  the BS/IS/CF structures stay empty.
  """
  if "balance_sheet" not in block_type_to_id:
    return None
  if "income_statement" not in block_type_to_id:
    return None

  bs_id = block_type_to_id["balance_sheet"]
  is_id = block_type_to_id["income_statement"]
  cf_id = block_type_to_id.get("cash_flow_statement")

  def _route(parent_qname: str, parent_period_type: str | None) -> str | None:
    if parent_period_type == "instant":
      return bs_id
    if parent_period_type == "duration":
      if cf_id and any(p in parent_qname for p in _CF_QNAME_PATTERNS):
        return cf_id
      return is_id
    return None

  return _route


def _resolve_trait_id(session: Session, category: str, identifier: str) -> str | None:
  row = session.execute(
    select(Trait.id)
    .where(Trait.category == category, Trait.identifier == identifier)
    .limit(1)
  ).scalar_one_or_none()
  return row


def create_library_arcs(
  session: Session,
  package: TaxonomyPackage,
  created_by: str = "library-seeder",
) -> dict[str, int]:
  """Insert Associations + TraitAssignments for one package.

  Resolves qnames + traits via DB lookup so cross-package arcs work
  regardless of which seed defined the target element.
  """
  counts: dict[str, int] = {
    "associations": 0,
    "associations_skipped": 0,
    "trait_assignments": 0,
    "trait_assignments_skipped": 0,
    # AssociationClassification seed-time loading is not yet implemented;
    # TaxonomyPackage has no classification_assignments field, so these
    # counters always stay 0.  Reserved for when that path is added.
    "classification_assignments": 0,
    "classification_assignments_skipped": 0,
  }

  default_struct_id = _structure_id(_default_role_uri(package))

  # Build {block_type → struct_id} so the per-arc router can land
  # each association on the right declared structure when the seed
  # lacks per-arc role qualifiers (see ``_build_arc_router``).
  block_type_to_id: dict[str, str] = {}
  for spec in package.structures:
    sid = _structure_id(spec.role_uri)
    block_type_to_id.setdefault(spec.block_type, sid)
  arc_router = _build_arc_router(block_type_to_id)

  # Bulk-resolve all qnames needed by associations and trait assignments
  # in one query to avoid O(N) round trips for large packages.
  all_qnames = {q for a in package.associations for q in (a.from_qname, a.to_qname)}
  all_qnames |= {asn.element_qname for asn in package.trait_assignments}
  element_id_map = _bulk_resolve_element_ids(session, all_qnames)

  # Bulk-resolve period_types only when a router is active — saves a
  # second query for packages that don't need structural routing.
  parent_period_types: dict[str, str] = {}
  if arc_router is not None:
    parent_qnames = {a.from_qname for a in package.associations if not a.role}
    parent_period_types = _bulk_resolve_element_period_types(session, parent_qnames)

  unresolved: list[tuple[str, str, str]] = []
  for assoc in package.associations:
    from_id = element_id_map.get(assoc.from_qname)
    to_id = element_id_map.get(assoc.to_qname)
    if from_id is None or to_id is None:
      unresolved.append((assoc.from_qname, assoc.to_qname, assoc.association_type))
      continue
    if assoc.role:
      struct_id = _structure_id(assoc.role)
    elif arc_router is not None:
      routed = arc_router(assoc.from_qname, parent_period_types.get(assoc.from_qname))
      struct_id = routed if routed is not None else default_struct_id
    else:
      struct_id = default_struct_id
    assoc_id = _association_id(struct_id, from_id, to_id, assoc.association_type)
    session.execute(
      pg_insert(Association.__table__)
      .values(
        id=assoc_id,
        structure_id=struct_id,
        from_element_id=from_id,
        to_element_id=to_id,
        association_type=assoc.association_type,
        arcrole=assoc.arcrole,
        order_value=assoc.order,
        weight=assoc.weight,
        confidence=1.0,
        suggested_by="library",
        approved_by="library",
        metadata={},
        created_by=created_by,
      )
      .on_conflict_do_nothing(index_elements=["id"])
    )
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

  skipped_trait_assignments: list[TraitAssignmentSpec] = []
  for asn in package.trait_assignments:
    elem_id = element_id_map.get(asn.element_qname)
    if elem_id is None:
      skipped_trait_assignments.append(asn)
      continue
    trt_id = _resolve_trait_id(session, asn.category, asn.identifier)
    if trt_id is None:
      skipped_trait_assignments.append(asn)
      continue
    session.execute(
      pg_insert(ElementTrait.__table__)
      .values(
        element_id=elem_id,
        trait_id=trt_id,
        is_primary=asn.is_primary,
        confidence=asn.confidence,
        source=asn.source,
        created_by=created_by,
      )
      .on_conflict_do_update(
        index_elements=["element_id", "trait_id"],
        set_={
          "is_primary": pg_insert(ElementTrait.__table__).excluded.is_primary,
          "confidence": pg_insert(ElementTrait.__table__).excluded.confidence,
          "source": pg_insert(ElementTrait.__table__).excluded.source,
        },
      )
    )
    counts["trait_assignments"] += 1

  counts["trait_assignments_skipped"] = len(skipped_trait_assignments)
  if skipped_trait_assignments:
    sample = skipped_trait_assignments[:5]
    logger.warning(
      "[%s] %d trait assignment(s) skipped — element/trait not in library. Sample: %s%s",
      package.name,
      len(skipped_trait_assignments),
      ", ".join(f"{a.element_qname} --{a.category}--> {a.identifier}" for a in sample),
      ""
      if len(skipped_trait_assignments) <= 5
      else f" (+{len(skipped_trait_assignments) - 5} more)",
    )

  return counts


# ── Rules ─────────────────────────────────────────────────────────────────────


def _resolve_structure_id_by_role(session: Session, role_uri: str) -> str | None:
  expected_id = _structure_id(role_uri)
  row = session.execute(
    select(Structure.id).where(Structure.id == expected_id).limit(1)
  ).scalar_one_or_none()
  return row


def create_library_rules(
  session: Session,
  package: TaxonomyPackage,
  created_by: str = "library-seeder",
) -> dict[str, int]:
  """Insert Rules with polymorphic target resolution for one package.

  Must run after the cross-package arcs pass globally so structure +
  element targets resolve. Association-targeted rules are not yet
  supported (logs + skips).
  """
  counts: dict[str, int] = {"rules": 0, "rules_skipped": 0}
  taxonomy_id = _taxonomy_id(package.standard, package.version)

  for rule in package.rules:
    rule_uuid = _rule_id(package.standard, rule.id)
    target_kind: str | None = None
    target_structure_id: str | None = None
    target_element_id: str | None = None
    target_association_id: str | None = None

    if rule.rule_target is not None:
      target_kind = rule.rule_target.target_kind
      ref = rule.rule_target.target_ref
      if target_kind == "structure":
        target_structure_id = _resolve_structure_id_by_role(session, ref)
        if target_structure_id is None:
          logger.warning(
            "Rule %s: target structure role_uri %r not in library — skipping",
            rule.id,
            ref,
          )
          counts["rules_skipped"] += 1
          continue
      elif target_kind == "element":
        target_element_id = _resolve_element_id(session, ref)
        if target_element_id is None:
          logger.warning(
            "Rule %s: target element qname %r not in library — skipping",
            rule.id,
            ref,
          )
          counts["rules_skipped"] += 1
          continue
      elif target_kind == "association":
        logger.warning(
          "Rule %s: association-targeted rules not yet supported — skipping", rule.id
        )
        counts["rules_skipped"] += 1
        continue

    variables_json = [
      {"variable_name": v.variable_name, "variable_qname": v.variable_qname}
      for v in rule.rule_variables
    ]

    session.execute(
      pg_insert(Rule.__table__)
      .values(
        id=rule_uuid,
        taxonomy_id=taxonomy_id,
        rule_category=rule.rule_category,
        rule_pattern=rule.rule_pattern,
        rule_expression=rule.rule_expression,
        rule_message=rule.rule_message,
        rule_severity=rule.rule_severity,
        rule_origin=rule.rule_origin,
        target_kind=target_kind,
        target_structure_id=target_structure_id,
        target_element_id=target_element_id,
        target_association_id=target_association_id,
        rule_variables=variables_json,
        metadata={},
        created_by=created_by,
      )
      .on_conflict_do_update(
        index_elements=["id"],
        set_={
          "rule_expression": pg_insert(Rule.__table__).excluded.rule_expression,
          "rule_message": pg_insert(Rule.__table__).excluded.rule_message,
          "rule_severity": pg_insert(Rule.__table__).excluded.rule_severity,
          "rule_variables": pg_insert(Rule.__table__).excluded.rule_variables,
        },
      )
    )
    counts["rules"] += 1

  return counts
