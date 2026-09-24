"""TaxonomyPackage → ORM inserts for library-origin taxonomies (seed migrations).

Three passes, each run over every package before the next, so cross-package
references resolve by DB lookup regardless of load order:
``create_library_taxonomy_elements`` → ``create_library_arcs`` →
``create_library_rules``.

Ids are deterministic UUID5s, so an additive re-run hits ON CONFLICT. Changing
an id derivation (notably ``_element_id``) requires a full wipe and re-seed of
the library; an additive re-run would mint duplicate rows.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from sqlalchemy import delete, exists, select
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
  RuleVariableSpec,
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
    # rs-gaap extension packages; source is the namespace prefix.
    "disclosures",
    "checklist",
    "styles",
    "rs-metric",
    "rs-driver",
    # cm — Conceptual Model framework (cm:Debit/cm:Credit posting roles).
    "cm",
  }
)


def _taxonomy_id(standard: str, version: str) -> str:
  return generate_deterministic_uuid(f"{standard}:{version}", namespace="taxonomy")


# Matches the trailing framework-version path segment (e.g. ``/v1/`` or ``/v1``)
# in a namespace URI like ``https://robosystems.ai/taxonomy/rs-gaap/v1/``.
_VERSION_SEGMENT = re.compile(r"/v\d+/?$")


def _element_id(namespace_uri: str, qname: str) -> str:
  # Keyed on the version-less namespace so a concept keeps one id across
  # framework versions and tenant mappings survive a version bump.
  stable_ns = _VERSION_SEGMENT.sub("/", namespace_uri)
  return generate_deterministic_uuid(f"{stable_ns}#{qname}", namespace="element")


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


# Stored key names for a rule's variables. Rule evaluation indexes these
# snake_case keys directly; the JSON-LD camelCase spelling must not reach the
# column.
RULE_VARIABLE_NAME_KEY = "variable_name"
RULE_VARIABLE_QNAME_KEY = "variable_qname"


def rule_variables_json(variables: Iterable[RuleVariableSpec]) -> list[dict[str, str]]:
  """Serialize a rule's variables for the ``rules.rule_variables`` JSONB column.

  Every writer of the column must go through here.
  """
  return [
    {
      RULE_VARIABLE_NAME_KEY: v.variable_name,
      RULE_VARIABLE_QNAME_KEY: v.variable_qname,
    }
    for v in variables
  ]


def _default_role_uri(package: TaxonomyPackage) -> str:
  return f"{package.namespace_uri}default"


def create_library_taxonomy_elements(
  session: Session,
  package: TaxonomyPackage,
  created_by: str = "library-seeder",
) -> tuple[str, dict[str, int]]:
  """Insert Taxonomy + Elements + Labels + References + Structures +
  Traits + default catch-all structure for one package.

  Must run for every package before ``create_library_arcs`` runs for any.
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
        item_type=element.item_type,
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
          "item_type": pg_insert(Element.__table__).excluded.item_type,
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


def prune_empty_default_structures(session: Session) -> int:
  """Delete auto-created catch-all "default structure" rows with no arcs and
  no rule targeting them, so they aren't copied into every tenant library.

  Matches by the default-structure name, never by "empty custom": the
  reporting-style composition anchors are also arc-less ``custom``
  structures, and deleting them breaks rendering. (A guard against
  ``reporting_style_networks`` isn't possible: that table doesn't exist yet
  when migration 0002 runs this.) Call after every package's arcs and rules
  are loaded. Returns the count deleted.
  """
  result = session.execute(
    delete(Structure)
    .where(Structure.block_type == "custom")
    .where(Structure.name.like("% — default structure"))
    .where(~exists().where(Association.structure_id == Structure.id))
    .where(~exists().where(Rule.target_structure_id == Structure.id))
    .execution_options(synchronize_session=False)
  )
  return result.rowcount or 0


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
  """Resolve qname → period_type ('instant' | 'duration')."""
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
  """Router for arcs without a role (e.g. ``rs-gaap-presentation``'s flat
  parent triples): instant parents → balance sheet, duration parents → cash
  flow if the qname matches a CF pattern, else income statement.

  ``None`` when the package lacks BS and IS structures; the caller then uses
  the default structure.
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
  """Insert Associations + Trait/Label/Reference assignments for one package,
  resolving qnames by DB lookup so targets may live in any package."""
  counts: dict[str, int] = {
    "associations": 0,
    "associations_skipped": 0,
    "trait_assignments": 0,
    "trait_assignments_skipped": 0,
    "label_assignments": 0,
    "label_assignments_skipped": 0,
    "reference_assignments": 0,
    "reference_assignments_skipped": 0,
    # Always 0: seed-time AssociationClassification loading doesn't exist yet.
    "classification_assignments": 0,
    "classification_assignments_skipped": 0,
  }

  default_struct_id = _structure_id(_default_role_uri(package))

  block_type_to_id: dict[str, str] = {}
  for spec in package.structures:
    sid = _structure_id(spec.role_uri)
    block_type_to_id.setdefault(spec.block_type, sid)
  arc_router = _build_arc_router(block_type_to_id)

  all_qnames = {q for a in package.associations for q in (a.from_qname, a.to_qname)}
  all_qnames |= {asn.element_qname for asn in package.trait_assignments}
  all_qnames |= {la.element_qname for la in package.label_assignments}
  all_qnames |= {ra.element_qname for ra in package.reference_assignments}
  element_id_map = _bulk_resolve_element_ids(session, all_qnames)

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
      # The id excludes weight/order/arcrole, so edits to them update in
      # place. Keep in lockstep with ``writer._RESYNC_ASSOCIATION_CONFLICT``.
      .on_conflict_do_update(
        index_elements=["id"],
        set_={
          "weight": pg_insert(Association.__table__).excluded["weight"],
          "order_value": pg_insert(Association.__table__).excluded["order_value"],
          "arcrole": pg_insert(Association.__table__).excluded["arcrole"],
          "confidence": pg_insert(Association.__table__).excluded["confidence"],
          "metadata": pg_insert(Association.__table__).excluded["metadata"],
        },
      )
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

  # Same id as an inline label, so a label moved into a label-linkbase
  # package reseeds to an identical row.
  skipped_label_qnames: list[str] = []
  for la in package.label_assignments:
    elem_id = element_id_map.get(la.element_qname)
    if elem_id is None:
      skipped_label_qnames.append(la.element_qname)
      continue
    session.execute(
      pg_insert(ElementLabel.__table__)
      .values(
        id=_label_id(elem_id, la.role, la.language),
        element_id=elem_id,
        role=la.role,
        language=la.language,
        text=la.text,
        created_by=created_by,
      )
      .on_conflict_do_nothing(index_elements=["id"])
    )
    counts["label_assignments"] += 1
  counts["label_assignments_skipped"] = len(skipped_label_qnames)
  if skipped_label_qnames:
    logger.warning(
      "[%s] %d label assignment(s) skipped — element qname not in library. Sample: %s",
      package.name,
      len(skipped_label_qnames),
      ", ".join(skipped_label_qnames[:5]),
    )

  # Same id as an inline reference, likewise.
  skipped_ref_qnames: list[str] = []
  for ra in package.reference_assignments:
    elem_id = element_id_map.get(ra.element_qname)
    if elem_id is None:
      skipped_ref_qnames.append(ra.element_qname)
      continue
    session.execute(
      pg_insert(ElementReference.__table__)
      .values(
        id=_reference_id(elem_id, ra.citation),
        element_id=elem_id,
        ref_type=ra.ref_type,
        citation=ra.citation,
        uri=ra.uri,
        attributes=ra.attributes,
        created_by=created_by,
      )
      .on_conflict_do_nothing(index_elements=["id"])
    )
    counts["reference_assignments"] += 1
  counts["reference_assignments_skipped"] = len(skipped_ref_qnames)
  if skipped_ref_qnames:
    logger.warning(
      "[%s] %d reference assignment(s) skipped — element qname not in library. Sample: %s",
      package.name,
      len(skipped_ref_qnames),
      ", ".join(skipped_ref_qnames[:5]),
    )

  return counts


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

  Association-targeted rules are not supported yet (logged and skipped).
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

    variables_json = rule_variables_json(rule.rule_variables)

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
