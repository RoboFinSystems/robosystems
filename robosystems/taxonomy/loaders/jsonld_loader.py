"""JSON-LD → Pydantic TaxonomyPackage loader.

Reads a seed artifact produced by `robosystems.arelle.serialize_jsonld()`
and returns a validated `TaxonomyPackage` ready for the library writer.

The loader uses rdflib to parse the JSON-LD into triples, then walks
the graph by subject IRI to reconstruct ElementSpec, AssociationSpec,
and StructureSpec instances.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from rdflib import BNode, Graph, Literal, URIRef
from rdflib.collection import Collection
from rdflib.namespace import DCTERMS, OWL, RDFS, SKOS

from robosystems.arelle.context import CANONICAL_CONTEXT, RS_VOCAB
from robosystems.logger import logger
from robosystems.taxonomy.model import (
  AssociationSpec,
  ElementSpec,
  LabelSpec,
  ReferenceSpec,
  RuleSpec,
  RuleTargetSpec,
  RuleVariableSpec,
  StructureSpec,
  TaxonomyPackage,
  TraitAssignmentSpec,
  TraitSpec,
)

RS_NS = RS_VOCAB  # alias for readability

# Enum closures for rule axes. Kept here so both the loader (for
# cheap sanity-checking during parse) and the migration CHECK
# constraints can import one canonical source. Update RULE_CATEGORY_VALUES
# and RULE_PATTERN_VALUES when new categories or patterns are added to the
# seed.
RULE_CATEGORY_VALUES: frozenset[str] = frozenset(
  {
    "AutomatedAccountingAndReportingChecks",
    "DisclosureMechanicsRule",
    "FundamentalAccountingConceptRelation",
    "PeerConsistencyRule",
    "PriorPeriodConsistencyRule",
    "ReportLevelModelStructureRule",
    "ReportingSystemSpecificRule",
    "ToDoManualTask",
    "XBRLTechnicalSyntaxRule",
  }
)
RULE_PATTERN_VALUES: frozenset[str] = frozenset(
  {
    "Adjustment",
    "CoExists",
    "EqualTo",
    "Exists",
    "GreaterThan",
    "GreaterThanOrEqualToZero",
    "LessThan",
    "RollForward",
    "RollUp",
    "Variance",
  }
)


# Inverse label role mapping — predicate → role name + language preserved via literal.lang
LABEL_ROLE_FROM_PREDICATE: dict[URIRef, str] = {
  RDFS.label: "standard",
  RDFS.comment: "documentation",
  SKOS.altLabel: "verbose",
  SKOS.prefLabel: "standard",
}

# Reverse of arcrole → association_type, but keyed by the RDF predicate
ARC_PREDICATE_TO_ASSOC_TYPE: dict[str, tuple[str, str]] = {
  # predicate_iri: (association_type, arcrole)
  f"{RS_NS}hasChild": (
    "presentation",
    "http://www.xbrl.org/2003/arcrole/parent-child",
  ),
  f"{RS_NS}summationOf": (
    "calculation",
    "http://www.xbrl.org/2003/arcrole/summation-item",
  ),
  f"{RS_NS}generalOf": (
    "general-special",
    "http://www.xbrl.org/2003/arcrole/general-special",
  ),
  f"{RS_NS}dimensionOf": (
    "general-special",
    "http://xbrl.org/int/dim/arcrole/domain-member",
  ),
  f"{RS_NS}hypercubeOf": (
    "calculation",
    "http://xbrl.org/int/dim/arcrole/all",
  ),
  str(OWL.equivalentClass): (
    "equivalence",
    "http://xbrlsite.azurewebsites.net/2016/conceptual-model/arcrole/class-equivalentClass",
  ),
}


def _iri_to_qname(iri: str, context: dict) -> str | None:
  """Compact a full IRI back to prefix:local using the @context.

  Returns None if no matching prefix is found (e.g., blank nodes).
  """
  pairs: list[tuple[str, str]] = []
  for key, val in context.items():
    if isinstance(val, str) and val.startswith(("http://", "https://")):
      pairs.append((val, key))
  pairs.sort(key=lambda p: -len(p[0]))

  for long_iri, prefix in pairs:
    if iri.startswith(long_iri):
      remainder = iri[len(long_iri) :]
      if remainder and not remainder.startswith(("/", "#")):
        return f"{prefix}:{remainder}"
  return None


def _infer_namespace(iri: str) -> tuple[str, str, str]:
  """Split an IRI into (namespace_uri, prefix, local_name).

  Falls back to splitting at # then /.
  """
  for sep in ("#", "/"):
    if sep in iri:
      ns, local = iri.rsplit(sep, 1)
      ns_with_sep = ns + sep
      # Look up prefix in context
      prefix = ""
      for key, val in CANONICAL_CONTEXT.items():
        if isinstance(val, str) and val == ns_with_sep:
          prefix = key
          break
      return ns_with_sep, prefix, local
  return iri, "", iri


def _extract_labels(graph: Graph, subject: URIRef) -> list[LabelSpec]:
  """Collect all labels for a concept."""
  labels: list[LabelSpec] = []

  # Standard label predicates
  for pred, role in LABEL_ROLE_FROM_PREDICATE.items():
    for obj in graph.objects(subject, pred):
      if isinstance(obj, Literal):
        lang = obj.language or "en"
        labels.append(LabelSpec(role=role, language=lang, text=str(obj)))

  # Custom role labels stored under rs:labelRole blank nodes
  for label_node in graph.objects(subject, URIRef(f"{RS_NS}labelRole")):
    role_vals = list(graph.objects(label_node, URIRef(f"{RS_NS}role")))
    lang_vals = list(graph.objects(label_node, URIRef(f"{RS_NS}labelLanguage")))
    text_vals = list(graph.objects(label_node, RDFS.label))
    if not text_vals:
      continue
    role = str(role_vals[0]) if role_vals else "other"
    lang = str(lang_vals[0]) if lang_vals else "en"
    labels.append(LabelSpec(role=role, language=lang, text=str(text_vals[0])))

  return labels


def _extract_references(graph: Graph, subject: URIRef) -> list[ReferenceSpec]:
  """Collect all references for a concept."""
  refs: list[ReferenceSpec] = []
  for ref_node in graph.objects(subject, DCTERMS.references):
    citations = list(graph.objects(ref_node, URIRef(f"{RS_NS}citation")))
    ref_types = list(graph.objects(ref_node, URIRef(f"{RS_NS}refType")))
    if not citations:
      continue
    refs.append(
      ReferenceSpec(
        citation=str(citations[0]),
        ref_type=str(ref_types[0]) if ref_types else None,
      )
    )
  return refs


def _is_concept(graph: Graph, subject: URIRef) -> bool:
  """Heuristic: a concept has balance, periodType, or elementType."""
  has_balance = bool(list(graph.objects(subject, URIRef(f"{RS_NS}balance"))))
  has_element_type = bool(list(graph.objects(subject, URIRef(f"{RS_NS}elementType"))))
  has_period = bool(list(graph.objects(subject, URIRef(f"{RS_NS}periodType"))))
  return has_balance or has_element_type or has_period


def _is_trait(graph: Graph, subject: URIRef) -> bool:
  """A trait node has both category and identifier predicates."""
  has_cat = bool(list(graph.objects(subject, URIRef(f"{RS_NS}category"))))
  has_id = bool(list(graph.objects(subject, URIRef(f"{RS_NS}identifier"))))
  return has_cat and has_id


def _extract_element(graph: Graph, subject: URIRef) -> ElementSpec | None:
  """Build an ElementSpec from the subject's triples."""
  iri = str(subject)
  ns_uri, prefix, local = _infer_namespace(iri)
  if not local:
    return None

  qname = f"{prefix}:{local}" if prefix else local

  def _single(pred: URIRef, default: Any = None) -> Any:
    vals = list(graph.objects(subject, pred))
    return str(vals[0]) if vals else default

  def _bool(pred: URIRef, default: bool = False) -> bool:
    vals = list(graph.objects(subject, pred))
    if not vals:
      return default
    v = vals[0]
    if isinstance(v, Literal):
      return bool(v.value) if v.value is not None else default
    return str(v).lower() in ("true", "1")

  balance = _single(URIRef(f"{RS_NS}balance"), "debit") or "debit"
  period_type = _single(URIRef(f"{RS_NS}periodType"), "duration") or "duration"
  element_type = _single(URIRef(f"{RS_NS}elementType"), "concept") or "concept"
  source = _single(URIRef(f"{RS_NS}source"), prefix) or prefix or "native"

  is_abstract = _bool(URIRef(f"{RS_NS}abstract"), False)
  is_monetary = _bool(URIRef(f"{RS_NS}monetary"), True)

  # Substitution group (optional — XBRL intrinsic)
  sub_group: str | None = None
  sg_vals = list(graph.objects(subject, URIRef(f"{RS_NS}substitutionGroup")))
  if sg_vals and isinstance(sg_vals[0], URIRef):
    sub_group = _iri_to_qname(str(sg_vals[0]), CANONICAL_CONTEXT)

  # Parent (optional). Element-level tree-parent declarations.
  # No active seeds use this today; kept for back-compat with seed
  # packages that emit ``rs:childOf`` (RDF "subject is a child of X")
  # or ``rs:parent`` on individual element definitions.
  parent_qname: str | None = None
  parent_vals = list(graph.objects(subject, URIRef(f"{RS_NS}childOf"))) or list(
    graph.objects(subject, URIRef(f"{RS_NS}parent"))
  )
  if parent_vals and isinstance(parent_vals[0], URIRef):
    parent_qname = _iri_to_qname(str(parent_vals[0]), CANONICAL_CONTEXT)

  labels = _extract_labels(graph, subject)
  references = _extract_references(graph, subject)

  # Default name is the local part with spaces (or a standard label if found)
  name = local
  for label in labels:
    if label.role == "standard":
      name = label.text
      break

  return ElementSpec(
    qname=qname,
    namespace=prefix,
    namespace_uri=ns_uri,
    name=name,
    balance_type=balance,
    period_type=period_type,
    is_abstract=is_abstract,
    is_monetary=is_monetary,
    element_type=element_type,
    substitution_group=sub_group,
    source=source,
    parent_qname=parent_qname,
    labels=labels,
    references=references,
  )


def _extract_trait(graph: Graph, subject: URIRef) -> TraitSpec | None:
  """Build a TraitSpec from a Trait node's triples."""

  def _single(pred: URIRef, default: Any = None) -> Any:
    vals = list(graph.objects(subject, pred))
    return str(vals[0]) if vals else default

  category = _single(URIRef(f"{RS_NS}category"))
  identifier = _single(URIRef(f"{RS_NS}identifier"))
  source = _single(URIRef(f"{RS_NS}source"), "us-gaap-metamodel")
  if not category or not identifier:
    return None
  labels = _extract_labels(graph, subject)
  name = identifier
  description = None
  for label in labels:
    if label.role == "standard":
      name = label.text
    elif label.role == "documentation":
      description = label.text
  return TraitSpec(
    category=category,
    identifier=identifier,
    source=source,
    name=name,
    description=description,
  )


def _extract_trait_assignments(
  graph: Graph,
) -> list[TraitAssignmentSpec]:
  """Walk ``hasTrait`` arcs and emit trait assignment specs.

  Each arc connects an element (subject) to a Trait IRI of the
  shape ``{namespace_uri}/{standard}/{version}/{category}/{identifier}``
  — e.g. ``https://robosystems.ai/taxonomy/us-gaap-metamodel/v1/
  elementsOfFinancialStatements/asset``. We decode the last 4 IRI
  segments into (standard-as-source, version, category, identifier) so
  the assignment carries its vocabulary's provenance.
  """
  assignments: list[TraitAssignmentSpec] = []
  predicate = URIRef(f"{RS_NS}hasTrait")
  for subject, obj in graph.subject_objects(predicate):
    if not isinstance(obj, URIRef):
      continue
    element_qname = _iri_to_qname(str(subject), CANONICAL_CONTEXT)
    if not element_qname:
      continue
    iri = str(obj)
    parts = iri.rsplit("/", 4)
    # Need at least 4 trailing segments — anything shorter cannot carry
    # the {standard}/{version}/{category}/{identifier} structure and
    # would silently round-trip with a bogus category.
    if len(parts) < 5:
      continue
    source, _version, category, identifier = (
      parts[-4],
      parts[-3],
      parts[-2],
      parts[-1],
    )
    assignments.append(
      TraitAssignmentSpec(
        element_qname=element_qname,
        category=category,
        identifier=identifier,
        source=source,
      )
    )
  return assignments


_DEFAULT_ARCROLE_BY_ASSOC_TYPE = {
  "calculation": "http://www.xbrl.org/2003/arcrole/summation-item",
  "presentation": "http://www.xbrl.org/2003/arcrole/parent-child",
  "general-special": "http://www.xbrl.org/2003/arcrole/general-special",
  "equivalence": (
    "http://xbrlsite.azurewebsites.net/2016/conceptual-model/arcrole/class-equivalentClass"
  ),
}


def _extract_associations(graph: Graph) -> list[AssociationSpec]:
  """Walk all arc predicates + reified arcs, emit AssociationSpec entries.

  Two encodings are supported:

  1. Flat arc predicates (``rs:generalOf``, ``rs:summationOf``, ``rs:parent``,
     ``owl:equivalentClass``) — a single triple per arc; metadata like
     weight / order / structure role is not carried.

  2. Reified arcs — a subject that has ``rs:arcFrom`` + ``rs:arcTo`` is
     treated as one arc node carrying the full XBRL linkbase shape:
     ``rs:arcFrom`` / ``rs:arcTo`` / ``rs:arcAssociationType`` /
     ``rs:arcRoleUri`` (structure binding) / ``rs:arcWeight`` (calc) /
     ``rs:arcOrder`` (presentation). Mirrors ``link:calculationArc`` and
     ``link:presentationArc`` in XBRL linkbases.
  """
  associations: list[AssociationSpec] = []

  # All flat arc predicates declare arcs in XBRL parent-child / general-
  # special / summation-item direction (subject is the parent / general
  # / summation, object is the child / special / operand). The loader
  # extracts ``from=subject, to=object`` directly — same direction the
  # renderer expects. No per-predicate swapping needed.

  # 1. Flat arc predicates
  for pred_iri, (assoc_type, arcrole) in ARC_PREDICATE_TO_ASSOC_TYPE.items():
    predicate = URIRef(pred_iri)
    for s, o in graph.subject_objects(predicate):
      from_qname = _iri_to_qname(str(s), CANONICAL_CONTEXT)
      to_qname = (
        _iri_to_qname(str(o), CANONICAL_CONTEXT) if isinstance(o, URIRef) else None
      )
      if not from_qname or not to_qname:
        continue
      associations.append(
        AssociationSpec(
          from_qname=from_qname,
          to_qname=to_qname,
          association_type=assoc_type,
          arcrole=arcrole,
        )
      )

  # 2. Reified arcs (rs:arcFrom/arcTo + metadata)
  arc_from_pred = URIRef(f"{RS_NS}arcFrom")
  arc_to_pred = URIRef(f"{RS_NS}arcTo")
  arc_type_pred = URIRef(f"{RS_NS}arcAssociationType")
  arc_role_pred = URIRef(f"{RS_NS}arcRoleUri")
  arc_arcrole_pred = URIRef(f"{RS_NS}arcArcrole")
  arc_weight_pred = URIRef(f"{RS_NS}arcWeight")
  arc_order_pred = URIRef(f"{RS_NS}arcOrder")

  arc_subjects = {s for s, _ in graph.subject_objects(arc_from_pred)}
  for subject in arc_subjects:
    from_objs = list(graph.objects(subject, arc_from_pred))
    to_objs = list(graph.objects(subject, arc_to_pred))
    if not from_objs or not to_objs:
      continue
    from_qname = _iri_to_qname(str(from_objs[0]), CANONICAL_CONTEXT)
    to_qname = _iri_to_qname(str(to_objs[0]), CANONICAL_CONTEXT)
    if not from_qname or not to_qname:
      continue

    assoc_type_objs = list(graph.objects(subject, arc_type_pred))
    assoc_type = str(assoc_type_objs[0]) if assoc_type_objs else "presentation"

    arcrole_objs = list(graph.objects(subject, arc_arcrole_pred))
    arcrole = (
      str(arcrole_objs[0])
      if arcrole_objs
      else _DEFAULT_ARCROLE_BY_ASSOC_TYPE.get(assoc_type, "")
    )

    role_objs = list(graph.objects(subject, arc_role_pred))
    role = str(role_objs[0]) if role_objs else None

    weight_objs = list(graph.objects(subject, arc_weight_pred))
    weight = float(weight_objs[0]) if weight_objs else None

    order_objs = list(graph.objects(subject, arc_order_pred))
    order = float(order_objs[0]) if order_objs else None

    associations.append(
      AssociationSpec(
        from_qname=from_qname,
        to_qname=to_qname,
        association_type=assoc_type,
        arcrole=arcrole,
        role=role,
        weight=weight,
        order=order,
      )
    )

  return associations


def _extract_rules(graph: Graph) -> list[RuleSpec]:
  """Walk rule subjects and emit RuleSpec entries.

  A rule subject is any node carrying an ``rs:ruleCategory`` triple.
  The blank-node local id is the subject's string form (the local name
  after ``_:`` in the JSON-LD source); the writer rewrites it to a
  deterministic UUID5 at seed time.

  ``rule_target`` sits on a nested blank node (``rs:ruleTarget``) with
  ``rs:targetKind`` + ``rs:targetRef`` (a URIRef because the context
  marks ``targetRef`` as ``@type: @id``). ``rule_variables`` is an
  rdf:List under ``rs:ruleVariables`` — walked via
  :class:`rdflib.collection.Collection`.
  """
  rules: list[RuleSpec] = []
  category_pred = URIRef(f"{RS_NS}ruleCategory")
  pattern_pred = URIRef(f"{RS_NS}rulePattern")
  expression_pred = URIRef(f"{RS_NS}ruleExpression")
  target_pred = URIRef(f"{RS_NS}ruleTarget")
  target_kind_pred = URIRef(f"{RS_NS}targetKind")
  target_ref_pred = URIRef(f"{RS_NS}targetRef")
  variables_pred = URIRef(f"{RS_NS}ruleVariables")
  variable_name_pred = URIRef(f"{RS_NS}variableName")
  variable_qname_pred = URIRef(f"{RS_NS}variableQname")
  message_pred = URIRef(f"{RS_NS}ruleMessage")
  severity_pred = URIRef(f"{RS_NS}ruleSeverity")
  origin_pred = URIRef(f"{RS_NS}ruleOrigin")

  for subject in set(graph.subjects(category_pred)):
    category_vals = list(graph.objects(subject, category_pred))
    pattern_vals = list(graph.objects(subject, pattern_pred))
    expression_vals = list(graph.objects(subject, expression_pred))
    if not category_vals or not pattern_vals or not expression_vals:
      continue

    # Unknown categories / patterns are not silently round-tripped — a
    # typo in a seed would otherwise land in the DB and fail the CHECK
    # constraint only at write time. Skip with a warning here so the
    # failure surfaces during load.
    category = str(category_vals[0])
    pattern = str(pattern_vals[0])
    if category not in RULE_CATEGORY_VALUES:
      logger.warning("Rule %s has unknown ruleCategory=%r; skipping", subject, category)
      continue
    if pattern not in RULE_PATTERN_VALUES:
      logger.warning("Rule %s has unknown rulePattern=%r; skipping", subject, pattern)
      continue

    target_spec: RuleTargetSpec | None = None
    target_objs = list(graph.objects(subject, target_pred))
    if target_objs:
      target_node = target_objs[0]
      kind_vals = list(graph.objects(target_node, target_kind_pred))
      ref_vals = list(graph.objects(target_node, target_ref_pred))
      if kind_vals and ref_vals:
        target_spec = RuleTargetSpec(
          target_kind=str(kind_vals[0]),  # type: ignore[arg-type]
          target_ref=str(ref_vals[0]),
        )

    variable_specs: list[RuleVariableSpec] = []
    var_list_heads = list(graph.objects(subject, variables_pred))
    if var_list_heads:
      head = var_list_heads[0]
      if isinstance(head, (BNode, URIRef)):
        for item in Collection(graph, head):
          name_vals = list(graph.objects(item, variable_name_pred))
          qname_vals = list(graph.objects(item, variable_qname_pred))
          if not name_vals or not qname_vals:
            continue
          variable_specs.append(
            RuleVariableSpec(
              variable_name=str(name_vals[0]),
              variable_qname=str(qname_vals[0]),
            )
          )

    message_vals = list(graph.objects(subject, message_pred))
    severity_vals = list(graph.objects(subject, severity_pred))
    origin_vals = list(graph.objects(subject, origin_pred))

    severity = str(severity_vals[0]) if severity_vals else "error"
    origin = str(origin_vals[0]) if origin_vals else "native"

    local_id = str(subject)
    rules.append(
      RuleSpec(
        id=local_id,
        rule_category=category,
        rule_pattern=pattern,
        rule_expression=str(expression_vals[0]),
        rule_target=target_spec,
        rule_variables=variable_specs,
        rule_message=str(message_vals[0]) if message_vals else None,
        rule_severity=severity,  # type: ignore[arg-type]
        rule_origin=origin,  # type: ignore[arg-type]
      )
    )

  return rules


def _extract_structures(
  graph: Graph, default_structure_type: str | None = None
) -> list[StructureSpec]:
  """Extract extended link roles as structures.

  Resolution order for ``structure_type``:

  1. Explicit ``structureType`` on the role node — authoritative;
     used by presentation packages that carry per-structure types.
  2. ``default_structure_type`` from the package — set by mapping /
     rules / disclosure packages to override the role-uri name
     heuristic that would otherwise mistake (e.g.) a fac-to-rs-gaap
     crosswalk role for a balance_sheet just because the role URI
     mentions BS.
  3. Role-uri name heuristic — only fires for packages without a
     default; matches abbreviations like ``BS-classified`` /
     ``IS-multistep`` in real-world presentation taxonomies.
  4. ``custom`` fallback.
  """
  structures: list[StructureSpec] = []
  role_pred = URIRef(f"{RS_NS}roleUri")
  name_pred = URIRef(f"{RS_NS}structureName")
  type_pred = URIRef(f"{RS_NS}structureType")
  cap_pred = URIRef(f"{RS_NS}conceptArrangementPattern")
  for subject, role_uri in graph.subject_objects(role_pred):
    names = list(graph.objects(subject, name_pred))
    name = str(names[0]) if names else str(role_uri).rsplit("/", 1)[-1]
    explicit_types = list(graph.objects(subject, type_pred))
    if explicit_types:
      stype = str(explicit_types[0])
    elif default_structure_type is not None:
      stype = default_structure_type
    else:
      role_str = str(role_uri).lower()
      if "balancesheet" in role_str or "/bs-" in role_str or "/bs/" in role_str:
        stype = "balance_sheet"
      elif (
        "income" in role_str
        or "operations" in role_str
        or "/is-" in role_str
        or "/is/" in role_str
      ):
        stype = "income_statement"
      elif "cashflow" in role_str or "/cf-" in role_str or "/cf/" in role_str:
        stype = "cash_flow_statement"
      elif "equity" in role_str or "changesin" in role_str:
        stype = "equity_statement"
      else:
        stype = "custom"

    # Concept Arrangement Pattern: explicit field wins; otherwise default
    # by structure_type.
    explicit_caps = list(graph.objects(subject, cap_pred))
    if explicit_caps:
      cap: str | None = str(explicit_caps[0])
    else:
      cap = _default_concept_arrangement(stype)

    structures.append(
      StructureSpec(
        name=name,
        role_uri=str(role_uri),
        structure_type=stype,
        concept_arrangement=cap,
      )
    )
  return structures


def _default_concept_arrangement(structure_type: str) -> str | None:
  """Default Concept Arrangement Pattern per structure_type when seed
  doesn't declare one explicitly. Charlie's vocabulary."""
  return {
    "income_statement": "arithmetic",
    "balance_sheet": "arithmetic",
    "cash_flow_statement": "arithmetic",
    "equity_statement": "roll_forward",
    "validation_rules": "arithmetic",
  }.get(structure_type)


def load_taxonomy_package(path: Path | str) -> TaxonomyPackage:
  """Parse a JSON-LD seed file and return a TaxonomyPackage.

  Args:
      path: Path to the JSON-LD seed file.

  Returns:
      TaxonomyPackage with elements, associations, and structures.
  """
  path = Path(path)
  raw = path.read_text(encoding="utf-8")
  doc = json.loads(raw)

  # Extract top-level metadata
  standard = doc.get("standard", "unknown")
  version = doc.get("version", "v1")
  namespace_uri = doc.get("namespace_uri", "")
  description = doc.get("description")
  taxonomy_type = doc.get("taxonomy_type", "reporting_standard")
  default_structure_type = doc.get("default_structure_type")
  name = f"{standard} {version}"

  # Parse with rdflib — it handles the @context expansion
  graph = Graph()
  graph.parse(data=raw, format="json-ld")

  # Find concepts + trait nodes. A subject is one or the other,
  # not both (traits have category+identifier, concepts have
  # balance/period/elementType).
  elements: list[ElementSpec] = []
  traits: list[TraitSpec] = []
  seen_subjects: set[str] = set()
  for subject in graph.subjects():
    if not isinstance(subject, URIRef):
      continue
    subject_str = str(subject)
    if subject_str in seen_subjects:
      continue
    seen_subjects.add(subject_str)
    if _is_trait(graph, subject):
      trait = _extract_trait(graph, subject)
      if trait is not None:
        traits.append(trait)
    elif _is_concept(graph, subject):
      element = _extract_element(graph, subject)
      if element is not None:
        elements.append(element)

  associations = _extract_associations(graph)
  structures = _extract_structures(graph, default_structure_type=default_structure_type)
  trait_assignments = _extract_trait_assignments(graph)
  rules = _extract_rules(graph)

  logger.info(
    f"Loaded {name}: {len(elements)} elements, "
    f"{len(associations)} associations, {len(structures)} structures, "
    f"{len(traits)} traits, "
    f"{len(trait_assignments)} trait assignments, "
    f"{len(rules)} rules"
  )

  # Derive primary namespace_uri if not in metadata
  if not namespace_uri and elements:
    # Use most common namespace_uri among elements with matching standard
    ns_counts: dict[str, int] = {}
    for el in elements:
      if el.source == standard and el.namespace_uri:
        ns_counts[el.namespace_uri] = ns_counts.get(el.namespace_uri, 0) + 1
    if ns_counts:
      namespace_uri = max(ns_counts.items(), key=lambda kv: kv[1])[0]

  return TaxonomyPackage(
    name=name,
    standard=standard,
    version=version,
    namespace_uri=namespace_uri or "",
    elements=elements,
    associations=associations,
    structures=structures,
    traits=traits,
    trait_assignments=trait_assignments,
    rules=rules,
    taxonomy_type=taxonomy_type,
    default_structure_type=default_structure_type,
    is_shared=True,
    description=description,
  )
