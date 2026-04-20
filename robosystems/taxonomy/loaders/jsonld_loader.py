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

from rdflib import Graph, Literal, URIRef
from rdflib.namespace import DCTERMS, OWL, RDFS, SKOS

from robosystems.arelle.context import CANONICAL_CONTEXT, RS_VOCAB
from robosystems.logger import logger
from robosystems.taxonomy.model import (
  AssociationSpec,
  ClassificationAssignmentSpec,
  ClassificationSpec,
  ElementSpec,
  LabelSpec,
  ReferenceSpec,
  StructureSpec,
  TaxonomyPackage,
)

RS_NS = RS_VOCAB  # alias for readability


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
  f"{RS_NS}parent": ("presentation", "http://www.xbrl.org/2003/arcrole/parent-child"),
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


def _is_classification(graph: Graph, subject: URIRef) -> bool:
  """A classification node has both category and identifier predicates."""
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

  # Parent (optional)
  parent_qname: str | None = None
  parent_vals = list(graph.objects(subject, URIRef(f"{RS_NS}parent")))
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


def _extract_classification(graph: Graph, subject: URIRef) -> ClassificationSpec | None:
  """Build a ClassificationSpec from a Classification node's triples."""

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
  return ClassificationSpec(
    category=category,
    identifier=identifier,
    source=source,
    name=name,
    description=description,
  )


def _extract_classification_assignments(
  graph: Graph,
) -> list[ClassificationAssignmentSpec]:
  """Walk ``classifiedAs`` arcs and emit assignment specs.

  Each arc connects an element (subject) to a Classification IRI of the
  shape ``metamodel:{category}/{identifier}``. We decode the tail into
  (category, identifier) and emit one assignment per arc.
  """
  assignments: list[ClassificationAssignmentSpec] = []
  predicate = URIRef(f"{RS_NS}classifiedAs")
  for subject, obj in graph.subject_objects(predicate):
    if not isinstance(obj, URIRef):
      continue
    element_qname = _iri_to_qname(str(subject), CANONICAL_CONTEXT)
    if not element_qname:
      continue
    # Classification IRIs look like "…/us-gaap-metamodel/v1/{category}/{identifier}"
    iri = str(obj)
    tail = iri.rsplit("/", 2)
    if len(tail) < 2:
      continue
    category, identifier = tail[-2], tail[-1]
    assignments.append(
      ClassificationAssignmentSpec(
        element_qname=element_qname,
        category=category,
        identifier=identifier,
      )
    )
  return assignments


def _extract_associations(graph: Graph) -> list[AssociationSpec]:
  """Walk all arc predicates and emit AssociationSpec entries."""
  associations: list[AssociationSpec] = []
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
  return associations


def _extract_structures(graph: Graph) -> list[StructureSpec]:
  """Extract extended link roles as structures."""
  structures: list[StructureSpec] = []
  role_pred = URIRef(f"{RS_NS}roleUri")
  name_pred = URIRef(f"{RS_NS}structureName")
  for subject, role_uri in graph.subject_objects(role_pred):
    names = list(graph.objects(subject, name_pred))
    name = str(names[0]) if names else str(role_uri).rsplit("/", 1)[-1]
    # Heuristic structure_type from role URI
    role_str = str(role_uri).lower()
    if "balancesheet" in role_str:
      stype = "balance_sheet"
    elif "income" in role_str or "operations" in role_str:
      stype = "income_statement"
    elif "cashflow" in role_str:
      stype = "cash_flow_statement"
    elif "equity" in role_str or "changesin" in role_str:
      stype = "equity_statement"
    else:
      stype = "custom"
    structures.append(
      StructureSpec(name=name, role_uri=str(role_uri), structure_type=stype)
    )
  return structures


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
  taxonomy_type = doc.get("taxonomy_type", "reporting")
  name = f"{standard} {version}"

  # Parse with rdflib — it handles the @context expansion
  graph = Graph()
  graph.parse(data=raw, format="json-ld")

  # Find concepts + classification nodes. A subject is one or the other,
  # not both (classifications have category+identifier, concepts have
  # balance/period/elementType).
  elements: list[ElementSpec] = []
  classifications: list[ClassificationSpec] = []
  seen_subjects: set[str] = set()
  for subject in graph.subjects():
    if not isinstance(subject, URIRef):
      continue
    subject_str = str(subject)
    if subject_str in seen_subjects:
      continue
    seen_subjects.add(subject_str)
    if _is_classification(graph, subject):
      cls = _extract_classification(graph, subject)
      if cls is not None:
        classifications.append(cls)
    elif _is_concept(graph, subject):
      element = _extract_element(graph, subject)
      if element is not None:
        elements.append(element)

  associations = _extract_associations(graph)
  structures = _extract_structures(graph)
  assignments = _extract_classification_assignments(graph)

  logger.info(
    f"Loaded {name}: {len(elements)} elements, "
    f"{len(associations)} associations, {len(structures)} structures, "
    f"{len(classifications)} classifications, "
    f"{len(assignments)} classification assignments"
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
    classifications=classifications,
    classification_assignments=assignments,
    taxonomy_type=taxonomy_type,
    is_shared=True,
    description=description,
  )
