"""ModelXbrl → rdflib.Graph extractor.

Walks an Arelle-loaded XBRL taxonomy and produces an RDF graph that
captures:
- Concepts (as classes / instances with classification, balance, periodType)
- Labels (via rdfs:label, skos:altLabel, rdfs:comment per XBRL role)
- References (via dcterms:references with structured citation)
- Arcs (parent-child, summation-item, general-special, equivalence, etc)
- Extended link roles (as structure metadata)

The output rdflib.Graph is fed to `serialize_jsonld()` for on-disk
persistence.

Namespace / formula concept filtering: XBRL Formula / Variable /
Validation linkbase concepts are skipped at extraction time to keep
seed artifacts focused on reporting taxonomy. Full formula ingest is
deferred to the validation rules engine.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import Any

from arelle import XbrlConst
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, OWL, RDF, RDFS, SKOS, XSD

from robosystems.arelle.context import RS_VOCAB
from robosystems.logger import logger

# RoboSystems vocabulary namespace
RS = Namespace(RS_VOCAB)
# XBRL vocabulary — inherited where XBRL defines the term (concept
# attributes, arc endpoints/roles, weight/order). Structural taxonomy
# arcs are reified as rs:Association nodes carrying these predicates.
XBRLI = Namespace("http://www.xbrl.org/2003/instance#")
XLINK = Namespace("http://www.w3.org/1999/xlink#")
LINK = Namespace("http://www.xbrl.org/2003/linkbase#")


# Arcrole → association_type mapping.
#
# The association_type is the DB-side enum value (for library_writer) and is
# stamped on each reified rs:Association via rs:associationType. The arcrole
# URI itself is preserved on the Association as xlink:arcrole.
ARCROLE_MAPPING: dict[str, str] = {
  "http://www.xbrl.org/2003/arcrole/parent-child": "presentation",
  "http://www.xbrl.org/2003/arcrole/summation-item": "calculation",
  "http://xbrlsite.azurewebsites.net/2016/conceptual-model/arcrole/class-equivalentClass": "equivalence",
  # General-special appears under multiple arcrole URIs in practice; check
  # both the XBRL-standard and any custom variants.
  "http://www.xbrl.org/2003/arcrole/general-special": "general-special",
  "http://xbrl.org/int/dim/arcrole/domain-member": "general-special",
  "http://xbrl.org/int/dim/arcrole/all": "calculation",
  "http://xbrl.org/int/dim/arcrole/hypercube-dimension": "calculation",
}

# Equivalence is the one relationship that stays a DIRECT predicate
# (owl:equivalentClass): a genuine symmetric OWL relation with no
# weight/order/role to carry. Every other arcrole reifies into an
# rs:Association node (see _add_relationship_triples).
EQUIVALENCE_ASSOCIATION_TYPE = "equivalence"

# Namespace prefixes to skip. XBRL Formula / Variable / Validation
# linkbase infrastructure + XBRL core meta-concepts (xbrli:item,
# xbrldt:dimensionItem, etc) aren't reporting taxonomy content.
SKIP_NAMESPACE_PREFIXES = {
  # Formula / validation linkbase infrastructure
  "formula",
  "variable",
  "validation",
  "msg",
  "gen",
  "ca",
  "cf",
  "df",
  "ea",
  "ef",
  "gf",
  "pf",
  "uf",
  "va",
  # Linkbase structural prefixes
  "xl",
  "label",
  "link",
  "ref",
  # XBRL core meta-concepts (not reporting taxonomy)
  "xbrli",
  "xbrldt",
  # Charlie's XBRL-based ontology prefixes (concept-model infrastructure)
  "fro-xbrl",
  "other",
  # XBRL dimensions infrastructure
  "xbrldi",
  # SEC infrastructure namespaces that Arelle pulls in as transitive
  # dependencies of the us-gaap taxonomy (geographies, currencies,
  # exchange listings, etc). They're member concepts for dimensional
  # reporting, not reporting concepts themselves.
  "country",
  "currency",
  "stpr",
  "naics",
  "sic",
  "exch",
}

# Label role URI → canonical role name used in CANONICAL_CONTEXT / DB schema
LABEL_ROLE_MAPPING: dict[str, str] = {
  "http://www.xbrl.org/2003/role/label": "standard",
  "http://www.xbrl.org/2003/role/verboseLabel": "verbose",
  "http://www.xbrl.org/2003/role/terseLabel": "terse",
  "http://www.xbrl.org/2003/role/documentation": "documentation",
  "http://www.xbrl.org/2003/role/periodStartLabel": "periodStart",
  "http://www.xbrl.org/2003/role/periodEndLabel": "periodEnd",
  "http://www.xbrl.org/2003/role/negatedLabel": "negated",
  "http://www.xbrl.org/2003/role/totalLabel": "total",
  "http://www.xbrl.org/2003/role/commentaryGuidance": "commentaryGuidance",
  "http://www.xbrl.org/2009/role/deprecatedLabel": "deprecatedLabel",
}


def _concept_iri(concept: Any) -> URIRef | None:
  """Return the RDF IRI for an Arelle ModelConcept.

  Uses {namespace_uri}{local_name} — the XBRL-standard way to identify
  concepts by their qname. Returns None for concepts whose namespace
  prefix is in SKIP_NAMESPACE_PREFIXES (Formula/Variable infrastructure).
  """
  if concept is None or concept.qname is None:
    return None

  prefix = getattr(concept.qname, "prefix", None) or ""
  if prefix in SKIP_NAMESPACE_PREFIXES:
    return None

  ns_uri = getattr(concept.qname, "namespaceURI", None) or ""
  local = getattr(concept.qname, "localName", None) or concept.name or ""
  if not ns_uri or not local:
    return None

  # Ensure namespace URI ends with # or / for proper concatenation
  if not ns_uri.endswith(("#", "/")):
    ns_uri = ns_uri + "#"

  return URIRef(f"{ns_uri}{local}")


def _classify_concept(concept: Any) -> str:
  """Derive the classification (asset/liability/equity/revenue/expense).

  Heuristic: use balance + periodType as signals, fall back to
  name-based inference. Authoritative classification will eventually
  come from the rs-gaap-type-subtype linkbase.
  """
  balance = getattr(concept, "balance", None)
  period_type = getattr(concept, "periodType", None)
  name = (concept.name or "").lower()

  # Instant + credit → liability or equity
  if period_type == "instant":
    if balance == "credit":
      if any(k in name for k in ("equity", "capital", "earnings", "stock")):
        return "equity"
      return "liability"
    if balance == "debit":
      return "asset"

  # Duration + credit → revenue
  if period_type == "duration":
    if balance == "credit":
      return "revenue"
    if balance == "debit":
      return "expense"

  # Default
  return "asset"


def _classify_element_type(concept: Any) -> str:
  """Map Arelle concept attributes to our `element_type` enum."""
  if getattr(concept, "isHypercubeItem", False):
    return "hypercube"
  if getattr(concept, "isDimensionItem", False):
    return "axis"
  # Members are typed-domain items or domain members; Arelle doesn't
  # expose this uniformly, infer from substitution group
  sub_group = getattr(concept, "substitutionGroupQname", None)
  if sub_group is not None:
    sub_local = getattr(sub_group, "localName", "")
    if sub_local == "dimensionItem":
      return "axis"
    if sub_local == "hypercubeItem":
      return "hypercube"
  if getattr(concept, "isAbstract", False):
    return "abstract"
  return "concept"


def _add_concept_triples(graph: Graph, concept: Any) -> None:
  """Add all triples for a single ModelConcept to the graph."""
  iri = _concept_iri(concept)
  if iri is None:
    return

  balance = getattr(concept, "balance", None)
  if balance:
    graph.add((iri, XBRLI.balance, Literal(balance)))

  period_type = getattr(concept, "periodType", None)
  if period_type:
    graph.add((iri, XBRLI.periodType, Literal(period_type)))

  is_abstract = bool(getattr(concept, "isAbstract", False))
  graph.add((iri, RS.abstract, Literal(is_abstract, datatype=XSD.boolean)))

  is_monetary = bool(getattr(concept, "isMonetary", False))
  graph.add((iri, RS.monetary, Literal(is_monetary, datatype=XSD.boolean)))

  graph.add((iri, RS.elementType, Literal(_classify_element_type(concept))))

  sub_group = getattr(concept, "substitutionGroupQname", None)
  if sub_group is not None:
    sg_ns = getattr(sub_group, "namespaceURI", None)
    sg_local = getattr(sub_group, "localName", None)
    if sg_ns and sg_local:
      if not sg_ns.endswith(("#", "/")):
        sg_ns = sg_ns + "#"
      graph.add((iri, RS.substitutionGroup, URIRef(f"{sg_ns}{sg_local}")))

  # Source inferred from namespace prefix
  prefix = getattr(concept.qname, "prefix", None) or ""
  graph.add((iri, RS.source, Literal(prefix)))


def _add_label_triples(graph: Graph, model_xbrl: Any) -> int:
  """Walk the label linkbase and attach labels to concepts.

  Returns count of labels added.
  """
  label_rel_set = model_xbrl.relationshipSet(XbrlConst.conceptLabel)
  if not label_rel_set or not label_rel_set.modelRelationships:
    return 0

  count = 0
  for rel in label_rel_set.modelRelationships:
    concept_iri = _concept_iri(rel.fromModelObject)
    label_obj = rel.toModelObject
    if concept_iri is None or label_obj is None:
      continue

    text = (
      getattr(label_obj, "textValue", None) or getattr(label_obj, "text", None) or ""
    )
    if not text:
      continue

    role_uri = getattr(label_obj, "role", "") or ""
    role_name = LABEL_ROLE_MAPPING.get(role_uri, "other")
    language = getattr(label_obj, "xmlLang", "en") or "en"

    # Route based on role
    literal = Literal(text, lang=language)
    if role_name == "standard":
      graph.add((concept_iri, RDFS.label, literal))
    elif role_name == "documentation":
      graph.add((concept_iri, RDFS.comment, literal))
    elif role_name in ("verbose", "terse"):
      graph.add((concept_iri, SKOS.altLabel, literal))
    else:
      # Custom role — use rs:labelRole to preserve the role info
      label_node = BNode()
      graph.add((concept_iri, RS.labelRole, label_node))
      graph.add((label_node, RS.role, Literal(role_name)))
      graph.add((label_node, RS.labelLanguage, Literal(language)))
      graph.add((label_node, RDFS.label, Literal(text)))

    count += 1

  return count


def _add_reference_triples(graph: Graph, model_xbrl: Any) -> int:
  """Walk the reference linkbase and attach references to concepts.

  Returns count of references added.
  """
  ref_rel_set = model_xbrl.relationshipSet(XbrlConst.conceptReference)
  if not ref_rel_set or not ref_rel_set.modelRelationships:
    return 0

  count = 0
  for rel in ref_rel_set.modelRelationships:
    concept_iri = _concept_iri(rel.fromModelObject)
    ref_obj = rel.toModelObject
    if concept_iri is None or ref_obj is None:
      continue

    # Reference linkbase resources have nested <Publisher>, <Name>, <Number>, etc.
    # Build a citation string from those parts. Upstream reference resources
    # occasionally have malformed XML or unexpected child structures; we
    # tolerate them non-fatally but log at debug so extraction drift during
    # seed curation is observable rather than silent.
    parts: list[str] = []
    ref_type: str | None = None
    try:
      for part in ref_obj.iterchildren():
        tag = getattr(part, "localName", None) or part.tag.split("}")[-1]
        text = (part.text or "").strip()
        if text:
          parts.append(f"{tag}={text}")
          if tag in ("Publisher", "Name") and ref_type is None:
            if "ASC" in text or "Accounting Standards Codification" in text:
              ref_type = "ASC"
            elif "SEC" in text or "Regulation" in text:
              ref_type = "SEC"
            elif "SFAC" in text:
              ref_type = "SFAC"
            elif "IFRS" in text:
              ref_type = "IFRS"
    except Exception:
      logger.debug("Reference parse skipped for concept %s", concept_iri, exc_info=True)

    citation = "; ".join(parts) if parts else (ref_obj.textValue or "").strip()
    if not citation:
      continue

    ref_node = BNode()
    graph.add((concept_iri, DCTERMS.references, ref_node))
    graph.add((ref_node, RS.citation, Literal(citation)))
    if ref_type:
      graph.add((ref_node, RS.refType, Literal(ref_type)))

    count += 1

  return count


def _add_relationship_triples(graph: Graph, model_xbrl: Any) -> dict[str, int]:
  """Walk all relationship base sets and add canonical arc triples.

  Structural taxonomy arcs (presentation / calculation / definition) are
  REIFIED as ``rs:Association`` nodes carrying ``xlink:from`` / ``xlink:to``
  / ``xlink:arcrole`` / ``xlink:role`` (the ELR binding) / ``link:weight`` /
  ``link:order`` / ``rs:associationType`` — so weight + order + the role
  binding survive, which the old flat-predicate form dropped. Equivalence
  stays a DIRECT ``owl:equivalentClass`` triple (symmetric OWL relation,
  no arc metadata to carry).

  Returns a dict of arcrole → count for diagnostics.
  """
  counts: dict[str, int] = {}

  for base_set_key in model_xbrl.baseSets:
    arcrole, role, link_qname, arc_qname = base_set_key
    if not arcrole or arcrole not in ARCROLE_MAPPING:
      continue

    assoc_type = ARCROLE_MAPPING[arcrole]

    try:
      rel_set = model_xbrl.relationshipSet(arcrole, role, link_qname, arc_qname)
    except Exception:
      continue

    if not rel_set or not rel_set.modelRelationships:
      continue

    for rel in rel_set.modelRelationships:
      from_iri = _concept_iri(rel.fromModelObject)
      to_iri = _concept_iri(rel.toModelObject)
      if from_iri is None or to_iri is None:
        continue

      if assoc_type == EQUIVALENCE_ASSOCIATION_TYPE:
        # Direct symmetric OWL relation — no reification.
        graph.add((from_iri, OWL.equivalentClass, to_iri))
        counts[arcrole] = counts.get(arcrole, 0) + 1
        continue

      order = getattr(rel, "order", None)
      weight = getattr(rel, "weight", None) if assoc_type == "calculation" else None
      preferred = getattr(rel, "preferredLabel", None)
      # Deterministic content-hashed IRI so regenerated seeds are
      # byte-stable across runs (a BNode would get a fresh label each run
      # and churn the committed seed diff).
      digest = hashlib.sha1(
        f"{role}|{arcrole}|{from_iri}|{to_iri}|{order}|{weight}".encode()
      ).hexdigest()[:16]
      assoc = URIRef(f"{RS_VOCAB}association/{digest}")
      graph.add((assoc, RDF.type, RS.Association))
      graph.add((assoc, XLINK["from"], from_iri))
      graph.add((assoc, XLINK.to, to_iri))
      graph.add((assoc, XLINK.arcrole, URIRef(arcrole)))
      graph.add((assoc, RS.associationType, Literal(assoc_type)))
      if role:
        # ELR binding — ties the arc to its rs:Structure (same role URI).
        graph.add((assoc, XLINK.role, URIRef(role)))
      if order is not None:
        graph.add(
          (assoc, LINK.order, Literal(Decimal(str(order)), datatype=XSD.decimal))
        )
      if weight is not None:
        graph.add(
          (assoc, LINK.weight, Literal(Decimal(str(weight)), datatype=XSD.decimal))
        )
      if preferred:
        graph.add((assoc, RS.preferredLabel, Literal(preferred)))
      counts[arcrole] = counts.get(arcrole, 0) + 1

  return counts


def _add_structure_metadata(graph: Graph, model_xbrl: Any) -> int:
  """Record extended link roles as structures.

  Each role URI becomes a resource with rs:structureName and rs:roleUri.
  Returns count of structures added.
  """
  count = 0
  for role_uri, role_obj in (getattr(model_xbrl, "roleTypes", {}) or {}).items():
    if role_uri in ("http://www.xbrl.org/2003/role/link", ""):
      continue
    # role_obj can be a list of ModelRoleType
    role_entries = role_obj if isinstance(role_obj, list) else [role_obj]
    for rt in role_entries:
      definition = getattr(rt, "definition", None) or role_uri.rsplit("/", 1)[-1]
      iri = URIRef(role_uri)
      graph.add((iri, RS.roleUri, Literal(role_uri)))
      graph.add((iri, RS.structureName, Literal(definition)))
      count += 1
      break  # one entry per role URI is enough for metadata

  return count


def extract_taxonomy(model_xbrl: Any) -> Graph:
  """Produce an rdflib.Graph from an Arelle-loaded ModelXbrl.

  Walks concepts, labels, references, arcs, and role metadata into a
  semantic graph that downstream code (serializer, loader, library writer)
  consumes.

  Args:
      model_xbrl: Arelle ModelXbrl returned from `load_filing(path)`

  Returns:
      rdflib.Graph populated with the taxonomy content.
  """
  graph = Graph()

  # Concepts
  concepts_added = 0
  for concept in model_xbrl.qnameConcepts.values():
    iri = _concept_iri(concept)
    if iri is None:
      continue
    _add_concept_triples(graph, concept)
    concepts_added += 1

  logger.info(f"Extracted {concepts_added} concepts")

  # Labels
  label_count = _add_label_triples(graph, model_xbrl)
  logger.info(f"Extracted {label_count} labels")

  # References
  ref_count = _add_reference_triples(graph, model_xbrl)
  logger.info(f"Extracted {ref_count} references")

  # Relationships
  rel_counts = _add_relationship_triples(graph, model_xbrl)
  total_rels = sum(rel_counts.values())
  logger.info(f"Extracted {total_rels} relationships across {len(rel_counts)} arcroles")

  # Structure metadata (extended link roles)
  structure_count = _add_structure_metadata(graph, model_xbrl)
  logger.info(f"Recorded {structure_count} extended link roles")

  return graph
