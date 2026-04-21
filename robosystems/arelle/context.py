"""Canonical JSON-LD @context for the taxonomy library.

The same context is used by the serializer (rdflib.Graph → JSON-LD) and
the loader (JSON-LD → rdflib.Graph → TaxonomyPackage) to ensure
consistent IRI prefixes and predicate names across every seed artifact.

Predicate design:
- Standard RDF/XBRL predicates use their canonical IRIs (rdfs:label,
  skos:altLabel, owl:equivalentClass, etc).
- RoboSystems-specific predicates use the `rs:` prefix
  (https://robosystems.ai/vocab/).
- Taxonomy-specific prefixes (fac, rs-gaap, us-gaap, …) point at the
  authoritative namespaces used by Charlie Hoffman and FASB.
"""

from __future__ import annotations

# Base IRI for RoboSystems-owned predicates
RS_VOCAB = "https://robosystems.ai/vocab/"

# Canonical @context as a Python dict. Serialized directly to JSON-LD.
CANONICAL_CONTEXT: dict = {
  # RDF / semantic web
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
  "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
  "skos": "http://www.w3.org/2004/02/skos/core#",
  "owl": "http://www.w3.org/2002/07/owl#",
  "xsd": "http://www.w3.org/2001/XMLSchema#",
  "dcterms": "http://purl.org/dc/terms/",
  # XBRL core
  "xbrli": "http://www.xbrl.org/2003/instance#",
  "xbrl": "http://www.xbrl.org/2003/linkbase#",
  "xbrldt": "http://xbrl.org/2005/xbrldt#",
  # Taxonomy namespaces (external authorities).
  # XBRL schemas use '#' fragment separator between the targetNamespace
  # and the local element name, so concept IRIs need the '#' in the
  # prefix mapping to compact correctly.
  # Charlie publishes FAC under multiple target namespaces across
  # iterations. `fac` is pinned to the 2021/kg mapping variant since
  # that's the ingest target for the POC; `fac-luca` and
  # `fac-seattlemethod` are retained so concepts authored against those
  # older variants still compact to readable qnames.
  "fac": "http://www.xbrlsite.com/fac#",
  "fac-luca": "http://luca.auditchain.finance/fac#",
  "fac-seattlemethod": "http://xbrlsite.azurewebsites.net/seattlemethod/fac#",
  "us-gaap-2017": "http://fasb.org/us-gaap/2017-01-31#",
  "us-gaap-2020": "http://fasb.org/us-gaap/2020-01-31#",
  "us-gaap-2022": "http://fasb.org/us-gaap/2022-01-31#",
  "us-gaap-2024": "http://fasb.org/us-gaap/2024-01-31#",
  "us-gaap": "http://fasb.org/us-gaap/",
  # rs-gaap — RoboSystems's year-independent canonical reporting
  # taxonomy. Our namespace for concepts that previously lived under
  # us-gaap-2017; equivalence arcs bridge rs-gaap ↔ external us-gaap
  # versions, keeping our namespace stable as FASB evolves.
  "rs-gaap": "https://robosystems.ai/taxonomy/rs-gaap/v1/",
  "ifrs": "http://xbrl.ifrs.org/taxonomy/",
  "dei": "http://xbrl.sec.gov/dei/",
  # Seattle Method conceptual-model role URIs (Charlie's CM namespace)
  "cm-roles": "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/",
  # RoboSystems vocabulary
  "rs": RS_VOCAB,
  # Concept attributes — classification axes
  "classification": {"@id": f"{RS_VOCAB}classification"},
  "statementContext": {"@id": f"{RS_VOCAB}statementContext"},
  "derivationRole": {"@id": f"{RS_VOCAB}derivationRole"},
  "balance": {"@id": f"{RS_VOCAB}balance"},
  "periodType": {"@id": f"{RS_VOCAB}periodType"},
  "abstract": {"@id": f"{RS_VOCAB}abstract", "@type": "xsd:boolean"},
  "monetary": {"@id": f"{RS_VOCAB}monetary", "@type": "xsd:boolean"},
  "elementType": {"@id": f"{RS_VOCAB}elementType"},
  "substitutionGroup": {"@id": f"{RS_VOCAB}substitutionGroup", "@type": "@id"},
  "source": {"@id": f"{RS_VOCAB}source"},
  # Relationships
  "parent": {"@id": f"{RS_VOCAB}parent", "@type": "@id"},
  "equivalent": {"@id": "owl:equivalentClass", "@type": "@id"},
  "generalOf": {"@id": f"{RS_VOCAB}generalOf", "@type": "@id"},
  "summationOf": {"@id": f"{RS_VOCAB}summationOf", "@type": "@id"},
  "dimensionOf": {"@id": f"{RS_VOCAB}dimensionOf", "@type": "@id"},
  "hypercubeOf": {"@id": f"{RS_VOCAB}hypercubeOf", "@type": "@id"},
  # Association metadata
  "arcrole": {"@id": f"{RS_VOCAB}arcrole"},
  "role": {"@id": f"{RS_VOCAB}role"},
  "order": {"@id": f"{RS_VOCAB}order", "@type": "xsd:decimal"},
  "weight": {"@id": f"{RS_VOCAB}weight", "@type": "xsd:decimal"},
  # Labels
  "label": "rdfs:label",
  "altLabel": "skos:altLabel",
  "prefLabel": "skos:prefLabel",
  "documentation": "rdfs:comment",
  "labelRole": {"@id": f"{RS_VOCAB}labelRole"},
  "labelLanguage": {"@id": f"{RS_VOCAB}labelLanguage"},
  # References
  "references": {"@id": "dcterms:references"},
  "refType": {"@id": f"{RS_VOCAB}refType"},
  "citation": {"@id": f"{RS_VOCAB}citation"},
  # Structure (extended link roles)
  "structureName": {"@id": f"{RS_VOCAB}structureName"},
  "structureType": {"@id": f"{RS_VOCAB}structureType"},
  "roleUri": {"@id": f"{RS_VOCAB}roleUri"},
}


def context_document() -> dict:
  """Return a JSON-LD document with only the context (for seeds/context.jsonld).

  Consumers that import the context via a URL reference can point to this
  file. Sidecar artifact for discoverability.
  """
  return {"@context": CANONICAL_CONTEXT}
