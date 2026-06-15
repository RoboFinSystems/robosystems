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
  # XBRL core. The linkbase namespace is bound to `link` (XBRL-conventional
  # and matching the export bundle); `xlink` + `xbrldi` are bound so reified
  # arcs and dimensional members compact cleanly. `iso4217` is bound for
  # instance-side unit measures.
  "xbrli": "http://www.xbrl.org/2003/instance#",
  "link": "http://www.xbrl.org/2003/linkbase#",
  "xlink": "http://www.w3.org/1999/xlink#",
  "xbrldt": "http://xbrl.org/2005/xbrldt#",
  "xbrldi": "http://xbrl.org/2006/xbrldi#",
  "iso4217": "http://www.xbrl.org/2003/iso4217#",
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
  # rs-gaap-disclosures — named Disclosures (BalanceSheet, IncomeStatement,
  # PropertyPlantAndEquipmentDisclosure, …) anchored to the rs-gaap framework.
  # Each entry is an abstract qname-addressable element AND a Structure with
  # CAP + secType metadata. Sibling namespace to rs-gaap, not nested under it.
  "disclosures": "https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/",
  # rs-gaap-reporting-checklist — declares the abstract "FinancialReport"
  # subjects that a Reporting Checklist's `financialReport-requiresDisclosure`
  # arcs anchor on.
  "checklist": "https://robosystems.ai/taxonomy/rs-gaap/reporting-checklist/v1/",
  # rs-gaap-reporting-styles — declares Style entities that compose specific
  # Disclosures for a vertical / filer profile.
  "styles": "https://robosystems.ai/taxonomy/rs-gaap/reporting-styles/v1/",
  "ifrs": "http://xbrl.ifrs.org/taxonomy/",
  "dei": "http://xbrl.sec.gov/dei/",
  # Seattle Method conceptual-model role URIs (Charlie's CM namespace)
  "cm-roles": "http://www.xbrlsite.com/seattlemethod/conceptual-model/cm-roles/roles/",
  # cm — Seattle Method 'universal' conceptual model (Charlie Hoffman). The
  # Debit/Credit posting-role concepts anchor has-part arcs from Chart-of-
  # Accounts elements. '#' fragment separator so concept IRIs compact to
  # cm:Debit / cm:Credit.
  "cm": "https://github.com/seattlemethod/universal/cm#",
  # RoboSystems vocabulary
  "rs": RS_VOCAB,
  # Concept attributes — XBRL vocabulary where XBRL defines the attribute
  # (balance, periodType), rs: for our denormalized booleans/axes that XBRL
  # has no predicate for (monetary, abstract, elementType, classification …).
  "classification": {"@id": f"{RS_VOCAB}classification"},
  "statementContext": {"@id": f"{RS_VOCAB}statementContext"},
  "derivationRole": {"@id": f"{RS_VOCAB}derivationRole"},
  "balance": {"@id": "xbrli:balance"},
  "periodType": {"@id": "xbrli:periodType"},
  "abstract": {"@id": f"{RS_VOCAB}abstract", "@type": "xsd:boolean"},
  "monetary": {"@id": f"{RS_VOCAB}monetary", "@type": "xsd:boolean"},
  "elementType": {"@id": f"{RS_VOCAB}elementType"},
  "substitutionGroup": {"@id": f"{RS_VOCAB}substitutionGroup", "@type": "@id"},
  "source": {"@id": f"{RS_VOCAB}source"},
  # Relationships. Structural taxonomy arcs (presentation / calculation /
  # definition) are REIFIED as rs:Association nodes carrying xlink:from/to +
  # xlink:arcrole + link:weight/order — the direct-predicate terms
  # (parent/summationOf/generalOf/dimensionOf/hypercubeOf) are RETIRED.
  # `equivalent` stays a direct owl:equivalentClass predicate: it is a genuine
  # symmetric OWL relation with no weight/order/role to carry, so reifying it
  # would gain nothing and lose the OWL semantics the bridges rely on.
  "equivalent": {"@id": "owl:equivalentClass", "@type": "@id"},
  # Reified-association predicates (one rs:Association node per arc)
  "from": {"@id": "xlink:from", "@type": "@id"},
  "to": {"@id": "xlink:to", "@type": "@id"},
  "arcrole": {"@id": "xlink:arcrole", "@type": "@id"},
  "role": {"@id": "xlink:role", "@type": "@id"},
  "order": {"@id": "link:order", "@type": "xsd:decimal"},
  "weight": {"@id": "link:weight", "@type": "xsd:decimal"},
  "associationType": {"@id": f"{RS_VOCAB}associationType"},
  "preferredLabel": {"@id": f"{RS_VOCAB}preferredLabel"},
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
  "blockType": {"@id": f"{RS_VOCAB}blockType"},
  "roleUri": {"@id": f"{RS_VOCAB}roleUri"},
  "conceptArrangementPattern": {"@id": f"{RS_VOCAB}conceptArrangementPattern"},
  "hasAssociation": {"@id": f"{RS_VOCAB}hasAssociation", "@type": "@id"},
  # Instance layer — Fact + its aspects, mirroring the graph's FACT_HAS_*
  # edges (Fact → Element / Entity / Period / Unit / Dimension). No XBRL
  # `context` exists here: a Fact references its aspects directly.
  "element": {"@id": f"{RS_VOCAB}element", "@type": "@id"},
  "entity": {"@id": f"{RS_VOCAB}entity", "@type": "@id"},
  "period": {"@id": f"{RS_VOCAB}period", "@type": "@id"},
  "unit": {"@id": f"{RS_VOCAB}unit", "@type": "@id"},
  "dimension": {"@id": f"{RS_VOCAB}dimension", "@type": "@id"},
  "factSet": {"@id": f"{RS_VOCAB}factSet", "@type": "@id"},
  "structure": {"@id": f"{RS_VOCAB}structure", "@type": "@id"},
  "numericValue": {"@id": f"{RS_VOCAB}numericValue", "@type": "xsd:decimal"},
  "decimals": {"@id": f"{RS_VOCAB}decimals"},
  # Period node — period kind uses XBRL's instant/duration vocabulary
  "instant": {"@id": "xbrli:instant", "@type": "xsd:date"},
  "startDate": {"@id": "xbrli:startDate", "@type": "xsd:date"},
  "endDate": {"@id": "xbrli:endDate", "@type": "xsd:date"},
  "calendarPeriodKey": {"@id": f"{RS_VOCAB}calendarPeriodKey"},
  # Unit node
  "measure": {"@id": "xbrli:measure", "@type": "@id"},
  # Dimension node
  "axis": {"@id": f"{RS_VOCAB}axis"},
  "member": {"@id": f"{RS_VOCAB}member"},
  # Entity / report-bundle header (rs: — no XBRL equivalent)
  "scheme": {"@id": f"{RS_VOCAB}scheme", "@type": "@id"},
  "legalName": {"@id": f"{RS_VOCAB}legalName"},
  "ein": {"@id": f"{RS_VOCAB}ein"},
  "country": {"@id": f"{RS_VOCAB}country"},
  "reportingStyle": {"@id": f"{RS_VOCAB}reportingStyle"},
  "serializationVersion": {"@id": f"{RS_VOCAB}serializationVersion"},
  "mode": {"@id": f"{RS_VOCAB}mode"},
  "internalId": {"@id": f"{RS_VOCAB}internalId"},
  # ── Domain / package terms ─────────────────────────────────────────────
  # Every term any framework seed uses must live here so the one canonical
  # context is a true superset — undeclared terms would either drop on parse
  # or compact to ugly rs:-prefixed keys. These are pure binary relations
  # (drules), rule/trait/style metadata, and structure annotations; none are
  # the retired structural-arc dialect.
  "category": {"@id": f"{RS_VOCAB}category"},
  "classifiedAs": {"@id": f"{RS_VOCAB}classifiedAs", "@type": "@id"},
  "deprecated": {"@id": f"{RS_VOCAB}deprecated", "@type": "xsd:boolean"},
  "replacedBy": {"@id": f"{RS_VOCAB}replacedBy", "@type": "@id"},
  "orphan2026": {"@id": f"{RS_VOCAB}orphan2026", "@type": "xsd:boolean"},
  "identifier": {"@id": f"{RS_VOCAB}identifier"},
  "secType": {"@id": f"{RS_VOCAB}secType"},
  "hasTrait": {"@id": f"{RS_VOCAB}hasTrait", "@type": "@id"},
  "trait": "https://robosystems.ai/taxonomy/fac-traits/v1/",
  "sfac6": "http://xbrlsite.com/seattlemethod/sfac6#",
  # Disclosure / checklist / style "drules" — direct binary relations
  "reportedDisclosureRequiresDisclosure": {
    "@id": f"{RS_VOCAB}reportedDisclosureRequiresDisclosure",
    "@type": "@id",
  },
  "conceptArrangementPatternRequiresConcept": {
    "@id": f"{RS_VOCAB}conceptArrangementPatternRequiresConcept",
    "@type": "@id",
  },
  "disclosureRequiresHypercube": {
    "@id": f"{RS_VOCAB}disclosureRequiresHypercube",
    "@type": "@id",
  },
  "disclosureRequiresConcept": {
    "@id": f"{RS_VOCAB}disclosureRequiresConcept",
    "@type": "@id",
  },
  "disclosureEquivalentTextblock": {
    "@id": f"{RS_VOCAB}disclosureEquivalentTextblock",
    "@type": "@id",
  },
  "financialReportRequiresDisclosure": {
    "@id": f"{RS_VOCAB}financialReportRequiresDisclosure",
    "@type": "@id",
  },
  "financialReportPossibleDisclosure": {
    "@id": f"{RS_VOCAB}financialReportPossibleDisclosure",
    "@type": "@id",
  },
  "disclosureAllowedAlternativeDisclosure": {
    "@id": f"{RS_VOCAB}disclosureAllowedAlternativeDisclosure",
    "@type": "@id",
  },
  "reportingStyleComposesDisclosure": {
    "@id": f"{RS_VOCAB}reportingStyleComposesDisclosure",
    "@type": "@id",
  },
  # Reporting-style composition (read as raw JSON by the style seeder)
  "reportingStyleCode": {"@id": f"{RS_VOCAB}reportingStyleCode"},
  "retainedEarningsConcept": {"@id": f"{RS_VOCAB}retainedEarningsConcept"},
  "reportingStyleNetworks": {"@id": f"{RS_VOCAB}reportingStyleNetworks"},
  "statementType": {"@id": f"{RS_VOCAB}statementType"},
  "networkRoleUri": {"@id": f"{RS_VOCAB}networkRoleUri"},
  # Validation-rule terms (rs-gaap-rules / rollup-rules packages)
  "ruleTarget": {"@id": f"{RS_VOCAB}ruleTarget"},
  "ruleCategory": {"@id": f"{RS_VOCAB}ruleCategory"},
  "rulePattern": {"@id": f"{RS_VOCAB}rulePattern"},
  "ruleSeverity": {"@id": f"{RS_VOCAB}ruleSeverity"},
  "ruleExpression": {"@id": f"{RS_VOCAB}ruleExpression"},
  "ruleMessage": {"@id": f"{RS_VOCAB}ruleMessage"},
  "ruleOrigin": {"@id": f"{RS_VOCAB}ruleOrigin"},
  "ruleVariables": {"@id": f"{RS_VOCAB}ruleVariables", "@container": "@list"},
  "variableName": {"@id": f"{RS_VOCAB}variableName"},
  "variableQname": {"@id": f"{RS_VOCAB}variableQname"},
  "targetKind": {"@id": f"{RS_VOCAB}targetKind"},
  "targetRef": {"@id": f"{RS_VOCAB}targetRef"},
}


def context_document() -> dict:
  """Return a JSON-LD document with only the context (for seeds/context.jsonld).

  Consumers that import the context via a URL reference can point to this
  file. Sidecar artifact for discoverability.
  """
  return {"@context": CANONICAL_CONTEXT}
