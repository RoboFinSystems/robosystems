"""XBRL 2.1 emitter — walks the v1.0 bundle, produces a flat zip.

Phase 1b ships the standalone files (``instance.xml`` + ``report.xsd``
+ ``report-pre.xml`` + ``report-cal.xml`` + ``report-def.xml``) zipped
together. Phase 2 wraps them in the full XBRL Report Package shape
(``META-INF/taxonomyPackage.xml`` and related). Phase 4+ adds label
and reference linkbases.

Walks the same :class:`StatementBundle` as the JSON-LD encoder — the
two share an envelope, not just a fact set. Per the v1.0 ontology spec
§7, ``rs:`` extensions are dropped from the XBRL output (they have no
place in standards-compliant XBRL); v1.0 round-trip is at the fact
level.

Hand-emitted with lxml rather than Arelle's ``saveInstance``. Arelle's
XBRL-emit path requires constructing ``ModelInstanceObject`` /
``ModelConcept`` / etc. internals; lxml builds the XML tree directly
with ~10x less code. Arelle is used downstream for *validation* of
emitted output (round-trip harness, Phase 1b.3).
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime
from decimal import Decimal

from lxml import etree

from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleContext,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleUnit,
  StatementBundle,
)

# ── Namespace constants (matching the v1.0 ontology) ─────────────────────

NS_XBRLI = "http://www.xbrl.org/2003/instance"
NS_LINK = "http://www.xbrl.org/2003/linkbase"
NS_XLINK = "http://www.w3.org/1999/xlink"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_XS = "http://www.w3.org/2001/XMLSchema"
NS_ISO4217 = "http://www.xbrl.org/2003/iso4217"
# rs-gaap is the canonical reporting taxonomy emitted into report.xsd
# under its published namespace IRI; concepts referenced from other
# frameworks (fac, us-gaap, ifrs, dei, etc.) are emitted under their
# own namespaces via the prefix table below.
NS_RS_GAAP = "https://robosystems.ai/taxonomy/rs-gaap/v1/"
NS_FAC = "http://www.xbrlsite.com/fac"
NS_US_GAAP = "http://fasb.org/us-gaap"
NS_IFRS = "http://xbrl.ifrs.org/taxonomy"
NS_DEI = "http://xbrl.sec.gov/dei"
NS_DISCLOSURES = "https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/"

# Maps a bundle prefix → emitted XBRL namespace URI.
_PREFIX_TO_NAMESPACE: dict[str, str] = {
  "rs-gaap": NS_RS_GAAP,
  "fac": NS_FAC,
  "us-gaap": NS_US_GAAP,
  "ifrs": NS_IFRS,
  "dei": NS_DEI,
  "disclosures": NS_DISCLOSURES,
  "iso4217": NS_ISO4217,
  "xbrli": NS_XBRLI,
}


# ── Public entry point ───────────────────────────────────────────────────


def serialize_to_xbrl_21(bundle: StatementBundle) -> bytes:
  """Emit the bundle as a flat-zip XBRL 2.1 Report Package.

  Returns the zip bytes ready to stream as a download or write to
  storage. Phase 1b shape is a flat zip containing standalone files;
  Phase 2 will wrap in the full Report Package META-INF directory.
  """
  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("instance.xml", _serialize_xml(_build_instance(bundle)))
    zf.writestr("report.xsd", _serialize_xml(_build_schema(bundle)))
    zf.writestr("report-pre.xml", _serialize_xml(_build_presentation_linkbase(bundle)))
    zf.writestr("report-cal.xml", _serialize_xml(_build_calculation_linkbase(bundle)))
    zf.writestr("report-def.xml", _serialize_xml(_build_definition_linkbase(bundle)))
  return buf.getvalue()


def _serialize_xml(root: etree._Element) -> bytes:
  """Serialize an lxml tree to UTF-8 XML bytes with declaration."""
  return etree.tostring(
    root,
    xml_declaration=True,
    encoding="UTF-8",
    pretty_print=True,
    standalone=True,
  )


# ── instance.xml ─────────────────────────────────────────────────────────


def _build_instance(bundle: StatementBundle) -> etree._Element:
  """Build the XBRL instance document with contexts, units, and facts.

  Structure follows XBRL 2.1: ``<xbrli:xbrl>`` root with a
  ``<link:schemaRef>`` pointing at the bundled ``report.xsd``, then a
  flat sequence of ``<xbrli:context>`` blocks, ``<xbrli:unit>`` blocks,
  and one fact element per :class:`BundleFact` typed by its concept
  qname.
  """
  nsmap = _build_instance_nsmap(bundle)
  root = etree.Element(
    f"{{{NS_XBRLI}}}xbrl", nsmap=nsmap, attrib=_xsi_schema_location()
  )

  # schemaRef → bundled report.xsd
  schema_ref = etree.SubElement(root, f"{{{NS_LINK}}}schemaRef")
  schema_ref.set(f"{{{NS_XLINK}}}type", "simple")
  schema_ref.set(f"{{{NS_XLINK}}}href", "report.xsd")

  for ctx in bundle.contexts:
    _append_context(root, ctx)
  for unit in bundle.units:
    _append_unit(root, unit)
  for fact in bundle.facts:
    _append_fact(root, fact)

  return root


def _build_instance_nsmap(bundle: StatementBundle) -> dict[str | None, str]:
  """Build the namespace map for the instance root, including every
  framework prefix referenced by facts or concepts."""
  ns: dict[str | None, str] = {
    "xbrli": NS_XBRLI,
    "link": NS_LINK,
    "xlink": NS_XLINK,
    "xsi": NS_XSI,
    "iso4217": NS_ISO4217,
  }
  for prefix in _bundle_framework_prefixes(bundle):
    if prefix in _PREFIX_TO_NAMESPACE:
      ns[prefix] = _PREFIX_TO_NAMESPACE[prefix]
  return ns


def _bundle_framework_prefixes(bundle: StatementBundle) -> set[str]:
  """Collect every framework prefix referenced in the bundle (from
  concept qnames and fact element qnames)."""
  prefixes: set[str] = set()
  for concept in bundle.schema_concepts:
    if ":" in concept.qname:
      prefixes.add(concept.qname.split(":", 1)[0])
  for fact in bundle.facts:
    if ":" in fact.element_qname:
      prefixes.add(fact.element_qname.split(":", 1)[0])
  return prefixes


def _xsi_schema_location() -> dict[str, str]:
  """xsi:schemaLocation pointing at the XBRL 2.1 instance schema +
  the bundled report.xsd. Standard XBRL processors use this to
  resolve concept declarations."""
  return {
    f"{{{NS_XSI}}}schemaLocation": (
      f"{NS_XBRLI} http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd "
      f"{NS_RS_GAAP} report.xsd"
    )
  }


def _append_context(parent: etree._Element, ctx: BundleContext) -> None:
  context = etree.SubElement(parent, f"{{{NS_XBRLI}}}context", id=ctx.id)
  entity = etree.SubElement(context, f"{{{NS_XBRLI}}}entity")
  identifier = etree.SubElement(
    entity, f"{{{NS_XBRLI}}}identifier", scheme=ctx.entity_scheme
  )
  identifier.text = ctx.entity_identifier
  period = etree.SubElement(context, f"{{{NS_XBRLI}}}period")
  if ctx.period_type == "instant":
    instant = etree.SubElement(period, f"{{{NS_XBRLI}}}instant")
    instant.text = ctx.period_end.isoformat()
  else:
    start = etree.SubElement(period, f"{{{NS_XBRLI}}}startDate")
    start.text = (ctx.period_start or ctx.period_end).isoformat()
    end = etree.SubElement(period, f"{{{NS_XBRLI}}}endDate")
    end.text = ctx.period_end.isoformat()


def _append_unit(parent: etree._Element, unit: BundleUnit) -> None:
  unit_el = etree.SubElement(parent, f"{{{NS_XBRLI}}}unit", id=unit.id)
  measure = etree.SubElement(unit_el, f"{{{NS_XBRLI}}}measure")
  measure.text = unit.measure


def _append_fact(parent: etree._Element, fact: BundleFact) -> None:
  """Emit a fact element typed by its concept qname.

  XBRL's "the element name IS the type tag" pattern — the fact element
  uses the concept qname as its tag, with contextRef / unitRef /
  decimals on attributes and the numeric value as the element text.
  """
  if ":" in fact.element_qname:
    prefix, local = fact.element_qname.split(":", 1)
    namespace = _PREFIX_TO_NAMESPACE.get(prefix, NS_RS_GAAP)
    tag = f"{{{namespace}}}{local}"
  else:
    tag = fact.element_qname
  fact_el = etree.SubElement(
    parent,
    tag,
    contextRef=fact.context_ref,
    unitRef=fact.unit_ref,
    decimals=fact.decimals,
  )
  fact_el.text = _format_value(fact.value)


def _format_value(value: float) -> str:
  """Format a numeric fact value the XBRL way.

  Integer-valued floats render without a decimal point (XBRL convention
  for whole-currency amounts). Non-integers preserve full precision.
  """
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)


# ── report.xsd ───────────────────────────────────────────────────────────


def _build_schema(bundle: StatementBundle) -> etree._Element:
  """Build the per-report XBRL taxonomy schema document.

  Declares one ``<xs:element>`` per concept in the bundle, under the
  rs-gaap targetNamespace (concepts emitted into this schema ARE
  rs-gaap concepts; the schema completes the rs-gaap declarations
  scoped to this report). Linkbase references at the bottom point at
  the bundled linkbase files.
  """
  nsmap: dict[str | None, str] = {
    "xs": NS_XS,
    "xbrli": NS_XBRLI,
    "link": NS_LINK,
    "xlink": NS_XLINK,
    "rs-gaap": NS_RS_GAAP,
  }
  root = etree.Element(
    f"{{{NS_XS}}}schema",
    nsmap=nsmap,
    attrib={
      "targetNamespace": NS_RS_GAAP,
      "elementFormDefault": "qualified",
    },
  )

  # Import xbrli — required so xbrli:item / xbrli:monetaryItemType etc.
  # resolve at the schema-validation level.
  etree.SubElement(
    root,
    f"{{{NS_XS}}}import",
    attrib={
      "namespace": NS_XBRLI,
      "schemaLocation": ("http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd"),
    },
  )

  # Per-concept declarations
  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    _append_concept_declaration(root, concept)

  # Linkbase references — each points at the bundled linkbase file
  if bundle.linkbases.presentation_links:
    _append_linkbase_ref(root, "report-pre.xml", "presentationLinkbaseRef")
  if bundle.linkbases.calculation_links:
    _append_linkbase_ref(root, "report-cal.xml", "calculationLinkbaseRef")
  if bundle.linkbases.definition_links:
    _append_linkbase_ref(root, "report-def.xml", "definitionLinkbaseRef")

  return root


def _append_concept_declaration(parent: etree._Element, concept: BundleElement) -> None:
  """One ``<xs:element>`` per concept with XBRL attributes."""
  attrs: dict[str, str] = {
    "id": _concept_id(concept),
    "name": concept.name,
    "type": (
      "xbrli:monetaryItemType" if concept.is_monetary else "xbrli:stringItemType"
    ),
    "substitutionGroup": concept.substitution_group or "xbrli:item",
    "abstract": "true" if concept.is_abstract else "false",
    "nillable": "true",
    f"{{{NS_XBRLI}}}periodType": concept.period_type,
  }
  if concept.balance_type:
    attrs[f"{{{NS_XBRLI}}}balance"] = concept.balance_type
  etree.SubElement(parent, f"{{{NS_XS}}}element", attrib=attrs)


def _append_linkbase_ref(parent: etree._Element, href: str, role_local: str) -> None:
  """Append a ``<link:linkbaseRef>`` pointing at a bundled linkbase
  file. The role uses the XBRL standard linkbase-role URIs."""
  etree.SubElement(
    parent,
    f"{{{NS_LINK}}}linkbaseRef",
    attrib={
      f"{{{NS_XLINK}}}type": "simple",
      f"{{{NS_XLINK}}}href": href,
      f"{{{NS_XLINK}}}role": f"http://www.xbrl.org/2003/role/{role_local}",
      f"{{{NS_XLINK}}}arcrole": "http://www.w3.org/1999/xlink/properties/linkbase",
    },
  )


def _concept_id(concept: BundleElement) -> str:
  """Schema-local id for a concept — used as the ``xlink:href`` fragment
  target from linkbase locators (``report.xsd#<id>``).

  Prefer the bundle's internal id (stable ULID) over the qname so the
  same concept resolves consistently across regenerations even if a
  framework migration re-qnames it.
  """
  return concept.id


# ── report-{pre,cal,def}.xml — linkbase emitters ─────────────────────────


def _build_presentation_linkbase(bundle: StatementBundle) -> etree._Element:
  return _build_linkbase(
    links=bundle.linkbases.presentation_links,
    link_local="presentationLink",
    arc_local="presentationArc",
    include_weight=False,
  )


def _build_calculation_linkbase(bundle: StatementBundle) -> etree._Element:
  return _build_linkbase(
    links=bundle.linkbases.calculation_links,
    link_local="calculationLink",
    arc_local="calculationArc",
    include_weight=True,
  )


def _build_definition_linkbase(bundle: StatementBundle) -> etree._Element:
  return _build_linkbase(
    links=bundle.linkbases.definition_links,
    link_local="definitionLink",
    arc_local="definitionArc",
    include_weight=False,
  )


def _build_linkbase(
  links: list[BundleLinkbaseLink],
  link_local: str,
  arc_local: str,
  include_weight: bool,
) -> etree._Element:
  """Build a presentation / calculation / definition linkbase document.

  Shape: ``<link:linkbase>`` root wrapping one ``<link:roleRef>`` per
  ELR plus one ``<link:{presentationLink}>`` per ELR. Each link wraps
  one ``<link:loc>`` per distinct concept endpoint and one
  ``<link:{presentationArc}>`` per arc.
  """
  nsmap: dict[str | None, str] = {
    "link": NS_LINK,
    "xlink": NS_XLINK,
    "xsi": NS_XSI,
  }
  root = etree.Element(
    f"{{{NS_LINK}}}linkbase",
    nsmap=nsmap,
    attrib={
      f"{{{NS_XSI}}}schemaLocation": (
        f"{NS_LINK} http://www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd"
      ),
    },
  )

  for link in links:
    if link.role_uri:
      etree.SubElement(
        root,
        f"{{{NS_LINK}}}roleRef",
        attrib={
          f"{{{NS_XLINK}}}type": "simple",
          f"{{{NS_XLINK}}}href": f"report.xsd#{_role_id(link)}",
          "roleURI": link.role_uri,
        },
      )

  for link in links:
    _append_link_block(root, link, link_local, arc_local, include_weight)

  return root


def _append_link_block(
  parent: etree._Element,
  link: BundleLinkbaseLink,
  link_local: str,
  arc_local: str,
  include_weight: bool,
) -> None:
  """One ``<link:presentationLink>`` (or calc/def) with locs + arcs."""
  link_attrs: dict[str, str] = {
    f"{{{NS_XLINK}}}type": "extended",
  }
  if link.role_uri:
    link_attrs[f"{{{NS_XLINK}}}role"] = link.role_uri
  link_el = etree.SubElement(parent, f"{{{NS_LINK}}}{link_local}", attrib=link_attrs)

  # Emit one <link:loc> per distinct concept referenced by the link's
  # arcs. XLink labels are arbitrary but conventional — use the concept
  # qname (with ":" replaced for XML id compatibility) so debugging is
  # easier when comparing emitted vs reference linkbases.
  concept_qnames: set[str] = set()
  for arc in link.arcs:
    concept_qnames.add(arc.from_qname)
    concept_qnames.add(arc.to_qname)
  for qname in sorted(concept_qnames):
    etree.SubElement(
      link_el,
      f"{{{NS_LINK}}}loc",
      attrib={
        f"{{{NS_XLINK}}}type": "locator",
        f"{{{NS_XLINK}}}href": f"report.xsd#{_qname_to_id(qname)}",
        f"{{{NS_XLINK}}}label": _qname_to_label(qname),
      },
    )

  # Emit one arc per BundleArc
  for arc in link.arcs:
    _append_arc(link_el, arc, arc_local, include_weight)


def _append_arc(
  link_el: etree._Element,
  arc: BundleArc,
  arc_local: str,
  include_weight: bool,
) -> None:
  attrs: dict[str, str] = {
    f"{{{NS_XLINK}}}type": "arc",
    f"{{{NS_XLINK}}}arcrole": arc.arcrole,
    f"{{{NS_XLINK}}}from": _qname_to_label(arc.from_qname),
    f"{{{NS_XLINK}}}to": _qname_to_label(arc.to_qname),
  }
  if arc.order_value is not None:
    attrs["order"] = _format_decimal(arc.order_value)
  if include_weight and arc.weight is not None:
    attrs["weight"] = _format_decimal(arc.weight)
  etree.SubElement(link_el, f"{{{NS_LINK}}}{arc_local}", attrib=attrs)


def _qname_to_id(qname: str) -> str:
  """Convert ``prefix:Local`` to ``prefix_Local`` for schema id refs.

  Linkbase locators point at ``report.xsd#<id>`` and schema ids can't
  contain ``:``. We use the bundled element's internal id where
  possible; this fallback covers cases where the locator points at a
  framework concept not in our schema slice (rare in v1.0 since the
  schema slice covers everything referenced).
  """
  return qname.replace(":", "_")


def _qname_to_label(qname: str) -> str:
  """XLink label — same convention as the schema id."""
  return _qname_to_id(qname)


def _role_id(link: BundleLinkbaseLink) -> str:
  """Schema-local id for the role declaration. v1.0 uses the structure_id
  as the id; XBRL 2.1 requires roles be declared in a schema (we omit
  this for now — Phase 2 adds the proper ``<link:roleType>`` declarations
  to report.xsd)."""
  return f"role_{link.structure_id}"


def _format_decimal(value: float) -> str:
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)


# ── Utility (kept for symmetry; not currently used) ──────────────────────


def _emit_timestamp() -> str:
  return datetime.utcnow().isoformat() + "Z"
