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

  # v2 bundles carry graph-native period nodes, not XBRL contexts —
  # derive the contexts here (XBRL 2.1 requires shared <context> elements).
  contexts, ctx_for_period = _derive_contexts(bundle)
  for ctx in contexts:
    _append_context(root, ctx)
  for unit in bundle.units:
    _append_unit(root, unit)
  for fact in bundle.facts:
    _append_fact(root, fact, ctx_for_period[fact.period_ref])

  return root


def _derive_contexts(
  bundle: StatementBundle,
) -> tuple[list[BundleContext], dict[str, str]]:
  """Reconstruct ``<xbrli:context>`` set from the bundle's period nodes.

  v2 collapsed entity+period into per-fact references; XBRL 2.1 still
  needs shared contexts. Single entity per bundle → one context per
  period node. Returns ``(contexts, period_ref -> context_id)``.
  """
  contexts: list[BundleContext] = []
  ctx_for_period: dict[str, str] = {}
  for period in bundle.period_nodes:
    ctx_id = f"ctx_{period.id}"
    ctx_for_period[period.id] = ctx_id
    contexts.append(
      BundleContext(
        id=ctx_id,
        entity_identifier=bundle.entity.id,
        period_start=period.period_start,
        period_end=period.period_end,
        period_type=period.period_type,
      )
    )
  return contexts, ctx_for_period


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


def _append_fact(parent: etree._Element, fact: BundleFact, context_ref: str) -> None:
  """Emit a fact element typed by its concept qname.

  XBRL's "the element name IS the type tag" pattern — the fact element
  uses the concept qname as its tag, with contextRef / unitRef /
  decimals on attributes and the numeric value as the element text. The
  ``context_ref`` is derived (see :func:`_derive_contexts`) since v2
  facts carry a ``period_ref`` rather than an XBRL context ref.
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
    contextRef=context_ref,
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

  # Per XBRL 2.1 §5.1.2: ``link:linkbaseRef`` and ``link:roleType``
  # may only appear inside ``xs:schema/xs:annotation/xs:appinfo``.
  # Emitting them directly under the schema root produces an
  # ``xbrl.5.1.2.linkbaseRefLocation`` load error. The roleType
  # appinfo block is the same one — both kinds of XBRL annotation
  # share the wrapper.
  appinfo = _ensure_appinfo(root)
  _append_role_type_declarations(appinfo, bundle)

  # Per-concept declarations (under schema root, NOT inside appinfo —
  # element declarations are first-class schema content)
  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    _append_concept_declaration(root, concept)

  # Linkbase references go inside the SAME appinfo block as the
  # roleType declarations (XBRL spec requirement).
  if bundle.linkbases.presentation_links:
    _append_linkbase_ref(appinfo, "report-pre.xml", "presentationLinkbaseRef")
  if bundle.linkbases.calculation_links:
    _append_linkbase_ref(appinfo, "report-cal.xml", "calculationLinkbaseRef")
  if bundle.linkbases.definition_links:
    _append_linkbase_ref(appinfo, "report-def.xml", "definitionLinkbaseRef")

  return root


def _ensure_appinfo(schema_root: etree._Element) -> etree._Element:
  """Return the schema's ``<xs:annotation><xs:appinfo>`` element,
  creating both wrappers if they don't exist yet. Both ``link:roleType``
  and ``link:linkbaseRef`` go inside this block per XBRL 2.1 §5.1.2."""
  annotation = schema_root.find(f"{{{NS_XS}}}annotation")
  if annotation is None:
    annotation = etree.SubElement(schema_root, f"{{{NS_XS}}}annotation")
  appinfo = annotation.find(f"{{{NS_XS}}}appinfo")
  if appinfo is None:
    appinfo = etree.SubElement(annotation, f"{{{NS_XS}}}appinfo")
  return appinfo


def _append_role_type_declarations(
  appinfo: etree._Element, bundle: StatementBundle
) -> None:
  """Emit one ``<link:roleType>`` per unique custom ``xlink:role``
  referenced by the bundle's linkbases into the schema's appinfo block.

  XBRL 2.1 requires every Extended Link Role (ELR) URI used in a
  linkbase to be declared in a schema via ``<link:roleType>`` with a
  matching ``<link:usedOn>`` enumeration of the link types it may
  appear on. Without these, Arelle (and any other conformant XBRL
  processor) reports ``Role ... is missing a roleRef``.
  """
  used_on_per_role = _collect_role_usage(bundle)
  for role_uri, used_on in sorted(used_on_per_role.items()):
    role_type = etree.SubElement(
      appinfo,
      f"{{{NS_LINK}}}roleType",
      attrib={
        "id": _role_uri_to_id(role_uri),
        "roleURI": role_uri,
      },
    )
    definition = etree.SubElement(role_type, f"{{{NS_LINK}}}definition")
    definition.text = role_uri.rsplit("/", 1)[-1]
    for link_local in sorted(used_on):
      used_el = etree.SubElement(role_type, f"{{{NS_LINK}}}usedOn")
      used_el.text = f"link:{link_local}"


def _any_arcs(bundle: StatementBundle) -> bool:
  return any(
    bool(link.arcs)
    for link in (
      *bundle.linkbases.presentation_links,
      *bundle.linkbases.calculation_links,
      *bundle.linkbases.definition_links,
    )
  )


def _collect_role_usage(bundle: StatementBundle) -> dict[str, set[str]]:
  """Walk the bundle's linkbases and return ``{role_uri: {link_type, …}}``
  for every non-empty ``xlink:role`` referenced. Used to drive the
  roleType emission so each role declares exactly the link types it's
  actually used on (XBRL conformance requirement)."""
  by_role: dict[str, set[str]] = {}
  for link in bundle.linkbases.presentation_links:
    if link.role_uri and link.arcs:
      by_role.setdefault(link.role_uri, set()).add("presentationLink")
  for link in bundle.linkbases.calculation_links:
    if link.role_uri and link.arcs:
      by_role.setdefault(link.role_uri, set()).add("calculationLink")
  for link in bundle.linkbases.definition_links:
    if link.role_uri and link.arcs:
      by_role.setdefault(link.role_uri, set()).add("definitionLink")
  return by_role


def _role_uri_to_id(role_uri: str) -> str:
  """Mint a stable, NCName-valid id for a role URI.

  The id is what ``<link:roleRef xlink:href="report.xsd#…">`` targets
  from each linkbase; it must be a valid ``xs:ID`` and must agree on
  both sides of the reference. We use the role's last URI segment
  with non-NCName characters replaced by underscores and a leading
  ``role_`` prefix to guarantee a letter start regardless of how the
  taxonomy author named the role.
  """
  last = role_uri.rsplit("/", 1)[-1]
  sanitized = "".join(c if c.isalnum() or c in "-_." else "_" for c in last)
  return f"role_{sanitized}"


def _append_concept_declaration(parent: etree._Element, concept: BundleElement) -> None:
  """One ``<xs:element>`` per concept with XBRL attributes.

  Both ``id`` and ``name`` must be valid ``xs:NCName`` per the XML
  Schema spec — NCNames start with a letter or underscore and contain
  only letters, digits, hyphens, underscores, and periods. Two pitfalls
  this declaration avoids:

  * The bundle's internal id is a ULID (``elem_01K…``) which starts
    with a letter when prefixed but commonly starts with a digit when
    raw — failing Arelle's ``xs:ID`` regex. We use the qname-derived
    id (``rs-gaap_Assets``) so the schema id and the linkbase locator
    href ``report.xsd#rs-gaap_Assets`` agree and resolve cleanly.
  * The bundle's human-readable ``name`` (``"Treasury Stock, Value"``)
    contains spaces, commas, parentheses — none of which are NCName
    characters. We use the qname's local part (``TreasuryStockValue``)
    which is always a valid NCName by construction (taxonomy authors
    enforce this upstream).
  """
  attrs: dict[str, str] = {
    "id": _concept_id(concept),
    "name": _local_name(concept.qname),
    "type": (
      "xbrli:monetaryItemType" if concept.is_monetary else "xbrli:stringItemType"
    ),
    "substitutionGroup": concept.substitution_group or "xbrli:item",
    "abstract": "true" if concept.is_abstract else "false",
    "nillable": "true",
    f"{{{NS_XBRLI}}}periodType": concept.period_type,
  }
  # Per XBRL 2.1 §5.1.1.2: ``xbrli:balance`` is only valid on
  # monetary item types, and concept declarations marked abstract
  # must not carry it. Arelle reports
  # ``Item ... may not have a balance`` when we violate either.
  if concept.balance_type and not concept.is_abstract and concept.is_monetary:
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

  Must be a valid ``xs:ID`` (NCName) — letter or underscore start, no
  spaces / colons / other punctuation. We derive the id from the
  qname via ``_qname_to_id`` (``rs-gaap:Assets`` → ``rs-gaap_Assets``)
  so the schema-side id is naming-equivalent to the locator-side
  reference (linkbases already use ``_qname_to_id`` for ``xlink:href``
  fragments). Using ``concept.id`` directly would commonly produce a
  digit-leading ULID which is not a valid NCName; resolver mismatches
  between schema and linkbases would surface as Arelle ``MissingLoc``
  errors at validation time.
  """
  return _qname_to_id(concept.qname)


def _local_name(qname: str) -> str:
  """The part of a qname after the colon (``rs-gaap:Assets`` →
  ``Assets``). Used as the ``name`` attribute on ``<xs:element>``
  declarations, which must be a valid NCName — the prefix is dropped
  because the namespace comes from the schema's ``targetNamespace``,
  not the element name itself.
  """
  return qname.split(":", 1)[-1]


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
  """Schema-local id for a linkbase's roleRef href fragment.

  Must match the id emitted in the corresponding ``<link:roleType>``
  declaration in ``report.xsd`` so the cross-reference resolves at
  validation time. Both sides derive from ``_role_uri_to_id`` over the
  link's ``role_uri``; we no longer key on ``link.structure_id``
  because the same ELR (xlink:role URI) can be reused across multiple
  Networks and the schema-side declaration is keyed on the role URI,
  not the structure.
  """
  return (
    _role_uri_to_id(link.role_uri) if link.role_uri else f"role_{link.structure_id}"
  )


def _format_decimal(value: float) -> str:
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)
