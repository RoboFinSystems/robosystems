"""XBRL 2.1 emitter: a :class:`StatementBundle` to a flat zip of instance,
schema and linkbase files.

``rs:`` extensions are dropped. The Report Package ``META-INF`` wrapper and
reference linkbases are not emitted. Built with lxml directly; Arelle is only
used downstream to validate the output.
"""

from __future__ import annotations

import io
import zipfile
from decimal import Decimal

from lxml import etree

from robosystems.operations.information_block.envelope import DISCLOSURE_BLOCK_TYPE
from robosystems.operations.serialization.bundle import (
  BundleArc,
  BundleContext,
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundleUnit,
  StatementBundle,
  concept_label,
)

# ── Namespace constants ──────────────────────────────────────────────────

NS_XBRLI = "http://www.xbrl.org/2003/instance"
NS_LINK = "http://www.xbrl.org/2003/linkbase"
NS_XLINK = "http://www.w3.org/1999/xlink"
NS_XSI = "http://www.w3.org/2001/XMLSchema-instance"
NS_XS = "http://www.w3.org/2001/XMLSchema"
NS_XML = "http://www.w3.org/XML/1998/namespace"
NS_ISO4217 = "http://www.xbrl.org/2003/iso4217"
NS_RS_GAAP = "https://robosystems.ai/taxonomy/rs-gaap/v1/"
NS_FAC = "http://www.xbrlsite.com/fac"
NS_US_GAAP = "http://fasb.org/us-gaap"
NS_IFRS = "http://xbrl.ifrs.org/taxonomy"
NS_DEI = "http://xbrl.sec.gov/dei"
NS_DISCLOSURES = "https://robosystems.ai/taxonomy/rs-gaap/disclosures/v1/"

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
  """Emit the bundle as flat-zip XBRL 2.1 bytes.

  Disclosure notes are stripped (they ride in the JSON-LD / holon flavors),
  and any remaining Nonnumeric fact is dropped: the emitter is numeric-only.
  """
  bundle = _strip_disclosure_content(bundle)
  if any(f.value is None for f in bundle.facts):
    bundle = bundle.model_copy(
      update={"facts": [f for f in bundle.facts if f.value is not None]}
    )
  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("instance.xml", _serialize_xml(_build_instance(bundle)))
    zf.writestr("report.xsd", _serialize_xml(_build_schema(bundle)))
    # Same gates as the linkbaseRefs in ``_build_schema``.
    if bundle.linkbases.presentation_links:
      zf.writestr(
        "report-pre.xml", _serialize_xml(_build_presentation_linkbase(bundle))
      )
    if bundle.linkbases.calculation_links:
      zf.writestr("report-cal.xml", _serialize_xml(_build_calculation_linkbase(bundle)))
    if bundle.linkbases.definition_links:
      zf.writestr("report-def.xml", _serialize_xml(_build_definition_linkbase(bundle)))
    if _has_labels(bundle):
      zf.writestr("report-lab.xml", _serialize_xml(_build_label_linkbase(bundle)))
  return buf.getvalue()


def _serialize_xml(root: etree._Element) -> bytes:
  return etree.tostring(
    root,
    xml_declaration=True,
    encoding="UTF-8",
    pretty_print=True,
    standalone=True,
  )


def _strip_disclosure_content(bundle: StatementBundle) -> StatementBundle:
  """Return the bundle without disclosure structures' links, facts and concepts.

  This emitter is statement-shaped: fixed framework prefixes (extension
  concepts get no namespace), one arc type per ELR, and no per-fact context
  typing, so a note would produce an instance Arelle rejects. Returns the
  same object when there is nothing to strip.
  """
  disclosure_structure_ids = {
    link.structure_id
    for group in (
      bundle.linkbases.presentation_links,
      bundle.linkbases.calculation_links,
      bundle.linkbases.definition_links,
    )
    for link in group
    if link.block_type == DISCLOSURE_BLOCK_TYPE
  }
  if not disclosure_structure_ids:
    return bundle

  linkbases = bundle.linkbases.model_copy(
    update={
      "presentation_links": [
        li
        for li in bundle.linkbases.presentation_links
        if li.structure_id not in disclosure_structure_ids
      ],
      "calculation_links": [
        li
        for li in bundle.linkbases.calculation_links
        if li.structure_id not in disclosure_structure_ids
      ],
      "definition_links": [
        li
        for li in bundle.linkbases.definition_links
        if li.structure_id not in disclosure_structure_ids
      ],
    }
  )
  facts = [
    f
    for f in bundle.facts
    if f.structure_id is None or f.structure_id not in disclosure_structure_ids
  ]

  # Otherwise note-only extension concepts get declared under rs-gaap.
  referenced_qnames = {f.element_qname for f in facts}
  for group in (
    linkbases.presentation_links,
    linkbases.calculation_links,
    linkbases.definition_links,
  ):
    for link in group:
      for arc in link.arcs:
        referenced_qnames.add(arc.from_qname)
        referenced_qnames.add(arc.to_qname)
  schema_concepts = [c for c in bundle.schema_concepts if c.qname in referenced_qnames]

  referenced_periods = {f.period_ref for f in facts}
  period_nodes = [p for p in bundle.period_nodes if p.id in referenced_periods]

  referenced_units = {f.unit_ref for f in facts}
  units = [u for u in bundle.units if u.id in referenced_units]

  return bundle.model_copy(
    update={
      "linkbases": linkbases,
      "facts": facts,
      "schema_concepts": schema_concepts,
      "period_nodes": period_nodes,
      "units": units,
    }
  )


# ── instance.xml ─────────────────────────────────────────────────────────


def _build_instance(bundle: StatementBundle) -> etree._Element:
  """Build the XBRL instance document: schemaRef, contexts, units, facts."""
  nsmap = _build_instance_nsmap(bundle)
  root = etree.Element(
    f"{{{NS_XBRLI}}}xbrl", nsmap=nsmap, attrib=_xsi_schema_location()
  )

  schema_ref = etree.SubElement(root, f"{{{NS_LINK}}}schemaRef")
  schema_ref.set(f"{{{NS_XLINK}}}type", "simple")
  schema_ref.set(f"{{{NS_XLINK}}}href", "report.xsd")

  contexts, ctx_for_period = _derive_contexts(bundle)
  for ctx in contexts:
    _append_context(root, ctx)
  for unit in bundle.units:
    _append_unit(root, unit)
  # One value appears in several FactSets (NetIncomeLoss in IS, CF and SE),
  # which validators flag as duplicate facts. Dedupe on the full tuple
  # including value, so a real inconsistency still reaches the validator.
  seen: set[tuple[str, str, str, str, str]] = set()
  for fact in bundle.facts:
    context_ref = ctx_for_period[fact.period_ref]
    key = (
      fact.element_qname,
      context_ref,
      fact.unit_ref or "",
      _format_value(fact.value),
      fact.decimals,
    )
    if key in seen:
      continue
    seen.add(key)
    _append_fact(root, fact, context_ref)

  return root


def _derive_contexts(
  bundle: StatementBundle,
) -> tuple[list[BundleContext], dict[str, str]]:
  """One context per period node (single entity per bundle).

  Returns ``(contexts, period_ref -> context_id)``.
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
  prefixes: set[str] = set()
  for concept in bundle.schema_concepts:
    if ":" in concept.qname:
      prefixes.add(concept.qname.split(":", 1)[0])
  for fact in bundle.facts:
    if ":" in fact.element_qname:
      prefixes.add(fact.element_qname.split(":", 1)[0])
  return prefixes


def _xsi_schema_location() -> dict[str, str]:
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
    unitRef=fact.unit_ref or "",
    decimals=fact.decimals,
  )
  fact_el.text = _format_value(fact.value)


def _format_value(value: float | None) -> str:
  """Integer values render without a decimal point; others at full precision."""
  if value is None:
    # Unreachable: serialize_to_xbrl_21 filters Nonnumeric facts.
    raise ValueError("Nonnumeric fact reached the XBRL numeric emitter")
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)


# ── report.xsd ───────────────────────────────────────────────────────────


def _build_schema(bundle: StatementBundle) -> etree._Element:
  """Build ``report.xsd``: every bundle concept under the rs-gaap namespace."""
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

  etree.SubElement(
    root,
    f"{{{NS_XS}}}import",
    attrib={
      "namespace": NS_XBRLI,
      "schemaLocation": ("http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd"),
    },
  )

  # XBRL 2.1 §5.1.2: roleType and linkbaseRef must sit inside
  # xs:annotation/xs:appinfo, not under the schema root.
  appinfo = _ensure_appinfo(root)
  _append_role_type_declarations(appinfo, bundle)

  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    _append_concept_declaration(root, concept)

  if bundle.linkbases.presentation_links:
    _append_linkbase_ref(appinfo, "report-pre.xml", "presentationLinkbaseRef")
  if bundle.linkbases.calculation_links:
    _append_linkbase_ref(appinfo, "report-cal.xml", "calculationLinkbaseRef")
  if bundle.linkbases.definition_links:
    _append_linkbase_ref(appinfo, "report-def.xml", "definitionLinkbaseRef")
  if _has_labels(bundle):
    _append_linkbase_ref(appinfo, "report-lab.xml", "labelLinkbaseRef")

  return root


def _ensure_appinfo(schema_root: etree._Element) -> etree._Element:
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
  """Declare one ``<link:roleType>`` per ELR used, with its ``usedOn`` link types.

  XBRL 2.1 requires every ELR used in a linkbase to be declared this way.
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
  """``{role_uri: {link_type, …}}`` for every role on a link with arcs."""
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
  """Stable ``xs:ID`` for a role URI; roleRef hrefs and roleType ids must agree."""
  last = role_uri.rsplit("/", 1)[-1]
  sanitized = "".join(c if c.isalnum() or c in "-_." else "_" for c in last)
  return f"role_{sanitized}"


def _append_concept_declaration(parent: etree._Element, concept: BundleElement) -> None:
  """One ``<xs:element>`` per concept.

  ``id`` and ``name`` must be NCNames, so both derive from the qname rather
  than the ULID id or the human-readable ``name``.
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
  # XBRL 2.1 §5.1.1.2: balance is valid only on non-abstract monetary items.
  if concept.balance_type and not concept.is_abstract and concept.is_monetary:
    attrs[f"{{{NS_XBRLI}}}balance"] = concept.balance_type
  etree.SubElement(parent, f"{{{NS_XS}}}element", attrib=attrs)


def _append_linkbase_ref(parent: etree._Element, href: str, role_local: str) -> None:
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
  """Schema id for a concept; must match the linkbase locator fragments."""
  return _qname_to_id(concept.qname)


def _local_name(qname: str) -> str:
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


_ROLE_LINK = "http://www.xbrl.org/2003/role/link"
_ROLE_LABEL = "http://www.xbrl.org/2003/role/label"
_ARCROLE_CONCEPT_LABEL = "http://www.xbrl.org/2003/arcrole/concept-label"


_concept_label = concept_label


def _has_labels(bundle: StatementBundle) -> bool:
  return any(_concept_label(c) is not None for c in bundle.schema_concepts)


def _build_label_linkbase(bundle: StatementBundle) -> etree._Element:
  """Build ``report-lab.xml``: one loc + standard label + arc per labelled concept."""
  nsmap: dict[str | None, str] = {"link": NS_LINK, "xlink": NS_XLINK, "xsi": NS_XSI}
  root = etree.Element(
    f"{{{NS_LINK}}}linkbase",
    nsmap=nsmap,
    attrib={
      f"{{{NS_XSI}}}schemaLocation": (
        f"{NS_LINK} http://www.xbrl.org/2003/xbrl-linkbase-2003-12-31.xsd"
      ),
    },
  )
  label_link = etree.SubElement(
    root,
    f"{{{NS_LINK}}}labelLink",
    attrib={f"{{{NS_XLINK}}}type": "extended", f"{{{NS_XLINK}}}role": _ROLE_LINK},
  )
  for concept in sorted(bundle.schema_concepts, key=lambda c: c.qname):
    label_text = _concept_label(concept)
    if label_text is None:
      continue
    cid = _qname_to_id(concept.qname)
    loc_label = _qname_to_label(concept.qname)
    res_label = f"{loc_label}_lbl"
    etree.SubElement(
      label_link,
      f"{{{NS_LINK}}}loc",
      attrib={
        f"{{{NS_XLINK}}}type": "locator",
        f"{{{NS_XLINK}}}href": f"report.xsd#{cid}",
        f"{{{NS_XLINK}}}label": loc_label,
      },
    )
    label_el = etree.SubElement(
      label_link,
      f"{{{NS_LINK}}}label",
      attrib={
        f"{{{NS_XLINK}}}type": "resource",
        f"{{{NS_XLINK}}}label": res_label,
        f"{{{NS_XLINK}}}role": _ROLE_LABEL,
        f"{{{NS_XML}}}lang": "en",
      },
    )
    label_el.text = label_text
    etree.SubElement(
      label_link,
      f"{{{NS_LINK}}}labelArc",
      attrib={
        f"{{{NS_XLINK}}}type": "arc",
        f"{{{NS_XLINK}}}arcrole": _ARCROLE_CONCEPT_LABEL,
        f"{{{NS_XLINK}}}from": loc_label,
        f"{{{NS_XLINK}}}to": res_label,
      },
    )
  return root


def _build_linkbase(
  links: list[BundleLinkbaseLink],
  link_local: str,
  arc_local: str,
  include_weight: bool,
) -> etree._Element:
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
  link_attrs: dict[str, str] = {
    f"{{{NS_XLINK}}}type": "extended",
  }
  if link.role_uri:
    link_attrs[f"{{{NS_XLINK}}}role"] = link.role_uri
  link_el = etree.SubElement(parent, f"{{{NS_LINK}}}{link_local}", attrib=link_attrs)

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
  """``prefix:Local`` → ``prefix_Local``; schema ids can't contain ``:``."""
  return qname.replace(":", "_")


def _qname_to_label(qname: str) -> str:
  return _qname_to_id(qname)


def _role_id(link: BundleLinkbaseLink) -> str:
  """roleRef fragment; keyed on role URI (not structure id) to match roleType,
  since one ELR can be shared by several Networks."""
  return (
    _role_uri_to_id(link.role_uri) if link.role_uri else f"role_{link.structure_id}"
  )


def _format_decimal(value: float) -> str:
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)
