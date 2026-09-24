"""``StatementBundle`` → xbrlkit ``XbrlModel``, so a RoboLedger report and an
SEC filing go through the same xbrlkit emitters.

What the model cannot carry is named by the flavor that omits it
(``xbrl/tavi.py``), never dropped silently.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Literal

from markdown_it import MarkdownIt
from xbrlkit.model import (
  Arc,
  Concept,
  EntityIdentity,
  FilingMeta,
  Label,
  Network,
  Period,
  Unit,
  XbrlFact,
  XbrlModel,
)
from xbrlkit.namespaces import ENTITY_SCHEME

from robosystems.operations.serialization.bundle import (
  BundleElement,
  BundleFact,
  BundleLinkbaseLink,
  BundlePeriod,
  StatementBundle,
  concept_label,
  namespace_uri_for,
)

STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
REPORT_URI_BASE = "https://robosystems.ai/report"
# For structures with no role_uri; the structure id is stable across generations.
MINTED_ROLE_BASE = "https://robosystems.ai/role"
MARKDOWN_MEDIA_TYPE = "text/markdown"
TEXT_LANGUAGE = "en"

# In render order. Keep in sync with ``BLOCK_TITLES`` in report-components and
# ``_STATEMENT_BLOCK_TYPES`` in ``bundle.py``.
STATEMENT_TITLES: dict[str, str] = {
  "balance_sheet": "Balance Sheet",
  "income_statement": "Income Statement",
  "cash_flow_statement": "Cash Flow Statement",
  "equity_statement": "Statement of Changes in Equity",
}

# Wire item type → XBRL item-type local name. ``days`` maps to decimal, not
# Tavi's ``xbrlr:duration`` (an ``xs:duration`` lexical, not a day count).
ITEM_TYPE_TO_XBRL: dict[str, str] = {
  "monetary": "monetaryItemType",
  "shares": "sharesItemType",
  "percent": "percentItemType",
  "textBlock": "textBlockItemType",
  "string": "stringItemType",
  "date": "dateItemType",
  "boolean": "booleanItemType",
  "decimal": "decimalItemType",
  "integer": "integerItemType",
  "ratio": "pureItemType",
  "multiple": "pureItemType",
  "days": "decimalItemType",
}
_NON_NUMERIC_ITEM_TYPES = frozenset(
  {"stringItemType", "textBlockItemType", "dateItemType", "booleanItemType"}
)
# Concepts whose facts carry a language dimension (Tavi section 8.3).
_TEXT_ITEM_TYPES = frozenset({"stringItemType", "textBlockItemType"})

# Tenant notes are markdown but XBRL text blocks are rendered as HTML. Raw HTML
# is escaped: the narrative is untrusted input bound for other people's browsers.
_MARKDOWN = MarkdownIt("commonmark", {"html": False})


def bundle_to_xbrl_model(bundle: StatementBundle) -> XbrlModel:
  """Re-express ``bundle`` as the model xbrlkit's projections read."""
  concepts = {element.qname: _concept(element) for element in bundle.schema_concepts}
  return XbrlModel(
    filing=_filing(bundle, concepts),
    entity=_entity(bundle),
    concepts=concepts,
    periods=[_period(node) for node in bundle.period_nodes],
    units=[Unit(id=unit.id, measure=unit.measure) for unit in bundle.units],
    facts=[_fact(fact, concepts) for fact in bundle.facts],
    networks=_networks(bundle),
  )


def report_identifier(bundle: StatementBundle) -> str:
  """The id the model is scoped on: the report id, or the snapshot for a live bundle."""
  if bundle.report_meta is not None:
    return bundle.report_meta.report_id
  if bundle.live_meta is not None:
    return "live-" + bundle.live_meta.snapshot_at.strftime("%Y%m%dT%H%M%SZ")
  return "report"


def xbrl_item_type(element: BundleElement) -> str:
  """The XBRL item type for an element — the XBRL 2.1 rule when untyped."""
  mapped = ITEM_TYPE_TO_XBRL.get(element.item_type or "")
  if mapped:
    return mapped
  return "monetaryItemType" if element.is_monetary else "stringItemType"


def lexical_value(value: float) -> str:
  """A numeric value the way XBRL writes it: whole amounts without ``.0``."""
  decimal_value = Decimal(str(value))
  if decimal_value == decimal_value.to_integral_value():
    return str(int(decimal_value))
  return str(decimal_value)


def markdown_to_html(text: str) -> str:
  """A markdown note as an HTML fragment, raw HTML escaped."""
  return _MARKDOWN.render(text).strip()


def network_role(link: BundleLinkbaseLink) -> str:
  """The link's role, minted from the structure id when unset (so a structure's
  presentation and calculation links still share one)."""
  return link.role_uri or f"{MINTED_ROLE_BASE}/{link.structure_id}"


def network_definition(
  link: BundleLinkbaseLink, display_order: dict[str, int]
) -> tuple[str, str | None]:
  """The role's definition and, when it displaces the structure's name, that name.

  Statements and disclosures use the SEC role-definition shape (``0001 -
  Statement - Title``), which consumers parse for title, kind and order. Any
  other structure keeps its own name verbatim.
  """
  name = link.structure_name or link.structure_id
  block_type = link.block_type or ""
  if block_type in STATEMENT_TITLES:
    code = list(STATEMENT_TITLES).index(block_type) + 1
    title = STATEMENT_TITLES[block_type]
    return f"{code:04d} - Statement - {title}", (name if name != title else None)
  order = display_order.get(link.structure_id)
  if order is not None:
    return f"{order:04d} - Disclosure - {name}", None
  return name, None


def _filing(bundle: StatementBundle, concepts: dict[str, Concept]) -> FilingMeta:
  """The filing header, with xbrlkit's SEC-shaped fields repurposed.

  ``accession`` carries the report id and ``cik`` the entity id; the entity
  scheme is what marks them as not SEC identifiers. ``reporting_style`` must
  be set: xbrlkit defaults it to ``"sec-as-filed"``, which is false here.
  """
  meta = bundle.report_meta
  report_id = report_identifier(bundle)
  return FilingMeta(
    accession=report_id,
    cik=bundle.entity.id,
    filing_date=meta.filed_at.date() if meta and meta.filed_at else None,
    is_inline_xbrl=False,
    report_uri=f"{REPORT_URI_BASE}/{report_id}",
    reporting_style=bundle.reporting_style,
    taxonomy_namespaces=sorted({concept.namespace for concept in concepts.values()}),
  )


def _entity(bundle: StatementBundle) -> EntityIdentity:
  entity = bundle.entity
  return EntityIdentity(
    cik=entity.id,
    scheme=ENTITY_SCHEME,
    name=entity.name,
    legal_name=entity.legal_name,
    ein=entity.ein,
  )


def _concept(element: BundleElement) -> Concept:
  item_type = xbrl_item_type(element)
  local_name = element.qname.rsplit(":", 1)[-1]
  prefix = element.namespace or (
    element.qname.split(":", 1)[0] if ":" in element.qname else None
  )
  label = concept_label(element)
  labels = (
    [Label(value=label, role=STANDARD_LABEL_ROLE, language=TEXT_LANGUAGE)]
    if label
    else []
  )
  return Concept(
    qname=element.qname,
    namespace=element.namespace_uri or namespace_uri_for(prefix),
    name=local_name,
    period_type=element.period_type,
    balance=element.balance_type,
    is_abstract=element.is_abstract,
    is_numeric=not element.is_abstract and item_type not in _NON_NUMERIC_ITEM_TYPES,
    is_textblock=item_type == "textBlockItemType",
    is_shares=item_type == "sharesItemType",
    is_integer=item_type == "integerItemType",
    is_text_fact=item_type in _TEXT_ITEM_TYPES,
    item_type=item_type,
    # The bundle carries no nillable flag; an empty text block is a nil.
    nillable=True,
    pref_label=label,
    labels=labels,
  )


def _period(node: BundlePeriod) -> Period:
  return Period(
    id=node.id,
    period_type=node.period_type,
    start=node.period_start if node.period_type == "duration" else None,
    end=node.period_end,
  )


def _fact(fact: BundleFact, concepts: dict[str, Concept]) -> XbrlFact:
  concept = concepts.get(fact.element_qname)
  if fact.fact_type == "Nonnumeric" or fact.value is None:
    text = fact.text_value
    if text is not None and fact.content_type == MARKDOWN_MEDIA_TYPE:
      text = markdown_to_html(text)
    is_text = concept is None or concept.is_text_fact
    return XbrlFact(
      id=fact.id,
      concept_qname=fact.element_qname,
      period_id=fact.period_ref,
      unit_id=None,
      entity_cik=fact.entity_ref,
      value_str=text,
      value_kind="text",
      is_nil=text is None,
      language=TEXT_LANGUAGE if is_text else None,
      structure_id=fact.structure_id,
    )
  return XbrlFact(
    id=fact.id,
    concept_qname=fact.element_qname,
    period_id=fact.period_ref,
    unit_id=fact.unit_ref,
    entity_cik=fact.entity_ref,
    value_str=lexical_value(fact.value),
    numeric_value=fact.value,
    decimals=None if fact.decimals.upper() == "INF" else fact.decimals,
    value_kind="numeric",
    structure_id=fact.structure_id,
  )


def _networks(bundle: StatementBundle) -> list[Network]:
  """One network per link, statements first, then notes in order.

  Definition links are not bridged: Tavi's definition input is dimensional and
  the bundle's definition arcs are not.
  """
  order = bundle.structure_display_order
  keyed: list[tuple[str, int, Network]] = []
  links: list[tuple[Literal["presentation", "calculation"], BundleLinkbaseLink]] = [
    *(("presentation", link) for link in bundle.linkbases.presentation_links),
    *(("calculation", link) for link in bundle.linkbases.calculation_links),
  ]
  # Carried on the network so the holon names each structure by its IRI.
  fact_set_by_structure: dict[str, str] = {
    fact.structure_id: fact.fact_set_id
    for fact in bundle.facts
    if fact.structure_id and fact.fact_set_id
  }
  for position, (kind, link) in enumerate(links):
    definition, documentation = network_definition(link, order)
    targets = {arc.to_qname for arc in link.arcs}
    arcs = [
      Arc(
        from_qname=arc.from_qname,
        to_qname=arc.to_qname,
        arcrole=arc.arcrole or None,
        order=arc.order_value,
        weight=arc.weight if kind == "calculation" else None,
        is_root=arc.from_qname not in targets,
      )
      for arc in link.arcs
    ]
    network = Network(
      role_uri=network_role(link),
      definition=definition,
      documentation=documentation,
      kind=kind,
      arcs=arcs,
      block_type=link.block_type,
      structure_id=link.structure_id,
      fact_set_id=fact_set_by_structure.get(link.structure_id),
    )
    keyed.append((definition, position, network))
  # Coded definitions sort first; presentation precedes calculation per role.
  keyed.sort(key=lambda entry: (entry[0], entry[1]))
  return [network for _, _, network in keyed]
