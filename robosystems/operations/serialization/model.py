"""``StatementBundle`` → xbrlkit ``XbrlModel`` — the waist the projections share.

The platform owns the producer (``bundle.py``: rows → ``StatementBundle``) and
the storage; xbrlkit owns the encoders. This module is the seam between them:
one function that re-expresses a bundle as the model every xbrlkit projection
reads, so a RoboLedger report and an SEC filing go through the same emitter
and come out byte-comparable. The Tavi flavor is the first projection routed
through it; the holon, JSON-LD and XBRL 2.1 flavors follow, each behind a
parity gate, as the platform's own encoders retire.

What the model cannot carry — the Information Block envelopes, the reporting
style and framework pins, the definition linkbase, the fact-set partition, the
filing lifecycle — is named by the flavor that omits it (``xbrl/tavi.py``),
never dropped silently.
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
  namespace_uri_for,
)

STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
# The report's own IRI — the root the JSON-LD encoder scopes the report on.
REPORT_URI_BASE = "https://robosystems.ai/report"
# ``role_uri`` is optional on a tenant-authored Structure (it is copied into
# ``metadata_`` only when the request carried one); a network needs a role, so
# one is minted from the structure id, which is stable across generations.
MINTED_ROLE_BASE = "https://robosystems.ai/role"
MARKDOWN_MEDIA_TYPE = "text/markdown"
TEXT_LANGUAGE = "en"

# Section titles for the four statements, in the order the statements render.
# Mirrors ``BLOCK_TITLES`` in report-components (and ``_STATEMENT_BLOCK_TYPES``
# in ``bundle.py`` for the order), so a Tavi section reads exactly as the
# holon renderer titles the same block.
STATEMENT_TITLES: dict[str, str] = {
  "balance_sheet": "Balance Sheet",
  "income_statement": "Income Statement",
  "cash_flow_statement": "Cash Flow Statement",
  "equity_statement": "Statement of Changes in Equity",
}

# Wire item type (camelCase, ``BundleElement.item_type``) → XBRL item-type
# local name, which is what xbrlkit's ``ITEM_TYPE_DATATYPES`` keys on.
# ``ratio`` and ``multiple`` are dimensionless numbers, so ``pureItemType``;
# ``days`` is a count, so ``decimalItemType`` — Tavi's ``xbrlr:duration`` is
# an ``xs:duration`` lexical, not a day count, and the loss is a recorded gap
# rather than a wrong type.
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

# Tenant-authored notes are markdown (``Fact.content_type = text/markdown``);
# an XBRL text block is XHTML by convention, and every Tavi reader renders it
# as HTML. Raw HTML inside the markdown is escaped, not passed through — the
# narrative is untrusted tenant input on its way into other people's browsers.
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
  """The role a link's network lives in — minted from the structure when unset.

  A structure's presentation and calculation links share its role by
  construction (the producer binds calc arcs to the rendered network), so the
  minted role is the same for both and xbrlkit puts them in one group.
  """
  return link.role_uri or f"{MINTED_ROLE_BASE}/{link.structure_id}"


def network_definition(
  link: BundleLinkbaseLink, display_order: dict[str, int]
) -> tuple[str, str | None]:
  """The role's definition and, when it displaces the structure's name, that name.

  Statements and disclosures take the SEC role-definition shape — a sort code,
  a type word, a title — because that is the shape every consumer already
  parses to title, kind and order a section: the four statements in block-type
  order, the notes after them in the bundle's ``structure_display_order``. A
  structure that is neither keeps its own name verbatim.
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
  """The model's filing header, with xbrlkit's SEC-shaped fields repurposed.

  ``accession`` carries the report id and ``cik`` the entity's own id: the
  model names its identity fields after EDGAR's, and the scheme on the entity
  (``_entity``) is what says they are not a CIK. Neutral names are xbrlkit's
  to add (spec §11.3); the wire output already resolves under the platform's
  scheme, so nothing downstream reads them as SEC identifiers.
  """
  meta = bundle.report_meta
  report_id = report_identifier(bundle)
  return FilingMeta(
    accession=report_id,
    cik=bundle.entity.id,
    filing_date=meta.filed_at.date() if meta and meta.filed_at else None,
    is_inline_xbrl=False,
    report_uri=f"{REPORT_URI_BASE}/{report_id}",
    taxonomy_namespaces=sorted({concept.namespace for concept in concepts.values()}),
  )


def _entity(bundle: StatementBundle) -> EntityIdentity:
  entity = bundle.entity
  return EntityIdentity(
    # The entity's ULID in the field the model names ``cik``; ``scheme`` is
    # what the emitters bind it under, so it never becomes ``cik:``.
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
  labels = (
    [Label(value=element.label, role=STANDARD_LABEL_ROLE, language=TEXT_LANGUAGE)]
    if element.label
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
    # Every concept accepts a nil fact — the bundle carries no per-element
    # declaration, and a text block with no narrative yet is a nil.
    nillable=True,
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
  )


def _networks(bundle: StatementBundle) -> list[Network]:
  """One network per linkbase link, statements first, then the notes in order.

  Definition links are not bridged: Tavi's definition input is dimensional
  (hypercubes, axes, members) and the bundle's definition arcs are
  equivalence / general-special / essence-alias / mapping — recorded as
  omitted by the flavor.
  """
  order = bundle.structure_display_order
  keyed: list[tuple[str, int, Network]] = []
  links: list[tuple[Literal["presentation", "calculation"], BundleLinkbaseLink]] = [
    *(("presentation", link) for link in bundle.linkbases.presentation_links),
    *(("calculation", link) for link in bundle.linkbases.calculation_links),
  ]
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
    )
    keyed.append((definition, position, network))
  # The composed definitions sort by their code; a verbatim name sorts after
  # them. Presentation precedes calculation within a role by construction.
  keyed.sort(key=lambda entry: (entry[0], entry[1]))
  return [network for _, _, network in keyed]
