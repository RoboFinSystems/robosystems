"""Preferred labels in the export: the cash flow's subtracted rows and the
equity roll-forward's opening and closing balances.

The engine's networks carry neither. ``_apply_preferred_labels`` adds them at
export, and every flavor reads them through the bundle: the holon (which the
report components render by), the Tavi and XBRL 2.1.
"""

from __future__ import annotations

import io
import subprocess
import sys
import zipfile

import pytest
from lxml import etree
from xbrlkit.deserialize.holon import from_holon_json

from robosystems.operations.serialization.bundle import (
  NEGATED_LABEL_ROLE,
  PERIOD_END_LABEL_ROLE,
  PERIOD_START_LABEL_ROLE,
  BundleArc,
  BundleElement,
  BundleLinkbaseLink,
  BundleLinkbases,
  _apply_preferred_labels,
)
from robosystems.operations.serialization.rdf.holon import serialize_to_holon_jsonld
from robosystems.operations.serialization.xbrl.xbrl_21 import serialize_to_xbrl_21
from tests.operations.serialization.test_model_bridge import _bundle, _element

pytestmark = pytest.mark.unit

PARENT_CHILD = "http://www.xbrl.org/2003/arcrole/parent-child"
SUMMATION = "http://www.xbrl.org/2003/arcrole/summation-item"
NS_LINK = "http://www.xbrl.org/2003/linkbase"
NS_XLINK = "http://www.w3.org/1999/xlink"

OPERATING = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
NET_INCOME = "rs-gaap:NetIncomeLoss"
GAIN = "rs-gaap:GainLossOnDispositionOfAssets"
EQUITY = "rs-gaap:StockholdersEquity"
ISSUED = "rs-gaap:StockIssuedDuringPeriodValueNewIssues"
DIVIDENDS = "rs-gaap:Dividends"
HEADER = "rs-gaap:StatementOfStockholdersEquityAbstract"


def _arc(kind: str, parent: str, child: str, order: float, weight=None) -> BundleArc:
  return BundleArc(
    arc_type="presentationArc" if kind == "presentation" else "calculationArc",
    arcrole=PARENT_CHILD if kind == "presentation" else SUMMATION,
    from_qname=parent,
    to_qname=child,
    order_value=order,
    weight=weight,
  )


def _link(kind: str, block_type: str, arcs: list[BundleArc]) -> BundleLinkbaseLink:
  return BundleLinkbaseLink(
    link_type="presentationLink" if kind == "presentation" else "calculationLink",
    role_uri=f"http://robosystems.ai/role/{block_type}",
    structure_id=f"struct_{block_type}",
    structure_name=block_type,
    block_type=block_type,
    arcs=arcs,
  )


def _concepts() -> list[BundleElement]:
  return [
    _element(OPERATING, period_type="duration", label="Operating cash"),
    _element(NET_INCOME, period_type="duration", label="Net income"),
    _element(GAIN, period_type="duration", balance_type="credit", label="Gain"),
    _element(EQUITY, balance_type="credit", label="Stockholders' equity"),
    _element(ISSUED, period_type="duration", label="Stock issued"),
    _element(DIVIDENDS, period_type="duration", label="Dividends"),
  ]


def _linkbases() -> BundleLinkbases:
  return BundleLinkbases(
    presentation_links=[
      _link(
        "presentation",
        "cash_flow_statement",
        [
          _arc("presentation", OPERATING, NET_INCOME, 1.0),
          _arc("presentation", OPERATING, GAIN, 2.0),
        ],
      ),
      _link(
        "presentation",
        "equity_statement",
        [
          _arc("presentation", EQUITY, ISSUED, 10.0),
          _arc("presentation", EQUITY, DIVIDENDS, 20.0),
        ],
      ),
    ],
    calculation_links=[
      _link(
        "calculation",
        "cash_flow_statement",
        [
          _arc("calculation", OPERATING, NET_INCOME, 1.0, weight=1.0),
          _arc("calculation", OPERATING, GAIN, 2.0, weight=-1.0),
        ],
      )
    ],
  )


def _labelled() -> tuple[BundleLinkbases, list[BundleElement]]:
  linkbases, concepts = _linkbases(), _concepts()
  concepts.extend(_apply_preferred_labels(linkbases, concepts))
  return linkbases, concepts


def test_a_subtracted_cash_flow_row_is_negated() -> None:
  linkbases, concepts = _labelled()
  cash_flow = linkbases.presentation_links[0]
  labels = {arc.to_qname: arc.preferred_label for arc in cash_flow.arcs}
  assert labels == {NET_INCOME: None, GAIN: NEGATED_LABEL_ROLE}
  gain = next(c for c in concepts if c.qname == GAIN)
  assert gain.role_labels == {NEGATED_LABEL_ROLE: "Gain"}


def test_the_roll_forward_opens_and_closes_on_its_capital_concept() -> None:
  linkbases, concepts = _labelled()
  equity = linkbases.presentation_links[1]
  rows = [
    (arc.from_qname, arc.to_qname, arc.preferred_label)
    for arc in sorted(equity.arcs, key=lambda a: a.order_value or 0.0)
  ]
  assert rows == [
    (HEADER, EQUITY, PERIOD_START_LABEL_ROLE),
    (HEADER, ISSUED, None),
    (HEADER, DIVIDENDS, None),
    (HEADER, EQUITY, PERIOD_END_LABEL_ROLE),
  ]
  capital = next(c for c in concepts if c.qname == EQUITY)
  assert capital.role_labels == {
    PERIOD_START_LABEL_ROLE: "Stockholders' equity, beginning balance",
    PERIOD_END_LABEL_ROLE: "Stockholders' equity, ending balance",
  }
  header = next(c for c in concepts if c.qname == HEADER)
  assert header.is_abstract
  assert header.namespace_uri == capital.namespace_uri


def test_a_network_with_no_capital_root_is_left_alone() -> None:
  linkbases = BundleLinkbases(
    presentation_links=[
      _link(
        "presentation",
        "equity_statement",
        [_arc("presentation", ISSUED, DIVIDENDS, 1.0)],
      )
    ]
  )
  assert _apply_preferred_labels(linkbases, _concepts()) == []
  assert linkbases.presentation_links[0].arcs[0].from_qname == ISSUED


def _labelled_bundle():
  linkbases, concepts = _labelled()
  return _bundle(
    schema_concepts=concepts,
    linkbases=linkbases,
    facts=[],
    structure_display_order={},
  )


def test_xbrl_carries_the_preferred_label_and_a_label_in_its_role() -> None:
  package = zipfile.ZipFile(io.BytesIO(serialize_to_xbrl_21(_labelled_bundle())))
  names = package.namelist()
  pre = etree.fromstring(package.read(next(n for n in names if n.endswith("-pre.xml"))))
  lab = etree.fromstring(package.read(next(n for n in names if n.endswith("-lab.xml"))))
  concept_of = {
    loc.get(f"{{{NS_XLINK}}}label"): loc.get(f"{{{NS_XLINK}}}href").split("#")[1]
    for loc in pre.iter(f"{{{NS_LINK}}}loc")
  }
  preferred = {
    (concept_of[arc.get(f"{{{NS_XLINK}}}to")], arc.get("preferredLabel"))
    for arc in pre.iter(f"{{{NS_LINK}}}presentationArc")
    if arc.get("preferredLabel")
  }
  assert preferred == {
    ("rs-gaap_GainLossOnDispositionOfAssets", NEGATED_LABEL_ROLE),
    ("rs-gaap_StockholdersEquity", PERIOD_START_LABEL_ROLE),
    ("rs-gaap_StockholdersEquity", PERIOD_END_LABEL_ROLE),
  }
  roles = {
    (label.get(f"{{{NS_XLINK}}}label"), label.get(f"{{{NS_XLINK}}}role"))
    for label in lab.iter(f"{{{NS_LINK}}}label")
  }
  for concept, role in (
    ("rs-gaap_GainLossOnDispositionOfAssets", NEGATED_LABEL_ROLE),
    ("rs-gaap_StockholdersEquity", PERIOD_START_LABEL_ROLE),
    ("rs-gaap_StockholdersEquity", PERIOD_END_LABEL_ROLE),
  ):
    assert (f"{concept}_lbl", role) in roles


def test_the_labelled_package_validates_under_arelle(tmp_path) -> None:
  """The roll-forward presents its capital concept twice under one header,
  and the negated role comes from the Link Role Registry: both must still
  make a valid XBRL 2.1 package."""
  zipfile.ZipFile(io.BytesIO(serialize_to_xbrl_21(_labelled_bundle()))).extractall(
    tmp_path
  )
  result = subprocess.run(
    [
      sys.executable,
      "-m",
      "arelle.CntlrCmdLine",
      "--file",
      str(tmp_path / "instance.xml"),
      "--validate",
      "--internetConnectivity=offline",
    ],
    capture_output=True,
    text=True,
    timeout=120,
    check=False,
  )
  output = result.stdout + result.stderr
  assert "validated in" in output
  problems = [
    line
    for line in output.splitlines()
    if line.startswith("[") and not line.startswith("[info]")
  ]
  assert problems == []


def test_the_holon_round_trips_the_preferred_labels() -> None:
  model = from_holon_json(serialize_to_holon_jsonld(_labelled_bundle()))
  preferred = {
    (arc.from_qname, arc.to_qname, arc.preferred_label)
    for network in model.networks
    if network.kind == "presentation"
    for arc in network.arcs
    if arc.preferred_label
  }
  assert preferred == {
    (OPERATING, GAIN, NEGATED_LABEL_ROLE),
    (HEADER, EQUITY, PERIOD_START_LABEL_ROLE),
    (HEADER, EQUITY, PERIOD_END_LABEL_ROLE),
  }


def test_unordered_movements_still_sit_between_the_balances() -> None:
  linkbases = BundleLinkbases(
    presentation_links=[
      _link(
        "presentation",
        "equity_statement",
        [
          BundleArc(
            arc_type="presentationArc",
            arcrole=PARENT_CHILD,
            from_qname=EQUITY,
            to_qname=child,
          )
          for child in (ISSUED, DIVIDENDS)
        ],
      )
    ]
  )
  _apply_preferred_labels(linkbases, _concepts())
  ordered = sorted(linkbases.presentation_links[0].arcs, key=lambda a: a.order_value)
  assert [(a.to_qname, a.preferred_label) for a in ordered] == [
    (EQUITY, PERIOD_START_LABEL_ROLE),
    (ISSUED, None),
    (DIVIDENDS, None),
    (EQUITY, PERIOD_END_LABEL_ROLE),
  ]


def test_a_row_subtracted_by_a_dropped_summation_is_negated() -> None:
  """No calculation link ships for the indirect cash flow; the subtracted
  pairs still give the gain its negated label."""
  linkbases, concepts = _linkbases(), _concepts()
  linkbases.calculation_links = []
  _apply_preferred_labels(linkbases, concepts, {(OPERATING, GAIN)})
  labels = {
    arc.to_qname: arc.preferred_label for arc in linkbases.presentation_links[0].arcs
  }
  assert labels == {NET_INCOME: None, GAIN: NEGATED_LABEL_ROLE}


NCI = "rs-gaap:MinorityInterest"
NCI_IN = "rs-gaap:NoncontrollingInterestIncreaseFromSubsidiaryEquityIssuance"


def test_the_outside_owners_interest_opens_and_closes_beside_the_capital() -> None:
  linkbases = BundleLinkbases(
    presentation_links=[
      _link(
        "presentation",
        "equity_statement",
        [
          _arc("presentation", EQUITY, ISSUED, 10.0),
          _arc("presentation", NCI, NCI_IN, 30.0),
        ],
      )
    ]
  )
  concepts = [
    *_concepts(),
    _element(NCI, balance_type="credit", label="Noncontrolling interest"),
    _element(NCI_IN, period_type="duration", label="Interest issued"),
  ]
  _apply_preferred_labels(linkbases, concepts)
  rows = [
    (arc.from_qname, arc.to_qname, arc.preferred_label)
    for arc in sorted(
      linkbases.presentation_links[0].arcs, key=lambda a: a.order_value or 0.0
    )
  ]
  assert rows == [
    (HEADER, EQUITY, PERIOD_START_LABEL_ROLE),
    (HEADER, ISSUED, None),
    (HEADER, EQUITY, PERIOD_END_LABEL_ROLE),
    (HEADER, NCI, PERIOD_START_LABEL_ROLE),
    (HEADER, NCI_IN, None),
    (HEADER, NCI, PERIOD_END_LABEL_ROLE),
  ]
  nci = next(c for c in concepts if c.qname == NCI)
  assert nci.role_labels[PERIOD_END_LABEL_ROLE] == (
    "Noncontrolling interest, ending balance"
  )


CASH_TOTAL = "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
NET_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
CASH_HEADER = "rs-gaap:StatementOfCashFlowsAbstract"


def test_the_cash_flow_opens_and_closes_on_its_cash_total() -> None:
  """The engine holds the cash total as the parent of net change; the export
  presents it under the statement's header, first at the beginning of the
  period and again at the end, with the sections untouched between them."""
  linkbases = BundleLinkbases(
    presentation_links=[
      _link(
        "presentation",
        "cash_flow_statement",
        [
          _arc("presentation", CASH_TOTAL, NET_CHANGE, 30050.0),
          _arc("presentation", NET_CHANGE, OPERATING, 30100.0),
          _arc("presentation", OPERATING, NET_INCOME, 30110.0),
          _arc("presentation", OPERATING, GAIN, 30126.0),
        ],
      )
    ],
    calculation_links=[
      _link(
        "calculation",
        "cash_flow_statement",
        [
          _arc("calculation", NET_CHANGE, OPERATING, 1.0, weight=1.0),
          _arc("calculation", OPERATING, NET_INCOME, 1.0, weight=1.0),
          _arc("calculation", OPERATING, GAIN, 2.0, weight=-1.0),
        ],
      )
    ],
  )
  concepts = [
    *_concepts(),
    _element(CASH_TOTAL, label="Cash, cash equivalents and restricted cash"),
    _element(NET_CHANGE, period_type="duration", label="Net change in cash"),
  ]
  concepts.extend(_apply_preferred_labels(linkbases, concepts))

  cash_flow = linkbases.presentation_links[0]
  rows = [
    (arc.from_qname, arc.to_qname, arc.preferred_label)
    for arc in sorted(cash_flow.arcs, key=lambda a: a.order_value or 0.0)
  ]
  assert rows == [
    (CASH_HEADER, CASH_TOTAL, PERIOD_START_LABEL_ROLE),
    (CASH_HEADER, NET_CHANGE, None),
    (NET_CHANGE, OPERATING, None),
    (OPERATING, NET_INCOME, None),
    (OPERATING, GAIN, NEGATED_LABEL_ROLE),
    (CASH_HEADER, CASH_TOTAL, PERIOD_END_LABEL_ROLE),
  ]
  total = next(c for c in concepts if c.qname == CASH_TOTAL)
  assert total.role_labels == {
    PERIOD_START_LABEL_ROLE: "Cash, cash equivalents and restricted cash, beginning of period",
    PERIOD_END_LABEL_ROLE: "Cash, cash equivalents and restricted cash, end of period",
  }
  header = next(c for c in concepts if c.qname == CASH_HEADER)
  assert header.is_abstract
  # Cash rolls forward alone: no outside owners' interest beside it.
  assert not any(c.qname == "rs-gaap:MinorityInterest" for c in concepts)
