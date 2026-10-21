"""The ``#lineage`` exporter over a ledger supplied as rows: the hash is the
contract, members are complete and descriptions bounded, the vocabulary never
borrows ``rs:Entity`` or ``rs:Element``, and the spliced file reads back
through xbrlkit as the same report."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Sequence
from datetime import UTC, date, datetime

import pytest
from xbrlkit.deserialize.holon import from_holon_report
from xbrlkit.serialize import to_holon

import robosystems.operations.serialization.rdf.lineage as lineage_module
from robosystems.operations.serialization.bundle import BundleFact
from robosystems.operations.serialization.flavors import RdfFlavor
from robosystems.operations.serialization.rdf import serialize_to_rdf
from robosystems.operations.serialization.rdf.holon import serialize_to_holon_jsonld
from robosystems.operations.serialization.rdf.lineage import (
  LINEAGE_CONTEXT,
  AgentRow,
  DischargeRow,
  ElementRow,
  EntityRow,
  EntryRow,
  EventRow,
  Lineage,
  LineageContextClashError,
  LineageTooLargeError,
  LineRow,
  TransactionRow,
  _flow_as_of,
  build_lineage,
  report_span,
  splice_lineage,
)
from tests.operations.serialization.test_model_bridge import _bundle

ROOT = "https://robosystems.ai/report/rpt_test"
FROZEN = datetime(2025, 1, 5, 12, 0, tzinfo=UTC)


def _hash(ids: Iterable[str]) -> str:
  return "sha256:" + hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest()


def _when(day: date) -> datetime:
  return datetime(day.year, day.month, day.day, tzinfo=UTC)


class _Ledger:
  """Rows in memory, read the way the SQL reader reads them: lines by entity,
  account and window; everything else by id."""

  def __init__(self) -> None:
    self._lines: list[LineRow] = []
    self._entries: dict[str, EntryRow] = {}
    self._transactions: dict[str, TransactionRow] = {}
    self._events: dict[str, EventRow] = {}
    self._agents: dict[str, AgentRow] = {}
    self._elements: dict[str, ElementRow] = {}
    self._entities: dict[str, EntityRow] = {}
    self._discharges: list[DischargeRow] = []
    self._dims: dict[str, list[str]] = {}

  def lines(
    self,
    entity_id: str,
    window_start: date | None,
    window_end: date,
    account_ids: Sequence[str],
  ) -> dict[str, list[LineRow]]:
    out: dict[str, list[LineRow]] = {a: [] for a in account_ids}
    for r in self._lines:
      if (
        r.entity_id == entity_id
        and r.account_id in out
        and r.posting_date <= window_end
        and (window_start is None or r.posting_date >= window_start)
      ):
        out[r.account_id].append(r)
    return out

  def _pick(self, held: dict, ids: Iterable[str]) -> dict:
    return {i: held[i] for i in ids if i in held}

  def entries(self, ids):
    return self._pick(self._entries, ids)

  def reversed_by(self, entry_ids):
    wanted = set(entry_ids)
    return {
      e.reversal_of: e.id
      for e in self._entries.values()
      if e.reversal_of in wanted and e.status in ("posted", "reversed")
    }

  def transactions(self, ids):
    return self._pick(self._transactions, ids)

  def events(self, ids):
    return self._pick(self._events, ids)

  def discharges(self, event_ids):
    wanted = set(event_ids)
    return [
      d
      for d in self._discharges
      if d.payment_event_id in wanted or d.obligation_event_id in wanted
    ]

  def agents(self, ids):
    return self._pick(self._agents, ids)

  def elements(self, ids):
    return self._pick(self._elements, ids)

  def entities(self, ids):
    return self._pick(self._entities, ids)

  def line_dimensions(self, line_ids):
    return self._pick(self._dims, line_ids)


def _entry(
  id: str,
  on: date,
  *,
  status: str = "posted",
  entity_id: str = "ent_01",
  event: str | None = None,
  transaction: str | None = None,
  reversal_of: str | None = None,
  adjustment_kind: str | None = None,
  adjusts_entity_id: str | None = None,
  flow_retags=(),
) -> EntryRow:
  return EntryRow(
    id=id,
    entity_id=entity_id,
    number=f"JE-{id[-1]}",
    posting_date=on,
    posted_at=_when(on),
    type="adjusting" if adjustment_kind else "standard",
    status=status,
    provenance="system_computed" if adjustment_kind else "manual_entry",
    memo=f"memo {id}",
    transaction_id=transaction,
    triggered_by_event_id=event,
    reversal_of=reversal_of,
    adjustment_kind=adjustment_kind,
    adjusts_entity_id=adjusts_entity_id,
    flow_retags=flow_retags,
  )


def _event(
  id: str,
  on: date,
  event_type: str,
  *,
  entity_id: str = "ent_01",
  agent: str | None = None,
  discharges: str | None = None,
  obligated_by: str | None = None,
  amount: int | None = None,
) -> EventRow:
  return EventRow(
    id=id,
    entity_id=entity_id,
    event_type=event_type,
    event_category="sales",
    event_class="economic",
    event_action="transfer",
    status="fulfilled",
    occurred_at=_when(on),
    due_date=None,
    amount_cents=amount,
    currency="USD",
    source="manual",
    description=None,
    agent_id=agent,
    obligated_by_event_id=obligated_by,
    discharges_event_id=discharges,
    replaces_event_id=None,
    replaced_by_event_id=None,
  )


def _line(
  id: str,
  entry: str,
  account: str,
  on: date,
  *,
  debit: int = 0,
  credit: int = 0,
  entity_id: str = "ent_01",
  flow: str | None = None,
) -> LineRow:
  return LineRow(
    id=id,
    entry_id=entry,
    account_id=account,
    flow_element_id=flow,
    debit_cents=debit,
    credit_cents=credit,
    description=None,
    line_order=1,
    entity_id=entity_id,
    posting_date=on,
  )


@pytest.fixture
def ledger() -> _Ledger:
  led = _Ledger()
  led._elements = {
    "acct_cash": ElementRow(
      "acct_cash", "Operating Checking", "1000", None, "debit", "instant"
    ),
    "acct_rev": ElementRow("acct_rev", "Sales", "4000", None, "credit", "duration"),
    "acct_ic": ElementRow(
      "acct_ic", "Intercompany Revenue — Eliminated", "4900", None, "credit", "duration"
    ),
    "acct_other": ElementRow(
      "acct_other", "Petty Cash", "1010", None, "debit", "instant"
    ),
    "flow_new": ElementRow(
      "flow_new", "Repayments", None, "rs-gaap:RepaymentsOfDebt", None, "duration"
    ),
  }
  led._entities = {
    "ent_01": EntityRow("ent_01", "Test Co", "operating", None),
    "ent_02": EntityRow(
      "ent_02", "Consolidation Adjustments", "consolidation", "ent_01"
    ),
    "ent_03": EntityRow("ent_03", "Harbor Café", "operating", "ent_01"),
  }
  led._agents = {"agt_1": AgentRow("agt_1", "Driftline Subscribers", "customer")}
  retag_after_freeze = (
    {
      "at": "2025-01-10T00:00:00+00:00",
      "line_ids": ["li_2"],
      "flows": ["rs-gaap:RepaymentsOfDebt"],
      "from": {"li_2": "rs-gaap:PaymentsToSuppliers"},
    },
  )
  led._entries = {
    e.id: e
    for e in (
      _entry("je_1", date(2023, 6, 1), event="evt_1", transaction="txn_1"),
      _entry(
        "je_2",
        date(2024, 3, 1),
        event="evt_2",
        transaction="txn_2",
        flow_retags=retag_after_freeze,
      ),
      _entry("je_3", date(2024, 5, 1), status="reversed", event="evt_3"),
      _entry("je_4", date(2024, 5, 2), event="evt_4", reversal_of="je_3"),
      _entry("je_5", date(2024, 2, 1), event="evt_5", transaction="txn_5"),
      _entry(
        "je_6",
        date(2024, 12, 31),
        entity_id="ent_02",
        adjustment_kind="elimination_intercompany",
        adjusts_entity_id="ent_03",
      ),
      _entry("je_7", date(2024, 4, 1), event="evt_7"),
    )
  }
  led._transactions = {
    t.id: t
    for t in (
      TransactionRow(
        "txn_1",
        "ent_01",
        "deposit",
        None,
        10_000,
        "USD",
        date(2023, 6, 1),
        None,
        None,
        None,
        None,
        "manual",
        "posted",
        "evt_1",
      ),
      TransactionRow(
        "txn_2",
        "ent_01",
        "payment",
        "rent",
        2_500,
        "USD",
        date(2024, 3, 1),
        None,
        "Ballard Industrial",
        "INV-9",
        None,
        "manual",
        "posted",
        "evt_2",
      ),
      TransactionRow(
        "txn_5",
        "ent_01",
        "invoice",
        None,
        4_000,
        "USD",
        date(2024, 2, 1),
        date(2024, 3, 1),
        None,
        None,
        None,
        "manual",
        "posted",
        "evt_5",
      ),
    )
  }
  led._events = {
    ev.id: ev
    for ev in (
      _event("evt_1", date(2023, 6, 1), "payment_received", amount=10_000),
      _event("evt_2", date(2024, 3, 1), "bill_paid", discharges="evt_5", amount=-2_500),
      _event("evt_3", date(2024, 5, 1), "journal_entry_recorded"),
      _event("evt_4", date(2024, 5, 2), "journal_entry_recorded"),
      _event(
        "evt_5",
        date(2024, 2, 1),
        "invoice_issued",
        agent="agt_1",
        obligated_by="evt_sched",
        amount=4_000,
      ),
      _event("evt_sched", date(2024, 1, 15), "schedule_entry_due"),
      _event("evt_7", date(2024, 4, 1), "journal_entry_recorded"),
    )
  }
  led._discharges = [DischargeRow("evt_2", "evt_5", 2_500)]
  led._lines = [
    _line("li_1", "je_1", "acct_cash", date(2023, 6, 1), debit=10_000),
    _line("li_2", "je_2", "acct_cash", date(2024, 3, 1), credit=2_500, flow="flow_new"),
    _line("li_3", "je_3", "acct_cash", date(2024, 5, 1), debit=500),
    _line("li_4", "je_4", "acct_cash", date(2024, 5, 2), credit=500),
    _line("li_5", "je_5", "acct_rev", date(2024, 2, 1), credit=4_000),
    _line("li_6", "je_6", "acct_ic", date(2024, 12, 31), debit=600, entity_id="ent_02"),
    _line("li_7", "je_7", "acct_other", date(2024, 4, 1), debit=100),
  ]
  led._dims = {"li_6": ["department=Roastery"]}
  return led


def _pivot(sources, line_ids, **overrides) -> dict:
  descriptor = {
    "origin": "pivot",
    "mapping_id": "map_1",
    "period": "2024-12-31",
    "sources": sources,
    "content_hash": _hash(line_ids),
    "line_count": len(line_ids),
  }
  descriptor.update(overrides)
  return descriptor


@pytest.fixture
def bundle():
  cash_lines = ["li_1", "li_2", "li_3", "li_4"]
  facts = [
    BundleFact(
      id="f_cash",
      element_id="Assets",
      element_qname="rs-gaap:Assets",
      value=75.0,
      period_ref="p_instant",
      unit_ref="u_USD",
      entity_ref="ent_01",
      provenance=_pivot(
        [{"entity_id": "ent_01", "account_id": "acct_cash", "value": 75.0}],
        cash_lines,
      ),
      frozen_at=FROZEN,
    ),
    BundleFact(
      id="f_bad",
      element_id="AssetsCurrent",
      element_qname="rs-gaap:AssetsCurrent",
      value=1.0,
      period_ref="p_instant",
      unit_ref="u_USD",
      entity_ref="ent_01",
      # Its own account and line, so a refusal can be seen to leave nothing.
      provenance=_pivot(
        [{"entity_id": "ent_01", "account_id": "acct_other", "value": "1.00"}],
        ["li_7"],
        content_hash="sha256:deadbeef",
      ),
      frozen_at=FROZEN,
    ),
    BundleFact(
      id="f_live",
      element_id="SharesOutstanding",
      element_qname="rs-gaap:SharesOutstanding",
      value=75.0,
      period_ref="p_instant",
      unit_ref="u_shares",
      entity_ref="ent_01",
      provenance=_pivot(
        [{"entity_id": "ent_01", "account_id": "acct_cash", "value": 75.0}],
        cash_lines,
      ),
      frozen_at=None,
    ),
    BundleFact(
      id="f_widgets",
      element_id="Widgets",
      element_qname="custom:Widgets",
      value=34.0,
      period_ref="p_duration",
      unit_ref="u_USD",
      entity_ref="ent_01",
      provenance=_pivot(
        [
          {"entity_id": "ent_01", "account_id": "acct_rev", "value": 40.0},
          {"entity_id": "ent_02", "account_id": "acct_ic", "value": "-6.00"},
        ],
        ["li_5", "li_6"],
        period="2024-01-01/2024-12-31",
      ),
      frozen_at=FROZEN,
    ),
    BundleFact(
      id="f_rollup",
      element_id="DaysSalesOutstanding",
      element_qname="rs-gaap:DaysSalesOutstanding",
      value=1.0,
      period_ref="p_duration",
      unit_ref="u_USD",
      entity_ref="ent_01",
      provenance={"origin": "rollup", "rule": "calculation", "children": []},
      frozen_at=FROZEN,
    ),
  ]
  return _bundle(facts=facts)


@pytest.fixture
def lineage(bundle, ledger) -> Lineage:
  return build_lineage(bundle, ledger, root=ROOT, describe_from=date(2024, 1, 1))


def _by_id(lineage: Lineage) -> dict[str, dict]:
  return {n["@id"]: n for n in lineage.nodes}


def _types(node: dict) -> list[str]:
  t = node["@type"]
  return t if isinstance(t, list) else [t]


@pytest.mark.unit
def test_the_report_span_is_its_own_columns(bundle) -> None:
  assert report_span(bundle) == (date(2024, 1, 1), date(2024, 12, 31))


@pytest.mark.unit
class TestTheHashIsTheContract:
  def test_only_facts_whose_reconstruction_reproduces_the_hash_are_bridged(
    self, lineage
  ) -> None:
    assert set(lineage.bridges) == {"f_cash", "f_widgets"}
    assert lineage.refused == ["f_bad"]
    assert lineage.summary["facts"] == 3
    assert lineage.summary["facts_bridged"] == 2

  def test_a_live_column_and_a_rollup_have_no_lineage(self, lineage) -> None:
    nodes = _by_id(lineage)
    assert not any("f_live" in iri or "f_rollup" in iri for iri in nodes)

  def test_a_refused_fact_leaves_nothing_of_its_own_behind(self, lineage) -> None:
    nodes = _by_id(lineage)
    assert not any("f_bad" in iri for iri in nodes)
    for orphan in ("line/li_7", "entry/je_7", "event/evt_7", "account/acct_other"):
      assert f"{ROOT}/{orphan}" not in nodes


@pytest.mark.unit
class TestMembersAreCompleteAndDescriptionsBounded:
  def test_the_contribution_lists_every_member_and_counts_the_described(
    self, lineage
  ) -> None:
    c = _by_id(lineage)[f"{ROOT}/contribution/f_cash/ent_01/acct_cash"]
    assert c["lineCount"] == 4
    assert len(c["hadMember"]) == 4
    assert c["describedLineCount"] == 3
    assert c["describedFrom"] == "2024-01-01"
    assert c["priorLineCount"] == 1
    assert c["priorBalance"] == "100.00"
    assert c["amount"] == "75.00"
    assert c["frozenAt"] == "2025-01-05T12:00:00Z"
    assert c["windowEnd"] == "2024-12-31" and "windowStart" not in c

  def test_a_prior_balance_follows_the_accounts_natural_sign(
    self, bundle, ledger
  ) -> None:
    # Describe nothing: every line of the credit account is prior, and a
    # credit balance reads positive.
    out = build_lineage(bundle, ledger, root=ROOT, describe_from=date(2030, 1, 1))
    c = _by_id(out)[f"{ROOT}/contribution/f_widgets/ent_01/acct_rev"]
    assert c["priorBalance"] == "40.00"
    assert c["describedLineCount"] == 0

  def test_the_hash_recomputes_from_the_member_iris(self, lineage, bundle) -> None:
    c = _by_id(lineage)[f"{ROOT}/contribution/f_cash/ent_01/acct_cash"]
    ids = [iri.rsplit("/", 1)[1] for iri in c["hadMember"]]
    fact = next(f for f in bundle.facts if f.id == "f_cash")
    assert _hash(ids) == fact.provenance["content_hash"]

  def test_only_described_lines_get_nodes_and_their_closure(self, lineage) -> None:
    nodes = _by_id(lineage)
    assert f"{ROOT}/line/li_2" in nodes
    assert f"{ROOT}/line/li_1" not in nodes
    assert f"{ROOT}/entry/je_1" not in nodes
    assert f"{ROOT}/event/evt_1" not in nodes
    assert lineage.summary["lines_described"] == 5

  def test_describing_everything_reaches_the_opening_of_the_books(
    self, bundle, ledger
  ) -> None:
    out = build_lineage(bundle, ledger, root=ROOT, describe_from=None)
    nodes = _by_id(out)
    assert f"{ROOT}/line/li_1" in nodes
    assert f"{ROOT}/event/evt_1" in nodes
    c = nodes[f"{ROOT}/contribution/f_cash/ent_01/acct_cash"]
    assert "priorBalance" not in c and "describedFrom" not in c


@pytest.mark.unit
class TestTheWalk:
  def test_a_line_names_its_entry_account_entity_and_event(self, lineage) -> None:
    line = _by_id(lineage)[f"{ROOT}/line/li_2"]
    assert line["entry"] == f"{ROOT}/entry/je_2"
    assert line["account"] == f"{ROOT}/account/acct_cash"
    assert line["entity"] == f"{ROOT}/entity/ent_01"
    assert line["generatedBy"] == f"{ROOT}/event/evt_2"
    assert line["credit"] == "25.00" and "debit" not in line

  def test_the_flow_tag_is_read_as_of_the_freeze(self, lineage) -> None:
    line = _by_id(lineage)[f"{ROOT}/line/li_2"]
    assert line["flowConcept"] == "rs-gaap:PaymentsToSuppliers"

  def test_a_reversal_pair_names_each_other(self, lineage) -> None:
    nodes = _by_id(lineage)
    assert nodes[f"{ROOT}/entry/je_3"]["reversedBy"] == f"{ROOT}/entry/je_4"
    assert nodes[f"{ROOT}/entry/je_3"]["entryStatus"] == "reversed"
    assert nodes[f"{ROOT}/entry/je_4"]["reversalOf"] == f"{ROOT}/entry/je_3"

  def test_an_entry_reaches_its_transaction_and_event_and_the_event_its_agent(
    self, lineage
  ) -> None:
    nodes = _by_id(lineage)
    entry = nodes[f"{ROOT}/entry/je_5"]
    assert entry["transaction"] == f"{ROOT}/transaction/txn_5"
    assert entry["generatedBy"] == f"{ROOT}/event/evt_5"
    event = nodes[f"{ROOT}/event/evt_5"]
    assert event["associatedWith"] == f"{ROOT}/agent/agt_1"
    assert event["amount"] == "40.00"
    assert nodes[f"{ROOT}/agent/agt_1"]["prefLabel"] == "Driftline Subscribers"
    assert nodes[f"{ROOT}/transaction/txn_5"]["dueDate"] == "2024-03-01"

  def test_one_hop_of_duality_is_described_and_not_walked(self, lineage) -> None:
    nodes = _by_id(lineage)
    assert nodes[f"{ROOT}/event/evt_2"]["discharges"] == f"{ROOT}/event/evt_5"
    assert nodes[f"{ROOT}/event/evt_5"]["obligatedBy"] == f"{ROOT}/event/evt_sched"
    assert f"{ROOT}/event/evt_sched" in nodes
    assert lineage.summary["events_one_hop"] == 1
    discharge = nodes[f"{ROOT}/discharge/evt_2/evt_5"]
    assert discharge["amount"] == "25.00"

  def test_a_consolidating_entry_says_what_it_adjusts_and_has_no_event(
    self, lineage
  ) -> None:
    nodes = _by_id(lineage)
    entry = nodes[f"{ROOT}/entry/je_6"]
    assert entry["adjustmentKind"] == "elimination_intercompany"
    assert entry["adjustsEntity"] == f"{ROOT}/entity/ent_03"
    assert "generatedBy" not in entry
    assert nodes[f"{ROOT}/line/li_6"]["dimensionMember"] == ["department=Roastery"]
    contribution = nodes[f"{ROOT}/contribution/f_widgets/ent_02/acct_ic"]
    assert contribution["amount"] == "-6.00"
    assert contribution["entity"] == f"{ROOT}/entity/ent_02"

  def test_member_entities_and_accounts_never_borrow_the_reports_types(
    self, lineage
  ) -> None:
    nodes = _by_id(lineage)
    types = Counter(t for n in lineage.nodes for t in _types(n))
    assert "rs:Entity" not in types and "rs:Element" not in types
    assert types["rs:LedgerEntity"] == 3 and types["rs:Account"] == 3
    # The report's own entity is labelled in #scene; here it is only placed.
    assert "prefLabel" not in nodes[f"{ROOT}/entity/ent_01"]
    member = nodes[f"{ROOT}/entity/ent_02"]
    assert member["prefLabel"] == "Consolidation Adjustments"
    assert member["entityRole"] == "consolidation"
    assert member["parentEntity"] == f"{ROOT}/entity/ent_01"
    account = nodes[f"{ROOT}/account/acct_cash"]
    assert account["prefLabel"] == "Operating Checking"
    assert account["balance"] == "debit" and account["code"] == "1000"


@pytest.mark.unit
class TestTheSplice:
  def test_the_fourth_graph_and_the_bridge_edges_land_in_the_holon(
    self, bundle, lineage
  ) -> None:
    text = splice_lineage(serialize_to_holon_jsonld(bundle), lineage, root=ROOT)
    document = json.loads(text)
    graphs = {g["@id"].rsplit("#", 1)[1]: g["@graph"] for g in document["@graph"]}
    assert set(graphs) == {"scene", "boundary", "projection", "lineage"}
    facts = {n["internalId"]: n for n in graphs["scene"] if n.get("@type") == "rs:Fact"}
    assert facts["f_cash"]["derivedFrom"] == [
      f"{ROOT}/contribution/f_cash/ent_01/acct_cash"
    ]
    assert "derivedFrom" not in facts["f_bad"]
    assert "derivedFrom" not in facts["f_live"]
    assert document["@context"]["derivedFrom"] == LINEAGE_CONTEXT["derivedFrom"]
    assert len(graphs["lineage"]) == len(lineage.nodes)

  def test_xbrlkit_reads_the_entitled_file_as_the_same_report(
    self, bundle, lineage
  ) -> None:
    plain = serialize_to_holon_jsonld(bundle)
    entitled = splice_lineage(plain, lineage, root=ROOT)
    plain_model, plain_gaps = from_holon_report(plain)
    model, gaps = from_holon_report(entitled)
    assert len(model.facts) == len(plain_model.facts)
    assert set(model.concepts) == set(plain_model.concepts)
    assert model.entity == plain_model.entity
    assert gaps.missing == plain_gaps.missing
    assert to_holon(model, report_id="rpt_test") == to_holon(
      plain_model, report_id="rpt_test"
    )

  def test_a_term_the_holon_already_binds_differently_is_an_error(
    self, bundle, lineage
  ) -> None:
    document = json.loads(serialize_to_holon_jsonld(bundle))
    document["@context"]["derivedFrom"] = {"@id": "rs:somethingElse"}
    with pytest.raises(LineageContextClashError, match="derivedFrom"):
      splice_lineage(json.dumps(document), lineage, root=ROOT)

  def test_the_rdf_dispatch_names_the_session_bound_entry_point(self, bundle) -> None:
    with pytest.raises(ValueError, match="serialize_to_lineage_holon_jsonld"):
      serialize_to_rdf(bundle, RdfFlavor.HOLON_JSONLD_LINEAGE)


@pytest.mark.unit
class TestTheReviewsEdges:
  def test_the_flow_tag_cutoff_is_a_moment_not_a_string(self) -> None:
    """A retag a microsecond after the freeze, written with an offset and
    microseconds, is after it; one at the very moment is not."""
    after = _entry(
      "je_r",
      date(2024, 3, 1),
      flow_retags=(
        {
          "at": "2025-01-05T12:00:00.000001+00:00",
          "from": {"li_r": "rs-gaap:Before"},
        },
      ),
    )
    at_the_moment = _entry(
      "je_s",
      date(2024, 3, 1),
      flow_retags=({"at": "2025-01-05T12:00:00+00:00", "from": {"li_r": "x"}},),
    )
    unparsable = _entry(
      "je_t",
      date(2024, 3, 1),
      flow_retags=({"at": "yesterday", "from": {"li_r": "x"}},),
    )
    assert _flow_as_of(after, "li_r", "rs-gaap:Now", FROZEN) == "rs-gaap:Before"
    assert _flow_as_of(at_the_moment, "li_r", "rs-gaap:Now", FROZEN) == "rs-gaap:Now"
    assert _flow_as_of(unparsable, "li_r", "rs-gaap:Now", FROZEN) == "rs-gaap:Now"

  def test_a_descriptor_amount_may_be_a_string(self, lineage) -> None:
    c = _by_id(lineage)[f"{ROOT}/contribution/f_widgets/ent_02/acct_ic"]
    assert c["amount"] == "-6.00"

  def test_more_lines_than_one_file_carries_is_refused(
    self, bundle, ledger, monkeypatch
  ) -> None:
    monkeypatch.setattr(lineage_module, "LINEAGE_LINE_CEILING", 1)
    with pytest.raises(LineageTooLargeError, match="narrower span"):
      build_lineage(bundle, ledger, root=ROOT, describe_from=None)

  def test_a_holon_that_does_not_bind_a_reused_term_is_refused(
    self, bundle, lineage
  ) -> None:
    document = json.loads(serialize_to_holon_jsonld(bundle))
    del document["@context"]["prefLabel"]
    with pytest.raises(LineageContextClashError, match="prefLabel"):
      splice_lineage(json.dumps(document), lineage, root=ROOT)
