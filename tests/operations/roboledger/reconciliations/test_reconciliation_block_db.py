"""The reconciliation block against real Postgres: what a refresh records,
what a second refresh replaces, and how the block's rule becomes its status.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import select

from robosystems.adapters.quickbooks.reports import TrialBalanceReport
from robosystems.models.api.extensions.reconciliations import (
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
)
from robosystems.models.extensions import (
  Association,
  Element,
  Rule,
  Structure,
  VerificationResult,
)
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger import COA_SOURCES, Fact, FactSet
from robosystems.operations.information_block import get_information_block
from robosystems.operations.roboledger.commands.reconciliations import (
  ReconciliationNotFoundError,
  refresh_reconciliations,
  set_reconciliation_policy,
)
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import SourceLedgerResolver

from .conftest import GRAPH_ID, LIVE_CONNECTION, SYNCED_AT, TIED, source_report

pytestmark = pytest.mark.unit

# QuickBooks no longer has the April software bill: Checking and Software each
# differ by 120.00.
_BILL_REMOVED = (
  ("35", "Checking", 150_000),
  ("50", "Services", -50_000),
  ("3", "Retained Earnings", -100_000),
)


@pytest.fixture()
def ledger(ext_session, books):
  ext_session.add(Entity(name="Fictional Co", created_by="usr"))
  ext_session.commit()
  return ext_session


def _refresh(session, report: TrialBalanceReport, period: str = "2026-08"):
  with patch.object(
    SourceLedgerResolver,
    "_fetch",
    return_value=(report, LIVE_CONNECTION, SYNCED_AT),
  ):
    result = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period=period),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()
  return result


def _blocks(session) -> list[Structure]:
  return list(
    session.execute(
      select(Structure).where(Structure.block_type == "reconciliation")
    ).scalars()
  )


def test_a_tied_ledger_is_recorded_as_reconciled(ledger):
  result = _refresh(ledger, source_report(*TIED))

  (rec,) = result.reconciliations
  assert (rec.scope, rec.method, rec.status) == (
    "ledger",
    "source_ledger",
    "reconciled",
  )
  assert rec.name == "Source ledger (QuickBooks)"
  assert (rec.accounts_compared, rec.accounts_different) == (4, 0)
  assert rec.unreconciled_difference == 0
  assert rec.differences == []
  assert rec.as_of == date(2026, 8, 31)
  assert rec.source == "quickbooks"
  assert rec.required_for_close is True


def test_a_difference_is_recorded_as_unreconciled_with_its_accounts(ledger):
  result = _refresh(ledger, source_report(*_BILL_REMOVED))

  (rec,) = result.reconciliations
  assert rec.status == "unreconciled"
  assert rec.unreconciled_difference == 240.00
  assert (rec.accounts_compared, rec.accounts_different) == (4, 2)
  assert [(d.account_name, d.difference) for d in rec.differences] == [
    ("Checking", -120.00),
    ("Software", 120.00),
  ]


def test_the_block_is_made_of_existing_parts(ledger):
  """A Structure, concept elements, a rule, one FactSet and a result: no
  table of its own."""
  _refresh(ledger, source_report(*TIED))

  (structure,) = _blocks(ledger)
  assert structure.artifact_mechanics == {
    "kind": "reconciliation",
    "scope": "ledger",
    "method": "source_ledger",
    "element_id": None,
    "required_for_close": True,
    "materiality": 0.0,
    "review_required": False,
    "separate_reviewer": False,
  }
  arcs = (
    ledger.execute(select(Association).where(Association.structure_id == structure.id))
    .scalars()
    .all()
  )
  assert len(arcs) == 3

  (fact_set,) = ledger.execute(
    select(FactSet).where(FactSet.structure_id == structure.id)
  ).scalars()
  assert fact_set.period_end == date(2026, 8, 31)
  assert fact_set.provenance["origin"] == "observed"
  assert fact_set.provenance["source"] == "quickbooks"
  assert fact_set.provenance["method"] == "source_ledger"
  assert fact_set.provenance["as_of"] == "2026-08-31"
  assert fact_set.provenance["connection_id"] == LIVE_CONNECTION
  assert fact_set.provenance["basis"] == "Accrual"

  facts = (
    ledger.execute(select(Fact).where(Fact.fact_set_id == fact_set.id)).scalars().all()
  )
  assert sorted((f.unit, f.value) for f in facts) == [
    ("USD", 0.0),
    ("pure", 0.0),
    ("pure", 4.0),
  ]
  assert all(f.period_type == "instant" and f.period_start is None for f in facts)

  (rule,) = ledger.execute(
    select(Rule).where(Rule.target_structure_id == structure.id)
  ).scalars()
  (verdict,) = ledger.execute(
    select(VerificationResult).where(VerificationResult.rule_id == rule.id)
  ).scalars()
  assert (verdict.status, verdict.fact_set_id) == ("pass", fact_set.id)
  assert verdict.period_end == date(2026, 8, 31)


def test_the_concepts_stay_out_of_the_chart_of_accounts(ledger):
  _refresh(ledger, source_report(*TIED))

  concepts = (
    ledger.execute(select(Element).where(Element.qname.like("rs-rec:%")))
    .scalars()
    .all()
  )
  assert len(concepts) == 4
  assert {c.source for c in concepts} == {"system"}
  assert "system" not in COA_SOURCES
  # The graph builds a system element's qname from its code.
  assert all(c.code for c in concepts)


def test_refreshing_again_replaces_the_periods_comparison(ledger):
  _refresh(ledger, source_report(*_BILL_REMOVED))
  result = _refresh(ledger, source_report(*TIED))

  assert [r.status for r in result.reconciliations] == ["reconciled"]
  assert len(_blocks(ledger)) == 1
  assert ledger.query(FactSet).count() == 1
  assert ledger.query(Fact).count() == 3
  assert ledger.query(VerificationResult).count() == 1
  assert ledger.query(Rule).count() == 1
  assert (
    ledger.execute(select(Element).where(Element.qname.like("rs-rec:%")))
    .scalars()
    .all()
    .__len__()
    == 4
  )


def test_each_period_keeps_its_own_comparison(ledger):
  _refresh(ledger, source_report(*TIED), period="2026-08")
  # Through July the 2026 activity is the same, so the same balances tie.
  _refresh(ledger, source_report(*_BILL_REMOVED), period="2026-07")

  assert ledger.query(FactSet).count() == 2
  august = list_reconciliations(ledger, "2026-08").reconciliations[0]
  july = list_reconciliations(ledger, "2026-07").reconciliations[0]
  assert (august.status, july.status) == ("reconciled", "unreconciled")
  assert august.fact_set_id != july.fact_set_id


def test_a_period_never_compared_has_not_started(ledger):
  _refresh(ledger, source_report(*TIED), period="2026-08")

  (rec,) = list_reconciliations(ledger, "2026-06").reconciliations
  assert rec.status == "not_started"
  assert rec.unreconciled_difference is None
  assert rec.fact_set_id is None
  assert rec.compared_at is None


def test_a_ledger_with_no_block_lists_nothing(ledger):
  assert list_reconciliations(ledger, "2026-08").reconciliations == []


def test_materiality_applies_from_the_next_comparison(ledger):
  result = _refresh(ledger, source_report(*_BILL_REMOVED))
  structure_id = result.reconciliations[0].structure_id

  policy = set_reconciliation_policy(
    ledger,
    SetReconciliationPolicyRequest(structure_id=structure_id, materiality=250.00),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  ledger.commit()
  assert (policy.materiality, policy.required_for_close) == (250.00, True)
  # The recorded comparison is not re-judged.
  assert list_reconciliations(ledger, "2026-08").reconciliations[0].status == (
    "unreconciled"
  )

  (rec,) = _refresh(ledger, source_report(*_BILL_REMOVED)).reconciliations
  assert rec.status == "reconciled"
  assert rec.unreconciled_difference == 240.00
  assert rec.materiality == 250.00


def test_a_block_can_be_released_from_the_close(ledger):
  structure_id = _refresh(ledger, source_report(*TIED)).reconciliations[0].structure_id

  policy = set_reconciliation_policy(
    ledger,
    SetReconciliationPolicyRequest(structure_id=structure_id, required_for_close=False),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  ledger.commit()

  assert (policy.required_for_close, policy.materiality) == (False, 0.0)
  (rec,) = list_reconciliations(ledger, "2026-08").reconciliations
  assert rec.required_for_close is False


def test_policy_on_something_that_is_not_a_reconciliation_is_not_found(ledger):
  with pytest.raises(ReconciliationNotFoundError):
    set_reconciliation_policy(
      ledger,
      SetReconciliationPolicyRequest(structure_id="struct_missing", materiality=1),
      graph_id=GRAPH_ID,
      created_by="usr",
    )


def test_the_block_reads_as_an_information_block(ledger):
  _refresh(ledger, source_report(*TIED), period="2026-07")
  structure_id = (
    _refresh(ledger, source_report(*_BILL_REMOVED), period="2026-08")
    .reconciliations[0]
    .structure_id
  )

  envelope = get_information_block(ledger, structure_id)

  assert envelope is not None
  assert (envelope.block_type, envelope.category) == ("reconciliation", "Close")
  assert envelope.artifact.mechanics.kind == "reconciliation"
  assert len(envelope.facts) == 6
  assert len(envelope.rules) == 1
  rendering = envelope.view.rendering
  assert [p.end for p in rendering.periods] == [date(2026, 7, 31), date(2026, 8, 31)]
  by_row = {row.element_qname: row.values for row in rendering.rows}
  assert by_row["rs-rec:UnreconciledDifference"] == [0.0, 240.00]
  assert by_row["rs-rec:AccountsDifferent"] == [0.0, 2.0]


# ── The close gate ──────────────────────────────────────────────────────────


@pytest.fixture()
def closed_through_july(ledger):
  from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar

  ledger.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-07"))
  ledger.commit()
  return ledger


def _gate(session, **allow):
  from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService

  return FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-08", today=date(2026, 10, 1), **allow
  )


def test_a_ledger_with_no_reconciliation_closes_as_before(closed_through_july):
  gate = _gate(closed_through_july)

  assert gate.is_closeable
  assert gate.unreconciled_account_count == 0


def test_a_reconciled_period_passes_the_gate(closed_through_july):
  _refresh(closed_through_july, source_report(*TIED))

  gate = _gate(closed_through_july)

  assert gate.is_closeable
  assert gate.unreconciled_account_count == 0


def test_a_difference_holds_the_close_and_names_the_block(closed_through_july):
  _refresh(closed_through_july, source_report(*_BILL_REMOVED))

  gate = _gate(closed_through_july)

  assert gate.blockers == ["unreconciled_accounts"]
  assert gate.unreconciled_account_count == 1
  assert gate.unreconciled_account_sample == [
    "Source ledger (QuickBooks): unreconciled"
  ]


def test_a_period_nobody_compared_holds_the_close(closed_through_july):
  """The block exists from an earlier month; August was never compared."""
  _refresh(closed_through_july, source_report(*TIED), period="2026-07")

  gate = _gate(closed_through_july)

  assert gate.blockers == ["unreconciled_accounts"]
  assert gate.unreconciled_account_sample == ["Source ledger (QuickBooks): not_started"]


def test_the_bypass_lifts_the_gate_and_keeps_the_count(closed_through_july):
  _refresh(closed_through_july, source_report(*_BILL_REMOVED))

  gate = _gate(closed_through_july, allow_unreconciled_accounts=True)

  assert gate.is_closeable
  assert gate.unreconciled_account_count == 1


def test_a_released_block_does_not_hold_the_close(closed_through_july):
  result = _refresh(closed_through_july, source_report(*_BILL_REMOVED))
  set_reconciliation_policy(
    closed_through_july,
    SetReconciliationPolicyRequest(
      structure_id=result.reconciliations[0].structure_id, required_for_close=False
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  closed_through_july.commit()

  assert _gate(closed_through_july).is_closeable


# ── After a sync ────────────────────────────────────────────────────────────


def _refresh_next(session, report: TrialBalanceReport):
  from robosystems.operations.roboledger.commands.reconciliations import (
    refresh_next_period,
  )

  with patch.object(
    SourceLedgerResolver,
    "_fetch",
    return_value=(report, LIVE_CONNECTION, SYNCED_AT),
  ) as fetch:
    period = refresh_next_period(session, graph_id=GRAPH_ID, created_by="usr")
  session.commit()
  return period, fetch


def test_a_sync_does_not_enrol_a_ledger_that_never_reconciled(closed_through_july):
  period, fetch = _refresh_next(closed_through_july, source_report(*TIED))

  assert period is None
  fetch.assert_not_called()
  assert _blocks(closed_through_july) == []


def test_a_sync_refreshes_the_next_period_to_close(closed_through_july):
  """July was reconciled by hand; the sync keeps August current from then on."""
  _refresh(closed_through_july, source_report(*TIED), period="2026-07")

  period, _ = _refresh_next(closed_through_july, source_report(*_BILL_REMOVED))

  assert period == "2026-08"
  (august,) = list_reconciliations(closed_through_july, "2026-08").reconciliations
  assert august.status == "unreconciled"
  assert _gate(closed_through_july).blockers == ["unreconciled_accounts"]

  _refresh_next(closed_through_july, source_report(*TIED))
  assert _gate(closed_through_july).is_closeable


def test_a_sync_before_the_calendar_exists_refreshes_nothing(ledger):
  _refresh(ledger, source_report(*TIED), period="2026-07")

  period, fetch = _refresh_next(ledger, source_report(*TIED))

  assert period is None
  fetch.assert_not_called()


# ── Sign-off ────────────────────────────────────────────────────────────────

_MEMBERS = (
  "robosystems.operations.roboledger.commands.reconciliations._explicit_write_members"
)


def _sign_off(session, structure_id, *, by="usr", members=("usr",), period="2026-08"):
  from robosystems.models.api.extensions.reconciliations import (
    SignOffReconciliationRequest,
  )
  from robosystems.operations.roboledger.commands.reconciliations import (
    sign_off_reconciliation,
  )

  with patch(_MEMBERS, return_value=set(members)):
    result = sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(
        structure_id=structure_id, period=period, note="Tied to QuickBooks."
      ),
      graph_id=GRAPH_ID,
      created_by=by,
    )
  session.commit()
  return result


def _sign_off_events(session):
  from robosystems.models.extensions.roboledger import Event

  return list(
    session.execute(
      select(Event).where(Event.event_type == "reconciliation_signed_off")
    ).scalars()
  )


@pytest.fixture()
def reconciled(closed_through_july):
  result = _refresh(closed_through_july, source_report(*TIED))
  return closed_through_july, result.reconciliations[0].structure_id


def test_the_only_member_of_a_graph_can_sign_off(reconciled):
  """One user ran the comparison and reviews it: recorded as such, not refused."""
  session, structure_id = reconciled

  rec = _sign_off(session, structure_id)

  assert rec.status == "reviewed"
  assert (rec.reviewed_by, rec.compared_by, rec.compared_via) == (
    "usr",
    "usr",
    "operation",
  )
  assert rec.self_reviewed is True
  assert rec.reviewed_at is not None


def test_the_sign_off_is_a_support_event_that_writes_no_books(reconciled):
  session, structure_id = reconciled

  _sign_off(session, structure_id)

  (event,) = _sign_off_events(session)
  assert (event.event_class, event.event_category, event.status) == (
    "support",
    "approval",
    "committed",
  )
  assert event.created_by == "usr"
  assert event.amount is None
  assert event.metadata_["structure_id"] == structure_id
  assert event.metadata_["period"] == "2026-08"
  assert event.metadata_["note"] == "Tied to QuickBooks."
  assert event.metadata_["balance_digest"]
  # The close gate's unposted-event check does not see it.
  assert _gate(session).is_closeable


def test_access_through_the_org_alone_cannot_sign_off(reconciled):
  from robosystems.operations.roboledger.commands.reconciliations import (
    NotAGraphMemberError,
  )

  session, structure_id = reconciled

  with pytest.raises(NotAGraphMemberError, match="organization role"):
    _sign_off(session, structure_id, by="usr_org_admin", members=("usr",))

  assert _sign_off_events(session) == []


@pytest.mark.parametrize(
  ("report", "period", "reason"),
  [
    (_BILL_REMOVED, "2026-08", "do not reconcile"),
    (TIED, "2026-06", "has not been compared"),
  ],
)
def test_only_a_reconciled_period_can_be_signed_off(
  closed_through_july, report, period, reason
):
  from robosystems.operations.roboledger.commands.reconciliations import (
    ReconciliationNotReconciledError,
  )

  structure_id = (
    _refresh(closed_through_july, source_report(*report))
    .reconciliations[0]
    .structure_id
  )

  with pytest.raises(ReconciliationNotReconciledError, match=reason):
    _sign_off(closed_through_july, structure_id, period=period)


def test_signing_off_twice_adds_nothing(reconciled):
  session, structure_id = reconciled
  _sign_off(session, structure_id)

  rec = _sign_off(session, structure_id, by="usr2", members=("usr", "usr2"))

  assert (rec.status, rec.reviewed_by) == ("reviewed", "usr")
  assert len(_sign_off_events(session)) == 1


def test_a_refresh_with_the_same_balances_keeps_the_review(reconciled):
  session, structure_id = reconciled
  _sign_off(session, structure_id)

  (rec,) = _refresh(session, source_report(*TIED)).reconciliations

  assert (rec.status, rec.reviewed_by) == ("reviewed", "usr")


def test_a_balance_that_changes_after_the_review_lapses_it(reconciled, books):
  """The books moved after the review. Both sides still tie, but the reviewer
  never saw these figures."""
  session, structure_id = reconciled
  _sign_off(session, structure_id)
  from .conftest import entry

  entry(session, date(2026, 8, 30), books["software"], books["cash"], 4_000)
  session.commit()
  moved = (
    ("35", "Checking", 134_000),
    ("50", "Services", -50_000),
    ("70", "Software", 16_000),
    ("3", "Retained Earnings", -100_000),
  )

  (rec,) = _refresh(session, source_report(*moved)).reconciliations

  assert rec.status == "reconciled"
  assert (rec.reviewed_by, rec.reviewed_at, rec.self_reviewed) == (None, None, None)

  again = _sign_off(session, structure_id)
  assert again.status == "reviewed"
  assert len(_sign_off_events(session)) == 2


def test_balances_that_return_to_signed_figures_are_reviewed_again(reconciled, books):
  """A sign-off applies to the figures it pinned, whatever was signed since."""
  session, structure_id = reconciled
  _sign_off(session, structure_id)
  from .conftest import entry

  entry(session, date(2026, 8, 30), books["software"], books["cash"], 4_000)
  session.commit()
  moved = (
    ("35", "Checking", 134_000),
    ("50", "Services", -50_000),
    ("70", "Software", 16_000),
    ("3", "Retained Earnings", -100_000),
  )
  _refresh(session, source_report(*moved))
  members = ("usr", "usr2")
  assert _sign_off(session, structure_id, by="usr2", members=members).reviewed_by == (
    "usr2"
  )

  entry(session, date(2026, 8, 30), books["cash"], books["software"], 4_000)
  session.commit()
  (rec,) = _refresh(session, source_report(*TIED)).reconciliations

  assert (rec.status, rec.reviewed_by) == ("reviewed", "usr")
  assert len(_sign_off_events(session)) == 2


def test_a_reviewed_period_still_refuses_a_non_member(reconciled):
  from robosystems.operations.roboledger.commands.reconciliations import (
    NotAGraphMemberError,
  )

  session, structure_id = reconciled
  _sign_off(session, structure_id)

  with pytest.raises(NotAGraphMemberError):
    _sign_off(session, structure_id, by="usr_org_admin", members=("usr",))


def test_a_comparison_without_its_tied_rows_is_not_recorded(ledger):
  """The sign-off's digest covers every row, so a partial comparison would
  pin less than the reviewer saw."""
  from robosystems.operations.roboledger.commands import reconciliations as commands

  whole = commands.compute_reconciliations

  def open_rows_only(*args, **kwargs):
    return whole(*args, **{**kwargs, "include_tied": False})

  with (
    patch.object(commands, "compute_reconciliations", open_rows_only),
    pytest.raises(RuntimeError, match="every compared row"),
  ):
    _refresh(ledger, source_report(*TIED))
  ledger.rollback()

  assert _blocks(ledger) == []


# ── Review policy ───────────────────────────────────────────────────────────


def _policy(session, structure_id, *, members=("usr",), **fields):
  with patch(_MEMBERS, return_value=set(members)):
    result = set_reconciliation_policy(
      session,
      SetReconciliationPolicyRequest(structure_id=structure_id, **fields),
      graph_id=GRAPH_ID,
      created_by="usr",
    )
  session.commit()
  return result


def test_a_separate_reviewer_needs_two_members(reconciled):
  from robosystems.operations.roboledger.commands.reconciliations import (
    SeparateReviewerError,
  )

  session, structure_id = reconciled

  with pytest.raises(SeparateReviewerError, match="this graph has 1"):
    _policy(session, structure_id, separate_reviewer=True)
  session.rollback()

  policy = _policy(
    session, structure_id, members=("usr", "usr2"), separate_reviewer=True
  )
  assert policy.separate_reviewer is True


def test_a_separate_reviewer_cannot_be_the_person_who_ran_it(reconciled):
  from robosystems.operations.roboledger.commands.reconciliations import (
    SeparateReviewerError,
  )

  session, structure_id = reconciled
  _policy(session, structure_id, members=("usr", "usr2"), separate_reviewer=True)

  with pytest.raises(SeparateReviewerError, match="you ran this one"):
    _sign_off(session, structure_id, by="usr", members=("usr", "usr2"))
  session.rollback()

  rec = _sign_off(session, structure_id, by="usr2", members=("usr", "usr2"))
  assert (rec.status, rec.reviewed_by, rec.self_reviewed) == ("reviewed", "usr2", False)


def test_a_reviewer_left_as_the_only_member_is_told_the_way_out(reconciled):
  """The two-member check runs when the policy is turned on; the graph can
  lose a member afterwards."""
  from robosystems.operations.roboledger.commands.reconciliations import (
    SeparateReviewerError,
  )

  session, structure_id = reconciled
  _policy(session, structure_id, members=("usr", "usr2"), separate_reviewer=True)

  with pytest.raises(SeparateReviewerError, match="turn `separate_reviewer` off"):
    _sign_off(session, structure_id, by="usr", members=("usr",))


def test_a_comparison_the_sync_ran_has_no_person_to_conflict_with(closed_through_july):
  """The sync refreshed it, so the one who started the sync may review it even
  where a separate reviewer is required."""
  session = closed_through_july
  structure_id = (
    _refresh(session, source_report(*TIED), period="2026-07")
    .reconciliations[0]
    .structure_id
  )
  _policy(session, structure_id, members=("usr", "usr2"), separate_reviewer=True)
  _refresh_next(session, source_report(*TIED))

  rec = _sign_off(session, structure_id, by="usr", members=("usr", "usr2"))

  assert (rec.status, rec.compared_via, rec.self_reviewed) == (
    "reviewed",
    "sync",
    False,
  )


def test_a_block_that_requires_review_holds_the_close_until_signed(reconciled):
  session, structure_id = reconciled
  _policy(session, structure_id, review_required=True)

  gate = _gate(session)
  assert gate.blockers == ["unreconciled_accounts"]
  assert gate.unreconciled_account_sample == [
    "Source ledger (QuickBooks): awaiting review"
  ]

  _sign_off(session, structure_id)
  assert _gate(session).is_closeable


def test_a_comparison_recorded_before_sign_off_existed_asks_for_a_refresh(reconciled):
  from sqlalchemy.orm.attributes import flag_modified

  from robosystems.operations.roboledger.commands.reconciliations import (
    ReconciliationNotReconciledError,
  )

  session, structure_id = reconciled
  fact_set = session.query(FactSet).one()
  fact_set.metadata_ = {
    k: v for k, v in fact_set.metadata_.items() if k != "balance_digest"
  }
  flag_modified(fact_set, "metadata_")
  session.commit()

  with pytest.raises(ReconciliationNotReconciledError, match="run refresh"):
    _sign_off(session, structure_id)
  assert _sign_off_events(session) == []
