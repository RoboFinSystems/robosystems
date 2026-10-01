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
    created_by="usr",
  )
  closed_through_july.commit()

  assert _gate(closed_through_july).is_closeable
