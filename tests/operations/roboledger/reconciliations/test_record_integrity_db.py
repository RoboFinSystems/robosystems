"""The records a reconciliation is trusted on, against real Postgres: who may
write a sign-off or a recorded balance, who counts as its preparer, what a
policy change leaves behind, and which rule's result is its status.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import select

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.api.extensions.reconciliations import (
  RecordStatementBalanceRequest,
  RefreshReconciliationsRequest,
  SetReconciliationPolicyRequest,
  SignOffReconciliationRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleMetadataRequest,
)
from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
)
from robosystems.models.api.information_block import EvaluateRulesRequest
from robosystems.models.api.taxonomy_block import (
  DeleteTaxonomyBlockRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions import Rule, Structure, Taxonomy
from robosystems.models.extensions.entity import Entity
from robosystems.models.extensions.roboledger import Event, FactSet
from robosystems.models.extensions.roboledger.fiscal_calendar import FiscalCalendar
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  update_event_block,
)
from robosystems.operations.event_block.reserved import (
  RESERVED_EVENT_TYPES,
  ReservedEventTypeError,
)
from robosystems.operations.information_block.rules.commands import (
  cmd_evaluate_rules,
)
from robosystems.operations.information_block.rules.engine import (
  evaluate_rules_for_structure,
)
from robosystems.operations.roboledger.commands.reconciliations import (
  SeparateReviewerError,
  record_statement_balance,
  refresh_next_period,
  refresh_reconciliations,
  set_reconciliation_policy,
  sign_off_reconciliation,
)
from robosystems.operations.roboledger.commands.schedules import create_schedule
from robosystems.operations.roboledger.commands.taxonomies import (
  MappingStructureNotFoundError,
  create_mapping_association,
)
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.reads.reconciliations import (
  list_reconciliations,
)
from robosystems.operations.roboledger.reconciliations import (
  NoSourceLedgerError,
  SourceLedgerResolver,
)
from robosystems.operations.roboledger.reconciliations.blocks import (
  POLICY_CHANGE_EVENT_TYPE,
  SIGN_OFF_EVENT_TYPE,
  reconciliation_rule,
)
from robosystems.operations.roboledger.reconciliations.observations import (
  BALANCE_OBSERVED_EVENT_TYPE,
)
from robosystems.operations.taxonomy_block.commands import (
  delete_taxonomy_block,
  update_taxonomy_block,
)

from .conftest import (
  GRAPH_ID,
  LIVE_CONNECTION,
  SYNCED_AT,
  TIED,
  classified_account,
  entry,
  source_report,
)

pytestmark = pytest.mark.unit

_COMMANDS = "robosystems.operations.roboledger.commands.reconciliations"
_EVERYONE = {"usr", "usr2", "usr3"}


@pytest.fixture()
def loan(ext_session):
  """A ledger with a 4,800.00 loan, and a statement for it recorded by `usr`."""
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  cash = classified_account(session, "Checking", "asset")
  account = classified_account(
    session, "Equipment Loan", "liability", balance_type="credit"
  )
  entry(session, date(2026, 1, 10), cash, account, 480_000)
  rec = record_statement_balance(
    session,
    RecordStatementBalanceRequest(
      element_id=account, as_of=date(2026, 8, 31), balance=4_800.00
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  return session, rec.structure_id


def _refresh(session, *, by: str):
  with patch.object(
    SourceLedgerResolver, "_fetch", side_effect=NoSourceLedgerError("no source")
  ):
    refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period="2026-08"),
      graph_id=GRAPH_ID,
      created_by=by,
    )
  session.commit()


def _sign_off(session, structure_id, *, by: str):
  with patch(f"{_COMMANDS}._explicit_write_members", return_value=_EVERYONE):
    rec = sign_off_reconciliation(
      session,
      SignOffReconciliationRequest(structure_id=structure_id, period="2026-08"),
      graph_id=GRAPH_ID,
      created_by=by,
    )
  session.commit()
  return rec


def _policy(session, structure_id, **fields):
  with patch(f"{_COMMANDS}._explicit_write_members", return_value=_EVERYONE):
    set_reconciliation_policy(
      session,
      SetReconciliationPolicyRequest(structure_id=structure_id, **fields),
      graph_id=GRAPH_ID,
      created_by="usr2",
    )
  session.commit()


def _standing(session, structure_id):
  (rec,) = [
    r
    for r in list_reconciliations(session, "2026-08").reconciliations
    if r.structure_id == structure_id
  ]
  return rec


def test_the_reserved_types_are_the_ones_the_reconciliation_code_writes():
  assert (
    frozenset(
      {SIGN_OFF_EVENT_TYPE, POLICY_CHANGE_EVENT_TYPE, BALANCE_OBSERVED_EVENT_TYPE}
    )
    == RESERVED_EVENT_TYPES
  )


@pytest.mark.parametrize(
  ("event_type", "category"),
  [
    (SIGN_OFF_EVENT_TYPE, "approval"),
    (BALANCE_OBSERVED_EVENT_TYPE, "reconciliation"),
    (POLICY_CHANGE_EVENT_TYPE, "control"),
  ],
)
def test_the_event_operations_cannot_write_a_reconciliation_record(
  loan, event_type, category
):
  session, structure_id = loan
  digest = session.query(FactSet).one().metadata_["balance_digest"]

  with pytest.raises(ReservedEventTypeError):
    create_event_block_in_session(
      session,
      CreateEventBlockRequest(
        event_type=event_type,
        event_category=category,
        event_class="support",
        occurred_at=datetime(2026, 9, 2, tzinfo=UTC),
        source="manual",
        metadata={
          "structure_id": structure_id,
          "period": "2026-08",
          "balance_digest": digest,
        },
      ),
      "usr2",
      graph_id=GRAPH_ID,
    )

  assert _standing(session, structure_id).status == "reconciled"


def test_only_a_committed_sign_off_counts(loan):
  """A row that did not come from the sign-off operation is not a review."""
  session, structure_id = loan
  digest = session.query(FactSet).one().metadata_["balance_digest"]
  session.add(
    Event(
      event_type=SIGN_OFF_EVENT_TYPE,
      event_category="approval",
      event_class="support",
      occurred_at=datetime(2026, 9, 2, tzinfo=UTC),
      status="captured",
      source="manual",
      metadata_={
        "structure_id": structure_id,
        "period": "2026-08",
        "balance_digest": digest,
      },
      created_by="usr2",
    )
  )
  session.commit()

  assert _standing(session, structure_id).status == "reconciled"


def test_the_event_operations_cannot_alter_a_sign_off(loan):
  session, structure_id = loan
  _sign_off(session, structure_id, by="usr2")
  sign_off = session.execute(
    select(Event).where(Event.event_type == SIGN_OFF_EVENT_TYPE)
  ).scalar_one()

  with pytest.raises(ReservedEventTypeError):
    update_event_block(
      session,
      UpdateEventBlockRequest(
        event_id=sign_off.id, metadata_patch={"balance_digest": "another"}
      ),
      "usr",
      graph_id=GRAPH_ID,
    )
  with pytest.raises(ReservedEventTypeError):
    update_event_block(
      session,
      UpdateEventBlockRequest(event_id=sign_off.id, transition_to="voided"),
      "usr",
      graph_id=GRAPH_ID,
    )


def test_the_reconciliation_taxonomy_cannot_be_deleted(loan):
  session, structure_id = loan
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.name == "Reconciliations")
  ).scalar_one()

  with pytest.raises(ValueError, match="cannot be deleted"):
    delete_taxonomy_block(
      session,
      DeleteTaxonomyBlockRequest(
        taxonomy_id=taxonomy.id, cascade_facts=True, reason="not needed"
      ),
      "usr",
    )
  session.rollback()

  assert _standing(session, structure_id).status == "reconciled"


def test_a_taxonomy_created_before_the_lock_is_locked_on_the_next_refresh(loan):
  session, _ = loan
  taxonomy = session.execute(
    select(Taxonomy).where(Taxonomy.name == "Reconciliations")
  ).scalar_one()
  taxonomy.is_locked = False
  session.commit()

  _refresh(session, by="usr")

  session.refresh(taxonomy)
  assert taxonomy.is_locked is True


def test_the_person_who_recorded_a_statement_is_its_preparer(loan):
  """Someone else refreshing the comparison does not make the statement's
  author a separate reviewer of it."""
  session, structure_id = loan
  _policy(session, structure_id, separate_reviewer=True)
  _refresh(session, by="usr2")

  with pytest.raises(SeparateReviewerError, match="you recorded its balance"):
    _sign_off(session, structure_id, by="usr")
  session.rollback()
  with pytest.raises(SeparateReviewerError, match="you ran this one"):
    _sign_off(session, structure_id, by="usr2")
  session.rollback()

  rec = _sign_off(session, structure_id, by="usr3")
  assert (rec.status, rec.reviewed_by, rec.self_reviewed) == ("reviewed", "usr3", False)


def test_signing_your_own_statement_is_marked_as_a_self_review(loan):
  session, structure_id = loan
  _refresh(session, by="usr2")

  rec = _sign_off(session, structure_id, by="usr")

  assert (rec.status, rec.self_reviewed) == ("reviewed", True)


def test_a_policy_change_leaves_a_record_of_who_changed_what(loan):
  session, structure_id = loan

  _policy(session, structure_id, required_for_close=True, materiality=25.0)
  _policy(session, structure_id, required_for_close=True)

  (event,) = session.execute(
    select(Event).where(Event.event_type == POLICY_CHANGE_EVENT_TYPE)
  ).scalars()
  assert (event.event_class, event.event_category, event.status, event.created_by) == (
    "support",
    "control",
    "committed",
    "usr2",
  )
  assert event.metadata_ == {
    "structure_id": structure_id,
    "changes": {
      "required_for_close": {"from": False, "to": True},
      "materiality": {"from": 0.0, "to": 25.0},
    },
  }


def test_a_sync_that_recomputes_the_same_figures_keeps_their_preparer(
  ext_session, books
):
  """A scheduled sync re-running a comparison by hand left the figures as they
  were: whoever ran them still prepared what a reviewer signs."""
  session = ext_session
  session.add(Entity(name="Fictional Co", created_by="usr"))
  session.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-07"))
  session.commit()
  source = patch.object(
    SourceLedgerResolver,
    "_fetch",
    return_value=(source_report(*TIED), LIVE_CONNECTION, SYNCED_AT),
  )
  with source:
    (rec,) = refresh_reconciliations(
      session,
      RefreshReconciliationsRequest(period="2026-08"),
      graph_id=GRAPH_ID,
      created_by="usr",
    ).reconciliations
    session.commit()
    _policy(session, rec.structure_id, separate_reviewer=True)

    assert (
      refresh_next_period(session, graph_id=GRAPH_ID, created_by="usr") == "2026-08"
    )
    session.commit()

  with pytest.raises(SeparateReviewerError, match="you ran this one"):
    _sign_off(session, rec.structure_id, by="usr")


def _schedule_with_a_passing_rule(session) -> str:
  prepaid = classified_account(session, "Prepaid", "asset")
  expense = classified_account(session, "Insurance", "expense")
  return create_schedule(
    session,
    CreateScheduleRequest(
      name="Insurance",
      element_ids=[expense, prepaid],
      period_start=date(2026, 1, 1),
      period_end=date(2026, 12, 31),
      monthly_amount=10_000,
      entry_template=EntryTemplateRequest(
        debit_element_id=expense, credit_element_id=prepaid
      ),
      schedule_metadata=ScheduleMetadataRequest(original_amount=120_000),
    ),
    created_by="usr",
  ).structure_id


def _stamp_another_structures_rules(session, rec):
  schedule_id = _schedule_with_a_passing_rule(session)
  session.commit()
  with pytest.raises(ValueError, match="does not belong to structure"):
    cmd_evaluate_rules(
      session,
      EvaluateRulesRequest(structure_id=schedule_id, fact_set_id=rec.fact_set_id),
      "usr_writer",
    )
  session.rollback()


def _arc_a_foreign_element_onto_the_block(session, rec):
  account = classified_account(session, "Suspense", "asset")
  session.commit()
  with pytest.raises(MappingStructureNotFoundError):
    create_mapping_association(
      session,
      CreateMappingAssociationOperation(
        mapping_id=rec.structure_id,
        from_element_id=account,
        to_element_id=account,
        association_type="mapping",
      ),
      "usr_writer",
    )
  session.rollback()


def _a_pass_from_another_rule_lands_on_the_set(session, rec):
  schedule_id = _schedule_with_a_passing_rule(session)
  session.flush()
  for result in evaluate_rules_for_structure(session, schedule_id):
    result.fact_set_id = rec.fact_set_id
    result.status = "pass"
  session.commit()


def _update_the_blocks_taxonomy(session, rec):
  structure = session.get(Structure, rec.structure_id)
  with pytest.raises(ValueError, match="locked"):
    update_taxonomy_block(
      session,
      UpdateTaxonomyBlockRequest(taxonomy_id=str(structure.taxonomy_id)),
      "usr_writer",
    )
  session.rollback()


def _a_second_passing_rule_on_the_block(session, rec):
  # However it got there: the status is still read from the block's own rule.
  own = reconciliation_rule(session, rec.structure_id)
  assert own is not None
  session.add(
    Rule(
      taxonomy_id=own.taxonomy_id,
      rule_category=own.rule_category,
      rule_pattern="EqualTo",
      rule_expression=own.rule_expression,
      rule_severity="error",
      rule_origin="native",
      target_kind="structure",
      target_structure_id=own.target_structure_id,
      rule_variables=own.rule_variables,
      metadata_={"tolerance": 1e12},
      created_by="usr_writer",
    )
  )
  session.flush()
  cmd_evaluate_rules(
    session,
    EvaluateRulesRequest(structure_id=rec.structure_id, fact_set_id=rec.fact_set_id),
    "usr_writer",
  )
  session.commit()


@pytest.mark.parametrize(
  "door",
  [
    _stamp_another_structures_rules,
    _arc_a_foreign_element_onto_the_block,
    _a_pass_from_another_rule_lands_on_the_set,
    _update_the_blocks_taxonomy,
    _a_second_passing_rule_on_the_block,
  ],
  ids=[
    "evaluate-rules",
    "mapping-arc",
    "foreign-result",
    "taxonomy-update",
    "second-rule",
  ],
)
def test_only_the_blocks_own_rule_decides_its_status(loan, door):
  session, structure_id = loan
  session.add(FiscalCalendar(graph_id=GRAPH_ID, closed_through_period="2026-07"))
  record_statement_balance(
    session,
    RecordStatementBalanceRequest(
      element_id=_standing(session, structure_id).element_id,
      as_of=date(2026, 8, 31),
      balance=4_000.00,
    ),
    graph_id=GRAPH_ID,
    created_by="usr",
  )
  session.commit()
  _policy(session, structure_id, required_for_close=True)
  rec = _standing(session, structure_id)
  assert rec.status == "unreconciled"

  door(session, rec)

  assert _standing(session, structure_id).status == "unreconciled"
  gate = FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-08", today=date(2026, 10, 1)
  )
  assert "unreconciled_accounts" in gate.blockers
