"""Tests for schedule command lifecycle behavior."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

from robosystems.models.api.extensions.schedules import (
  DeleteScheduleRequest,
  RebuildScheduleRequest,
  UpdateScheduleRequest,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  FactSet,
  Rule,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.roboledger.commands.schedules import (
  delete_schedule,
  rebuild_schedule,
  update_schedule,
)


def _exec_result(
  *, row=None, scalars_all: list[str] | None = None, fetchone_row=None
) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = row
  result.scalars.return_value.all.return_value = scalars_all or []
  result.fetchone.return_value = fetchone_row
  return result


@dataclass
class _Query:
  model: type
  deleted_models: list[type]

  def filter(self, *_args) -> _Query:
    return self

  def delete(self, *, synchronize_session: bool = False) -> int:
    assert synchronize_session is False
    self.deleted_models.append(self.model)
    return 1


def test_delete_schedule_removes_information_block_dependents_before_structure() -> (
  None
):
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    # The pending-obligation void bounds its wait, so a `SET LOCAL
    # lock_timeout` lands here.
    _exec_result(),
    _exec_result(scalars_all=["assoc_1", "assoc_2"]),
    _exec_result(scalars_all=["rule_1"]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.void_pending_obligations",
    return_value=0,
  ):
    result = delete_schedule(
      session,
      DeleteScheduleRequest(structure_id="struct_sched"),
    )

  assert result == {"deleted": True}
  assert deleted_models == [
    VerificationResult,
    Fact,
    FactSet,
    Rule,
    AssociationClassification,
    Association,
  ]
  session.delete.assert_called_once_with(structure)
  session.commit.assert_called_once()


def test_delete_schedule_voids_pending_obligations_before_deletion() -> None:
  """Pending obligations must be voided so they don't outlive their originator."""
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(),  # SET LOCAL lock_timeout around the void
    _exec_result(scalars_all=[]),
    _exec_result(scalars_all=[]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.void_pending_obligations",
    return_value=12,
  ) as void:
    delete_schedule(
      session,
      DeleteScheduleRequest(structure_id="struct_sched"),
    )

  void.assert_called_once()
  kwargs = void.call_args.kwargs
  assert kwargs["structure"] is structure
  assert kwargs["void_reason"] == "schedule_deleted"
  # Schedule deletion does NOT pass voided_by_event_id — there is no
  # successor event, the schedule simply ceases to exist.
  assert "voided_by_event_id" not in kwargs or kwargs.get("voided_by_event_id") is None


def test_update_schedule_keeps_omitted_metadata_null_in_typed_mechanics() -> None:
  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Old Name"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
    }
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  response = update_schedule(
    session,
    UpdateScheduleRequest(structure_id="struct_sched", name="New Name"),
  )

  assert response.name == "New Name"
  assert structure.artifact_mechanics["schedule_metadata"] is None


def test_update_schedule_template_change_triggers_supersession() -> None:
  """Changing entry_template voids + re-materializes pending events."""
  from unittest.mock import patch

  from robosystems.models.api.extensions.schedules import EntryTemplateRequest

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Existing"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr_old",
      "credit_element_id": "elem_cr_old",
      "entry_type": "closing",
      "memo_template": "Old memo",
      "auto_reverse": False,
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    # The template-change branch bounds its wait for the pending obligations
    # before superseding them, so a `SET LOCAL lock_timeout` lands here.
    _exec_result(),
    _exec_result(fetchone_row=MagicMock(cnt=12)),
    _exec_result(fetchone_row=MagicMock(cnt=12)),
  ]

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.supersede_pending_obligations",
      return_value=12,
    ) as supersede,
    patch(
      "robosystems.operations.roboledger.commands.schedules.evaluate_rules_for_structure",
      return_value=[],
    ) as eval_rules,
  ):
    update_schedule(
      session,
      UpdateScheduleRequest(
        structure_id="struct_sched",
        entry_template=EntryTemplateRequest(
          debit_element_id="elem_dr_new",
          credit_element_id="elem_cr_new",
          entry_type="closing",
          memo_template="New memo",
          auto_reverse=False,
        ),
      ),
      updated_by="usr_admin",
    )

  supersede.assert_called_once()
  kwargs = supersede.call_args.kwargs
  assert kwargs["structure"] is structure
  assert kwargs["created_by"] == "usr_admin"
  # Template change re-runs the rule engine.
  eval_rules.assert_called_once()
  rule_kwargs = eval_rules.call_args.kwargs
  assert eval_rules.call_args.args[1] == "struct_sched"
  assert rule_kwargs["created_by"] == "usr_admin"


def test_update_schedule_no_template_change_skips_supersession() -> None:
  """Renaming the schedule must not touch the obligation chain."""
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Old Name"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.supersede_pending_obligations",
  ) as supersede:
    update_schedule(
      session,
      UpdateScheduleRequest(structure_id="struct_sched", name="New Name"),
    )

  supersede.assert_not_called()


def test_update_schedule_identical_template_skips_supersession() -> None:
  """Submitting the exact same template values is a no-op for the chain."""
  from unittest.mock import patch

  from robosystems.models.api.extensions.schedules import EntryTemplateRequest

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Existing"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
      "entry_type": "closing",
      "memo_template": "memo",
      "auto_reverse": False,
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.supersede_pending_obligations",
  ) as supersede:
    update_schedule(
      session,
      UpdateScheduleRequest(
        structure_id="struct_sched",
        entry_template=EntryTemplateRequest(
          debit_element_id="elem_dr",
          credit_element_id="elem_cr",
          entry_type="closing",
          memo_template="memo",
          auto_reverse=False,
        ),
      ),
    )

  supersede.assert_not_called()


def test_promote_obligations_maps_result_and_commits() -> None:
  """The on-demand promote-obligations command wraps the same
  ``promote_pending_obligations`` sweep the Dagster sensor uses, threads
  ``dispatch_handlers`` + actor through, commits, and maps the
  PromotionResult to the response envelope."""
  from unittest.mock import patch

  from robosystems.models.api.extensions.schedules import PromoteObligationsRequest
  from robosystems.operations.event_block.promotion import PromotionResult
  from robosystems.operations.roboledger.commands.schedules import promote_obligations

  session = MagicMock()
  fake = PromotionResult(
    graph_id="g",
    classified_event_ids=["evt_a", "evt_b"],
    dispatched_event_ids=["evt_a"],
    errors=[("evt_b", "boom")],
  )

  with patch(
    "robosystems.operations.event_block.promotion.promote_pending_obligations",
    return_value=fake,
  ) as mock_promote:
    resp = promote_obligations(
      session,
      PromoteObligationsRequest(dispatch_handlers=False),
      created_by="user_1",
    )

  assert mock_promote.call_args.kwargs["dispatch_handlers"] is False
  assert mock_promote.call_args.kwargs["created_by"] == "user_1"
  session.commit.assert_called_once()

  assert resp.classified_count == 2
  assert resp.dispatched_count == 1
  assert resp.error_count == 1
  assert resp.classified_event_ids == ["evt_a", "evt_b"]
  assert resp.errors == [{"event_id": "evt_b", "error": "boom"}]


# ─── rebuild_schedule ─────────────────────────────────────────────────────


def _rebuild_structure() -> MagicMock:
  """A schedule Structure whose stored metadata carries a full,
  reproducible definition (the post-create-schedule shape)."""
  structure = MagicMock()
  structure.id = "struct_rebuild"
  structure.name = "Depreciation Schedule"
  structure.block_type = "schedule"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
      "entry_type": "closing",
      "memo_template": "Monthly depreciation",
      "auto_reverse": False,
    },
    "schedule_metadata": {
      "method": "straight_line",
      "original_amount": 120_000,
      "residual_value": 0,
      "useful_life_months": 12,
      "asset_element_id": "elem_asset",
      "periodic_amounts": None,
    },
    "monthly_amount": 10_000,
    "period_start": "2026-01-01",
    "period_end": "2026-03-31",
    "schedule_created_event_id": "evt_old_origin",
    "pending_event_count": 3,
  }
  structure.artifact_mechanics = {
    "source_transaction_id": "txn_origin",
    "schedule_created_event_id": "evt_old_origin",
  }
  return structure


def _rebuild_session(
  structure: MagicMock, *, posted_count: int = 0, old_event=None
) -> tuple[MagicMock, list[type]]:
  """Wire a MagicMock session for the rebuild orchestration.

  execute call order:
    1. _load_schedule_or_404 → scalar_one_or_none → structure
    2. posted-entry guard → fetchone → MagicMock(c=posted_count)
    3. SET LOCAL lock_timeout (bounded wait around the obligation void)
    4. select(Rule.id) for cascade delete → scalars().all()
    5. DELETE line_items (draft sweep) → execute (result unused)
    6. DELETE entries (draft sweep) → execute (result unused)
    7. count facts → fetchone
    8. count distinct periods → fetchone
  query().filter().delete() captures the deleted models in order.
  _calendar_closed_through_date uses session.query(FiscalCalendar).first().
  session.get(Event, ...) returns ``old_event`` (the supersede path).
  """
  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),  # _load_schedule_or_404
    _exec_result(fetchone_row=MagicMock(c=posted_count)),  # posted-entry guard
    # The void of the old obligation chain bounds its wait for the rows the
    # promotion sweep may hold, so a `SET LOCAL lock_timeout` lands here.
    _exec_result(),
    _exec_result(scalars_all=["rule_old_1"]),  # select(Rule.id)
    _exec_result(),  # DELETE line_items (draft sweep)
    _exec_result(),  # DELETE entries (draft sweep)
    _exec_result(fetchone_row=MagicMock(cnt=6)),  # count facts
    _exec_result(fetchone_row=MagicMock(cnt=3)),  # count distinct periods
  ]

  session.get.return_value = old_event

  # session.query(...) is used both by _calendar_closed_through_date
  # (FiscalCalendar.first() → None) and by the cascade deletes
  # (.filter().delete()).
  def _query(model):
    if model.__name__ == "FiscalCalendar":
      q = MagicMock()
      q.first.return_value = None
      return q
    return _Query(model, deleted_models)

  session.query.side_effect = _query
  return session, deleted_models


def test_rebuild_schedule_voids_old_obligations_and_regenerates() -> None:
  from unittest.mock import patch

  structure = _rebuild_structure()
  session, deleted_models = _rebuild_session(structure)

  # create_schedule returns the (in-place) structure; stamp the fresh chain.
  rebuilt = structure
  rebuilt.metadata_ = {
    **structure.metadata_,
    "schedule_created_event_id": "evt_new_origin",
    "pending_event_count": 3,
  }

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.void_pending_obligations",
      return_value=3,
    ) as void,
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.create_schedule",
      return_value=rebuilt,
    ) as create,
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "evaluate_rules_for_structure",
      return_value=[],
    ),
  ):
    resp = rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_rebuild"),
      created_by="usr_admin",
    )

  # Old pending chain voided before regeneration, with the rebuild reason.
  void.assert_called_once()
  assert void.call_args.kwargs["structure"] is structure
  assert void.call_args.kwargs["void_reason"] == "schedule_rebuilt"

  # Regeneration runs in place on the SAME structure (id preserved).
  create.assert_called_once()
  ckwargs = create.call_args.kwargs
  assert ckwargs["existing_structure"] is structure
  assert ckwargs["taxonomy_id"] == "tax_1"
  assert ckwargs["monthly_amount"] == 10_000
  assert ckwargs["period_start"].isoformat() == "2026-01-01"
  assert ckwargs["period_end"].isoformat() == "2026-03-31"
  # Definition reconstructed from metadata.
  assert ckwargs["entry_template"].debit_element_id == "elem_dr"
  assert ckwargs["schedule_metadata"].original_amount == 120_000
  assert ckwargs["created_by"] == "usr_admin"

  # Old facts/factsets/rules deleted, but NOT associations or the structure.
  assert VerificationResult in deleted_models
  assert Fact in deleted_models
  assert FactSet in deleted_models
  assert Rule in deleted_models
  assert Association not in deleted_models
  session.delete.assert_not_called()

  session.commit.assert_called_once()

  # Response shape mirrors create_schedule.
  assert resp.structure_id == "struct_rebuild"
  assert resp.total_periods == 3
  assert resp.total_facts == 6
  assert resp.schedule_created_event_id == "evt_new_origin"


def test_rebuild_schedule_preserves_structure_id_and_associations() -> None:
  """The structure row + its associations survive a rebuild."""
  from unittest.mock import patch

  structure = _rebuild_structure()
  session, deleted_models = _rebuild_session(structure)

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.void_pending_obligations",
      return_value=0,
    ),
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.create_schedule",
      return_value=structure,
    ),
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "evaluate_rules_for_structure",
      return_value=[],
    ),
  ):
    resp = rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_rebuild"),
    )

  # Structure itself is never deleted; associations are preserved.
  session.delete.assert_not_called()
  assert Association not in deleted_models
  assert resp.structure_id == "struct_rebuild"


def test_rebuild_schedule_raises_when_definition_unreconstructable() -> None:
  """A schedule with no stored definition and no facts can't be rebuilt."""
  import pytest

  structure = MagicMock()
  structure.id = "struct_legacy"
  structure.block_type = "schedule"
  structure.metadata_ = {}  # no entry_template

  session = MagicMock()
  session.execute.side_effect = [_exec_result(row=structure)]

  with pytest.raises(ValueError, match="entry_template"):
    rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_legacy"),
    )

  session.commit.assert_not_called()


def test_rebuild_schedule_guards_posted_entries() -> None:
  """A schedule with posted closing entries refuses to rebuild."""
  from unittest.mock import patch

  import pytest

  structure = _rebuild_structure()
  # Posted-entry guard returns c=2; the rebuild must bail before any
  # void/regeneration work.
  session, _ = _rebuild_session(structure, posted_count=2)

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.void_pending_obligations",
      return_value=0,
    ) as void,
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.create_schedule",
      return_value=structure,
    ) as create,
    pytest.raises(ValueError, match="posted"),
  ):
    rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_rebuild"),
    )

  # Guard fires before voiding/regeneration and the transaction never commits.
  void.assert_not_called()
  create.assert_not_called()
  session.commit.assert_not_called()


def test_rebuild_schedule_preserves_source_transaction_id() -> None:
  """The source_transaction_id back-ref survives a rebuild — recovered
  from artifact_mechanics and passed through to create_schedule."""
  from unittest.mock import patch

  structure = _rebuild_structure()
  session, _ = _rebuild_session(structure)

  rebuilt = structure
  rebuilt.metadata_ = {
    **structure.metadata_,
    "schedule_created_event_id": "evt_new_origin",
  }

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.void_pending_obligations",
      return_value=0,
    ),
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.create_schedule",
      return_value=rebuilt,
    ) as create,
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "evaluate_rules_for_structure",
      return_value=[],
    ),
  ):
    rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_rebuild"),
    )

  create.assert_called_once()
  assert create.call_args.kwargs["source_transaction_id"] == "txn_origin"


def test_rebuild_schedule_supersedes_old_originator() -> None:
  """After a rebuild the old schedule_created event is voided and back-linked
  to the new originator."""
  from unittest.mock import patch

  structure = _rebuild_structure()
  old_evt = MagicMock()
  old_evt.id = "evt_old_origin"
  old_evt.status = "committed"
  old_evt.replaced_by_event_id = None
  session, _ = _rebuild_session(structure, old_event=old_evt)

  # create_schedule re-stamps the originator on the in-place structure. Mutate
  # metadata inside the mock so the OLD id is still readable when rebuild_schedule
  # captures it (before the void), and the NEW id afterward.
  def _restamp(*_args, **_kwargs):
    structure.metadata_ = {
      **structure.metadata_,
      "schedule_created_event_id": "evt_new_origin",
    }
    return structure

  with (
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.void_pending_obligations",
      return_value=0,
    ),
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "ScheduleService.create_schedule",
      side_effect=_restamp,
    ),
    patch(
      "robosystems.operations.roboledger.commands.schedules."
      "evaluate_rules_for_structure",
      return_value=[],
    ),
  ):
    rebuild_schedule(
      session,
      RebuildScheduleRequest(structure_id="struct_rebuild"),
    )

  # Old originator loaded by id, voided, and linked to its replacement.
  from robosystems.models.extensions.roboledger.event import Event

  session.get.assert_called_once_with(Event, "evt_old_origin")
  assert old_evt.status == "voided"
  assert old_evt.replaced_by_event_id == "evt_new_origin"


def test_reconstruct_schedule_definition_legacy_fallback() -> None:
  """A legacy schedule with no scalar inputs derives period bounds from the
  FactSet and monthly_amount from the first duration debit fact."""
  from datetime import date
  from unittest.mock import patch

  from robosystems.operations.roboledger.commands.schedules import (
    _reconstruct_schedule_definition,
  )

  structure = MagicMock()
  structure.id = "struct_legacy"
  structure.artifact_mechanics = None
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
    },
    # No monthly_amount / period_start / period_end → legacy fallbacks fire.
  }

  session = MagicMock()
  # 1. FactSet bounds (fetchall) → min_start / max_end rows.
  bounds_result = MagicMock()
  bounds_result.fetchall.return_value = [
    MagicMock(min_start=date(2026, 1, 1), max_end=date(2026, 2, 28)),
    MagicMock(min_start=date(2026, 2, 1), max_end=date(2026, 3, 31)),
  ]
  # 2. first duration debit fact value (scalar) → 10_000.00 dollars.
  debit_result = MagicMock()
  debit_result.scalar.return_value = 10_000.0
  session.execute.side_effect = [bounds_result, debit_result]

  with patch(
    "robosystems.operations.roboledger.commands.schedules.select",
    return_value=MagicMock(),
  ):
    (
      entry_template,
      schedule_metadata,
      monthly_amount,
      period_start,
      period_end,
      source_transaction_id,
    ) = _reconstruct_schedule_definition(session, structure)

  assert entry_template.debit_element_id == "elem_dr"
  assert schedule_metadata is None
  assert period_start == date(2026, 1, 1)
  assert period_end == date(2026, 3, 31)
  assert monthly_amount == 1_000_000  # 10_000.00 * 100 cents
  assert source_transaction_id is None


def test_reinstate_reopened_schedule_scopes_promotes_now_open_facts() -> None:
  """After the close boundary retreats, schedule facts beyond it flip
  ``historical`` → ``in_scope`` so the reopened month's movement is visible."""
  from datetime import date
  from unittest.mock import patch

  from robosystems.operations.roboledger.commands import schedules as sched_cmds

  session = MagicMock()
  exec_result = MagicMock()
  exec_result.rowcount = 3
  session.execute.return_value = exec_result

  with patch.object(
    sched_cmds, "_calendar_closed_through_date", return_value=date(2026, 2, 28)
  ):
    n = sched_cmds.reinstate_reopened_schedule_scopes(session)

  assert n == 3
  stmt_arg, params = session.execute.call_args[0]
  sql = str(stmt_arg)
  assert "UPDATE facts" in sql
  assert "fact_scope = 'in_scope'" in sql
  assert "block_type = 'schedule'" in sql
  assert params == {"closed_through": date(2026, 2, 28)}


def test_reinstate_reopened_schedule_scopes_handles_null_boundary() -> None:
  """When no period remains closed (boundary → None), all historical schedule
  facts reopen — the SQL guards NULL so every period is now in scope."""
  from unittest.mock import patch

  from robosystems.operations.roboledger.commands import schedules as sched_cmds

  session = MagicMock()
  exec_result = MagicMock()
  exec_result.rowcount = 5
  session.execute.return_value = exec_result

  with patch.object(sched_cmds, "_calendar_closed_through_date", return_value=None):
    n = sched_cmds.reinstate_reopened_schedule_scopes(session)

  assert n == 5
  _stmt, params = session.execute.call_args[0]
  assert params == {"closed_through": None}
