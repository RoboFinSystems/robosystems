"""Tests for the roboledger operation routes.

Direct-call tests (no FastAPI TestClient) matching the pattern used in
`tests/routers/ledger/test_entity.py`. The dispatch mechanics are
tested in `tests/middleware/test_extensions.py` — these tests verify
that each route function wires the right command + request model +
envelope shape.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, call, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.operations import OperationEnvelope
from robosystems.models.api.extensions.blocked_source_graphs import (
  BlockedSourceGraphResponse,
  BlockSourceGraphResult,
)
from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  DeleteJournalEntryRequest,
  JournalEntryLineItemInput,
  JournalEntryLineItemResponse,
  JournalEntryResponse,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.report_package import (
  FileReportRequest,
  TransitionFilingStatusRequest,
)
from robosystems.models.api.extensions.reports import (
  CreateReportRequest,
  ReportResponse,
  RevokeReportShareResponse,
  ShareReportResponse,
  ShareResultItem,
)
from robosystems.operations.roboledger.commands.entity import ParentEntityNotFoundError
from robosystems.routers.extensions.roboledger.operations import (
  AutoMapElementsOperation,
  BlockSourceGraphOperation,
  RegenerateReportOperation,
  RevokeReportShareOperation,
  ShareReportOperation,
  auto_map_elements_op,
  block_source_graph_op,
  create_report_op,
  delete_journal_entry_op,
  file_report_op,
  regenerate_report_op,
  revoke_report_share_op,
  share_report_op,
  transition_filing_status_op,
  update_entity_op,
  update_event_block_op,
  update_journal_entry_op,
)

GRAPH_ID = "kg01234567890abcdef"


@pytest.fixture(autouse=True)
def _bypass_write_role():
  """The registrar handler enforces the member/admin write role via
  ``require_graph_write_role`` (unit-tested in
  ``tests/middleware/auth/test_dependencies.py``; the deny path is asserted in
  ``TestRegistrarWriteRoleGate`` below). These direct-call wiring tests use a
  mock user with no DB-backed ``GraphUser`` row, so no-op the gate here.

  Deliberately scoped to ``middleware.extensions`` only. The hand-written
  handlers enforce through the ``_require_roboledger_write`` *dependency*,
  which direct calls never resolve — broadening this patch to that module
  would silently disarm ``TestHandWrittenWriteRoleGate`` below, which is the
  same blind spot that let the gap ship in the first place."""
  with patch(
    "robosystems.middleware.extensions.require_graph_write_role", return_value=None
  ):
    yield


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _make_entity_response() -> LedgerEntityResponse:
  return LedgerEntityResponse(
    id="ent_kg01234567890abcdef",
    name="Acme Corp",
    legal_name="Acme Corporation LLC",
    status="active",
    source="native",
    is_parent=True,
    created_at=datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
    updated_at=datetime(2026, 1, 1, tzinfo=UTC).isoformat(),
  )


class _FakeCache:
  async def reserve(self, *args, **kwargs):
    return True

  async def release(self, *args, **kwargs):
    return None

  """In-memory idempotency cache matching the real signature.

  Mirrors `IdempotencyCache.get/put` so route-handler tests can
  exercise the (user, graph, operation, key, fingerprint) tuple.
  """

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    from robosystems.middleware.operations import IdempotencyKeyConflictError

    entry = self.store.get((user_id, graph_id, operation_name, idempotency_key))
    if entry is None:
      return None
    cached_envelope, cached_fp = entry
    if cached_fp != body_fingerprint:
      raise IdempotencyKeyConflictError(operation_name)
    return cached_envelope

  async def put(
    self,
    user_id,
    graph_id,
    operation_name,
    idempotency_key,
    envelope,
    body_fingerprint,
    ttl_seconds=86400,
  ):
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = (
      envelope,
      body_fingerprint,
    )

  async def bind_operation(self, operation_id, cache_key):
    self.bindings = getattr(self, "bindings", {})
    self.bindings.setdefault(operation_id, set()).add(cache_key)


class TestUpdateEntityOp:
  @pytest.mark.asyncio
  async def test_happy_path_wraps_result_in_envelope(self) -> None:
    body = UpdateEntityRequest(name="New Name", phone="555-1234")
    entity_resp = _make_entity_response()

    with (
      patch(
        "robosystems.operations.roboledger.commands.entity.update_entity",
        return_value=entity_resp,
      ),
      _mock_session_ctx() as mock_session,
      patch("robosystems.middleware.extensions.mark_graph_stale") as mark,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_entity_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "update-entity"
    assert envelope.status == "completed"
    assert envelope.operation_id.startswith("op_")
    # Result is the Pydantic model serialized to dict
    assert envelope.result is not None
    assert envelope.result["id"] == "ent_kg01234567890abcdef"
    assert envelope.result["name"] == "Acme Corp"
    # `name`, `legal_name`, `ticker`, `cik` … are all columns of the
    # materialized Entity node, so the edit has to reach LadybugDB.
    mark.assert_called_once_with(GRAPH_ID, "entity_updated")

  @pytest.mark.asyncio
  async def test_failed_update_does_not_mark_the_graph(self) -> None:
    """The hook is `on_fresh_success` — a 404 must not schedule a rebuild."""
    body = UpdateEntityRequest(name="New Name")

    with (
      patch(
        "robosystems.operations.roboledger.commands.entity.update_entity",
        side_effect=ParentEntityNotFoundError(),
      ),
      _mock_session_ctx() as mock_session,
      patch("robosystems.middleware.extensions.mark_graph_stale") as mark,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException):
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    mark.assert_not_called()

  @pytest.mark.asyncio
  async def test_rejects_empty_update_with_400(self) -> None:
    body = UpdateEntityRequest()  # all fields None

    with pytest.raises(HTTPException) as exc:
      await update_entity_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert exc.value.status_code == 400
    assert "No fields provided" in exc.value.detail

  @pytest.mark.asyncio
  async def test_404_when_no_entity_exists(self) -> None:
    body = UpdateEntityRequest(name="New Name")

    # Patches the inner writer, not the command, so the real
    # `update_entity` runs and its None → ParentEntityNotFoundError → 404
    # translation is what gets asserted.
    with (
      patch(
        "robosystems.operations.roboledger.commands.entity.update_parent_entity",
        return_value=None,
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 404
    assert "No entity found" in exc.value.detail

  @pytest.mark.asyncio
  async def test_404_when_schema_not_initialized(self) -> None:
    from sqlalchemy.exc import ProgrammingError

    body = UpdateEntityRequest(name="New Name")

    with patch(
      "robosystems.db.extensions.extensions_session",
      side_effect=ProgrammingError(
        "stmt", {}, Exception('relation "entities" does not exist')
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 404
    assert "not initialized" in exc.value.detail

  @pytest.mark.asyncio
  async def test_cancelled_statement_is_a_504_without_the_sql(self) -> None:
    """A statement the session ceiling cut short surfaces as a retryable 504
    with a fixed message — never the SQL, never a bare 500."""
    from sqlalchemy.exc import OperationalError

    body = UpdateEntityRequest(name="New Name")

    with patch(
      "robosystems.db.extensions.extensions_session",
      side_effect=OperationalError(
        "UPDATE entities SET name = %(n)s",
        {"n": "secret"},
        MagicMock(pgcode="57014"),
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 504
    assert "secret" not in exc.value.detail
    assert exc.value.headers == {"Retry-After": "5"}

  @pytest.mark.asyncio
  async def test_other_programming_error_is_not_a_404(self) -> None:
    """Only a missing tenant schema means "not initialized". A different
    database fault used to be swallowed into the same friendly 404 by the
    hand-written handlers; the shared guard lets it surface."""
    from sqlalchemy.exc import ProgrammingError

    body = UpdateEntityRequest(name="New Name")

    with patch(
      "robosystems.db.extensions.extensions_session",
      side_effect=ProgrammingError(
        "stmt", {}, Exception('column "nope" does not exist')
      ),
    ):
      with pytest.raises(ProgrammingError):
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

  @pytest.mark.asyncio
  async def test_unmapped_value_error_is_422_with_its_message(self) -> None:
    body = UpdateEntityRequest(name="New Name")

    with patch(
      "robosystems.operations.roboledger.commands.entity.update_parent_entity",
      side_effect=ValueError("ticker must be uppercase"),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_entity_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 422
    assert exc.value.detail == "ticker must be uppercase"

  @pytest.mark.asyncio
  async def test_idempotency_key_cached_replay(self) -> None:
    body = UpdateEntityRequest(name="Same Name")
    entity_resp = _make_entity_response()
    cache = _FakeCache()

    with (
      patch(
        "robosystems.operations.roboledger.commands.entity.update_parent_entity",
        return_value=entity_resp,
      ) as mock_cmd,
      patch("robosystems.db.extensions.extensions_session") as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      # First call — hits the ops layer, caches the result
      first = await update_entity_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="retry-key-1",
        cache=cache,
      )

      # Second call with same key — should short-circuit and not touch ops
      second = await update_entity_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="retry-key-1",
        cache=cache,
      )

    assert mock_cmd.call_count == 1  # ops fn only called once
    assert first.operation_id == second.operation_id
    assert first.result == second.result


class TestAutoMapElementsOp:
  """auto_map_elements_op hand-rolls its own idempotency cache handling
  because it's async/Dagster-dispatched and can't route through
  `execute_operation`. That manual path has to mirror the dispatcher's
  replay-marking contract — if it drifts, retried calls double-count
  business events and clients can't tell replay from fresh enqueue.
  """

  @pytest.mark.asyncio
  async def test_fresh_enqueue_returns_pending_envelope(self) -> None:
    body = AutoMapElementsOperation(mapping_id="map_abc")
    cache = _FakeCache()
    task_response = {
      "operation_id": "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
      "operation_type": "agent",
      "_links": {"stream": "/v1/operations/op_01ARZ.../stream"},
      "deduplicated": False,
    }

    with patch(
      "robosystems.worker.client.enqueue_task",
      return_value=task_response,
    ) as mock_enqueue:
      envelope = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=cache,
      )

    assert mock_enqueue.call_count == 1
    assert isinstance(envelope, OperationEnvelope)
    assert envelope.status == "pending"
    assert envelope.operation == "auto-map-elements"
    assert envelope.operation_id == "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
    assert envelope.created_by == "usr_test123"
    # Fresh enqueue — flag is False so the metrics decorator counts
    # the business event.
    assert envelope.idempotent_replay is False

  @pytest.mark.asyncio
  async def test_idempotent_replay_marks_envelope_and_skips_enqueue(self) -> None:
    """Regression test for the replay-flag bug.

    The cached-hit branch must return a copy with
    `idempotent_replay=True` so (a) the metrics decorator suppresses
    the business_event counter and (b) clients can tell "task already
    enqueued" from "task newly enqueued". Without the `model_copy`,
    retries inflate the `ledger_auto_map_elements` counter.
    """
    body = AutoMapElementsOperation(mapping_id="map_xyz")
    cache = _FakeCache()
    task_response = {
      "operation_id": "op_01REPLAYFLAGTEST0000000000",
      "operation_type": "agent",
      "_links": {},
      "deduplicated": False,
    }

    with patch(
      "robosystems.worker.client.enqueue_task",
      return_value=task_response,
    ) as mock_enqueue:
      first = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="retry-key-automap",
        cache=cache,
      )

      second = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="retry-key-automap",
        cache=cache,
      )

    # The worker must only be enqueued once — the second call is a replay
    assert mock_enqueue.call_count == 1
    # Same operation_id on both calls (same underlying task)
    assert first.operation_id == second.operation_id == "op_01REPLAYFLAGTEST0000000000"
    # Fresh call: idempotent_replay=False
    assert first.idempotent_replay is False
    # Replayed call: idempotent_replay=True (the fix under test)
    assert second.idempotent_replay is True

  @pytest.mark.asyncio
  async def test_fresh_enqueue_binds_the_operation_to_its_envelope_key(
    self,
  ) -> None:
    """The pending envelope is bound to the worker task's operation id so a
    failed run can evict it (a retry then dispatches again instead of
    replaying `pending` for 24h)."""
    from robosystems.middleware.operations import compute_idempotency_cache_key

    body = AutoMapElementsOperation(mapping_id="map_bind")
    cache = _FakeCache()
    task_response = {
      "operation_id": "op_01BINDTEST0000000000000000",
      "operation_type": "operator",
      "_links": {},
      "deduplicated": False,
    }

    with patch(
      "robosystems.worker.client.enqueue_task",
      return_value=task_response,
    ):
      await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="bind-key",
        cache=cache,
      )

    assert cache.bindings == {
      "op_01BINDTEST0000000000000000": {
        compute_idempotency_cache_key(
          "usr_test123", GRAPH_ID, "auto-map-elements", "bind-key"
        )
      }
    }

  @pytest.mark.asyncio
  async def test_idempotent_replay_does_not_poison_cached_instance(self) -> None:
    """The cached envelope must stay `idempotent_replay=False` after a
    replay so subsequent replays still read a clean copy. Tests that
    we `model_copy(update=...)` instead of mutating `cached` in place.
    """
    body = AutoMapElementsOperation(mapping_id="map_poison")
    cache = _FakeCache()
    task_response = {
      "operation_id": "op_01POISONTEST00000000000000",
      "operation_type": "agent",
      "_links": {},
      "deduplicated": False,
    }

    with patch(
      "robosystems.worker.client.enqueue_task",
      return_value=task_response,
    ):
      # Prime cache
      first = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="poison-key",
        cache=cache,
      )
      # Two replays in a row
      second = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="poison-key",
        cache=cache,
      )
      third = await auto_map_elements_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="poison-key",
        cache=cache,
      )

    assert first.idempotent_replay is False
    assert second.idempotent_replay is True
    assert third.idempotent_replay is True
    # The cached envelope stored in the fake cache should still read
    # False — we never mutated it. (If we had used `cached.idempotent_replay
    # = True` instead of model_copy, the stored object would now be True
    # and this assertion would fail.)
    stored_envelope, _ = next(iter(cache.store.values()))
    assert stored_envelope.idempotent_replay is False


# ═══════════════════════════════════════════════════════════════════════════
# Taxonomy / Structure / Element / Association CRUD route tests — REMOVED.
#
# The 12 raw CRUD ops (create/update/delete-taxonomy, create/update/delete-
# structure, create/update/delete-element, create-associations, update/delete-
# association) were retired from the public REST + MCP surface in favor of
# the Taxonomy Block envelope. The
# underlying `cmd_*` functions remain for internal seeders and continue to be
# covered by `tests/operations/roboledger/commands/test_{taxonomies,elements}.py`.
# ═══════════════════════════════════════════════════════════════════════════


def _mock_session_ctx():
  """Return a patched `extensions_session` context manager mock.

  Patches at the source module (`robosystems.db.extensions`) so the
  late-binding in `OperationRegistrar._build_handler` picks up the mock
  at call time. Patching `...operations.extensions_session` would not
  work because the registry captures the factory by late-binding via
  sys.modules, resolving the name against the source module.
  """
  from unittest.mock import patch

  return patch("robosystems.db.extensions.extensions_session")


# ═══════════════════════════════════════════════════════════════════════════
# Journal entry CRUD route tests (native-accounting write path)
# ═══════════════════════════════════════════════════════════════════════════


def _make_journal_entry_response(
  entry_id: str = "je_abc",
  status: str = "draft",
  reversal_of: str | None = None,
) -> JournalEntryResponse:
  return JournalEntryResponse(
    id=entry_id,
    transaction_id=None,
    type="standard",
    status=status,
    posting_date=date(2026, 3, 31),
    memo="Test entry",
    provenance="manual_entry",
    reversal_of=reversal_of,
    posted_at=None,
    line_items=[
      JournalEntryLineItemResponse(
        id="li_1",
        element_id="elem_cash",
        debit_amount=10000,
        credit_amount=0,
        description=None,
        line_order=1,
      ),
      JournalEntryLineItemResponse(
        id="li_2",
        element_id="elem_revenue",
        debit_amount=0,
        credit_amount=10000,
        description=None,
        line_order=2,
      ),
    ],
    total_debit=10000,
    total_credit=10000,
  )


def _balanced_lines() -> list[JournalEntryLineItemInput]:
  return [
    JournalEntryLineItemInput(element_id="elem_cash", debit_amount=10000),
    JournalEntryLineItemInput(element_id="elem_revenue", credit_amount=10000),
  ]


# TestCreateJournalEntryOp removed: the `create-journal-entry`
# OperationSpec was retired in favor of
# `create-event-block(event_type='journal_entry_recorded')`. See
# tests/operations/event_block/python_handlers/
# test_journal_entry_recorded.py for coverage of the event-driven path.


class TestUpdateJournalEntryOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateJournalEntryRequest(entry_id="je_abc", memo="Updated memo")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.update_journal_entry",
        return_value=_make_journal_entry_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_journal_entry_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "update-journal-entry"

  @pytest.mark.asyncio
  async def test_404_when_missing(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      JournalEntryNotFoundError,
    )

    body = UpdateJournalEntryRequest(entry_id="je_missing", memo="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.update_journal_entry",
        side_effect=JournalEntryNotFoundError("je_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_422_when_posted(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      JournalEntryNotDraftError,
    )

    body = UpdateJournalEntryRequest(entry_id="je_posted", memo="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.update_journal_entry",
        side_effect=JournalEntryNotDraftError("je_posted", "posted"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "posted" in exc.value.detail

  @pytest.mark.asyncio
  async def test_409_when_row_locked(self) -> None:
    from robosystems.operations.locking import RowLockedError

    body = UpdateJournalEntryRequest(entry_id="je_abc", memo="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.update_journal_entry",
        side_effect=RowLockedError("Journal entry je_abc is being written"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)
      with pytest.raises(HTTPException) as exc:
        await update_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409


class TestDeleteJournalEntryOp:
  @pytest.mark.asyncio
  async def test_hard_delete_draft(self) -> None:
    body = DeleteJournalEntryRequest(entry_id="je_draft")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.delete_journal_entry",
        return_value={"deleted": True},
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_journal_entry_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "delete-journal-entry"
    assert envelope.result["deleted"] is True

  @pytest.mark.asyncio
  async def test_422_when_posted(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      JournalEntryNotDraftError,
    )

    body = DeleteJournalEntryRequest(entry_id="je_posted")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.delete_journal_entry",
        side_effect=JournalEntryNotDraftError("je_posted", "posted"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await delete_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422

  @pytest.mark.asyncio
  async def test_409_when_row_locked(self) -> None:
    from robosystems.operations.locking import RowLockedError

    body = DeleteJournalEntryRequest(entry_id="je_draft")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.delete_journal_entry",
        side_effect=RowLockedError("Journal entry je_draft is being written"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)
      with pytest.raises(HTTPException) as exc:
        await delete_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 409

  @pytest.mark.asyncio
  async def test_422_when_period_closed(self) -> None:
    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    body = DeleteJournalEntryRequest(entry_id="je_draft")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.delete_journal_entry",
        side_effect=ClosedPeriodError("2026-01", date(2026, 1, 15)),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)
      with pytest.raises(HTTPException) as exc:
        await delete_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "closed period" in exc.value.detail


# TestReverseJournalEntryOp removed: the `reverse-journal-entry`
# OperationSpec was retired in favor of
# `create-event-block(event_type='journal_entry_reversed')`. See
# tests/operations/event_block/python_handlers/test_journal_entry_reversed.py
# for coverage of the event-driven path.


# ── Filing lifecycle ops ────────────────────────────────────────────────────


def _make_filed_report_response() -> ReportResponse:
  """Stand-in for the ReportResponse returned by file_report / transition."""
  return ReportResponse(
    id="rpt_01",
    name="Q1 2026 Statements",
    taxonomy_id="tax_usgaap_reporting",
    generation_status="complete",
    period_type="quarterly",
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    comparative=True,
    created_at=datetime(2026, 4, 1, tzinfo=UTC),
    filing_status="filed",
    filed_at=datetime(2026, 4, 15, tzinfo=UTC),
    filed_by="usr_test123",
  )


def _make_create_report_body() -> CreateReportRequest:
  return CreateReportRequest(
    name="Q1 2026 Financials",
    taxonomy_id="rs-gaap",
    mapping_id="map_test123",
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    period_type="quarterly",
    comparative=True,
  )


def _make_published_report_response() -> ReportResponse:
  return ReportResponse(
    id="rpt_01",
    name="Q1 2026 Financials",
    taxonomy_id="rs-gaap",
    generation_status="published",
    period_type="quarterly",
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    comparative=True,
    created_at=datetime(2026, 4, 1, tzinfo=UTC),
    filing_status="draft",
  )


class TestCreateReportOp:
  """Covers the new bundle-stamping error path on create-report."""

  @pytest.mark.asyncio
  async def test_502_when_bundle_upload_fails(self) -> None:
    """``_stamp_report_bundle`` raises ``BundleUploadError`` when S3 is
    unreachable at publish time, aborting the transaction so the
    invariant "every published Report has a stored bundle" holds.
    The router must translate it to 502, not let it surface as 500."""
    from robosystems.operations.roboledger.commands.reports import BundleUploadError

    body = _make_create_report_body()
    with (
      patch(
        "robosystems.operations.roboledger.commands.reports.create_report",
        side_effect=BundleUploadError(
          "Failed to upload JSON-LD bundle for report rpt_01 to "
          "s3://test-bucket/graph-bundles/kg.../rpt_01/g1.jsonld; aborting publish."
        ),
      ),
      patch("robosystems.db.extensions.extensions_session") as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_report_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 502
    assert "aborting publish" in exc.value.detail

  @pytest.mark.asyncio
  async def test_happy_path_returns_published_report(self) -> None:
    body = _make_create_report_body()
    with (
      patch(
        "robosystems.operations.roboledger.commands.reports.create_report",
        return_value=_make_published_report_response(),
      ),
      patch("robosystems.db.extensions.extensions_session") as mock_session,
      patch("robosystems.middleware.extensions.mark_graph_stale"),
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await create_report_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "create-report"
    assert envelope.result is not None
    assert envelope.result["generation_status"] == "published"


class TestRegenerateReportOp:
  """Covers the new bundle-stamping error path on regenerate-report."""

  @pytest.mark.asyncio
  async def test_502_when_bundle_upload_fails(self) -> None:
    """Same fail-loud semantics as create-report. Without the handler
    a regenerate would surface as 500; the router translates to 502."""
    from robosystems.operations.roboledger.commands.reports import BundleUploadError

    body = RegenerateReportOperation(report_id="rpt_01")
    with (
      patch(
        "robosystems.operations.roboledger.commands.reports.regenerate_report",
        side_effect=BundleUploadError(
          "Failed to upload JSON-LD bundle for report rpt_01 to "
          "s3://test-bucket/graph-bundles/kg.../rpt_01/g2.jsonld; aborting publish."
        ),
      ),
      patch("robosystems.db.extensions.extensions_session") as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await regenerate_report_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 502
    assert "aborting publish" in exc.value.detail


class TestFileReportOp:
  @pytest.mark.asyncio
  async def test_happy_path_wraps_filed_report_in_envelope(self) -> None:
    body = FileReportRequest(report_id="rpt_01")
    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_file_report",
        return_value=_make_filed_report_response(),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await file_report_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "file-report"
    assert envelope.status == "completed"
    assert envelope.result is not None
    assert envelope.result["filing_status"] == "filed"
    assert envelope.result["filed_by"] == "usr_test123"

  @pytest.mark.asyncio
  async def test_404_when_report_missing(self) -> None:
    from robosystems.operations.roboledger.commands.reports import ReportNotFoundError

    body = FileReportRequest(report_id="rpt_missing")
    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_file_report",
        side_effect=ReportNotFoundError("rpt_missing"),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await file_report_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "rpt_missing" in exc.value.detail

  @pytest.mark.asyncio
  async def test_422_when_transition_illegal(self) -> None:
    from robosystems.operations.roboledger.commands.reports import (
      InvalidFilingTransitionError,
    )

    body = FileReportRequest(report_id="rpt_01")
    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_file_report",
        side_effect=InvalidFilingTransitionError(
          "Report 'rpt_01' is in 'archived'; can only file from 'draft' or 'under_review'."
        ),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await file_report_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "archived" in exc.value.detail


class TestTransitionFilingStatusOp:
  @pytest.mark.asyncio
  async def test_happy_path_returns_transitioned_report(self) -> None:
    response = _make_filed_report_response()
    response.filing_status = "under_review"
    body = TransitionFilingStatusRequest(
      report_id="rpt_01", target_status="under_review"
    )
    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_transition_filing_status",
        return_value=response,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await transition_filing_status_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "transition-filing-status"
    assert envelope.result is not None
    assert envelope.result["filing_status"] == "under_review"

  @pytest.mark.asyncio
  async def test_422_when_transition_illegal(self) -> None:
    from robosystems.operations.roboledger.commands.reports import (
      InvalidFilingTransitionError,
    )

    body = TransitionFilingStatusRequest(report_id="rpt_01", target_status="filed")
    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_transition_filing_status",
        side_effect=InvalidFilingTransitionError(
          "Report 'rpt_01' cannot transition from 'under_review' to 'filed'."
        ),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await transition_filing_status_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422


# ═══════════════════════════════════════════════════════════════════════════
# Update Event Block route — the inbox approve flow
#
# Specifically covers the captured/classified → committed transition that
# fires the registered Python handler against captured metadata. The
# handler can raise four error types that all need to surface as 422 so
# the inbox UI can display the failure reason to the user.
# ═══════════════════════════════════════════════════════════════════════════


def _make_event_envelope(status: str = "committed"):
  """Match the EventBlockEnvelope shape returned by update_event_block."""
  from datetime import UTC, datetime

  from robosystems.models.api.event_block import EventBlockEnvelope

  return EventBlockEnvelope(
    id="evt_qb_001",
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    status=status,
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    source="quickbooks",
    external_id="qb_txn_42",
    currency="USD",
    metadata={"qb_txn_type": "Invoice", "entries": []},
    dimension_ids=[],
    created_at=datetime(2026, 3, 31, tzinfo=UTC),
    created_by="usr_test",
  )


class TestUpdateEventBlockOp:
  """Router-level tests for update-event-block — covers the error-map
  contract for handler-firing failures."""

  def _request(self, **overrides):
    from robosystems.models.api.event_block import UpdateEventBlockRequest

    return UpdateEventBlockRequest(
      event_id="evt_qb_001", transition_to="committed", **overrides
    )

  @pytest.mark.asyncio
  async def test_happy_path_returns_envelope(self) -> None:
    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        return_value=_make_event_envelope("committed"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_event_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "update-event-block"
    assert envelope.status == "completed"
    assert envelope.result["status"] == "committed"

  @pytest.mark.asyncio
  async def test_404_when_event_missing(self) -> None:
    from robosystems.operations.event_block.commands import EventNotFoundError

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=EventNotFoundError("evt_qb_001"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_422_when_transition_invalid(self) -> None:
    from robosystems.operations.event_block.commands import (
      InvalidEventTransitionError,
    )

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=InvalidEventTransitionError("captured → fulfilled not allowed"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422

  @pytest.mark.asyncio
  async def test_422_when_handler_metadata_validation_fails(self) -> None:
    """Approve fails because captured metadata doesn't satisfy the
    handler's schema (e.g., missing posting_date). Must surface as 422,
    not 500."""
    from robosystems.operations.event_block.python_handlers.types import (
      HandlerMetadataValidationError,
    )

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=HandlerMetadataValidationError(
          "Event evt_qb_001: posting_date required"
        ),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "posting_date" in exc.value.detail

  @pytest.mark.asyncio
  async def test_422_when_element_resolution_fails(self) -> None:
    """Captured QB metadata references element_external_ids that don't
    exist in the CoA — handler raises ElementResolutionError, must
    surface as 422."""
    from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
      ElementResolutionError,
    )

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=ElementResolutionError(
          "Event evt_qb_001: 2 element_external_id(s) could not be resolved"
        ),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "element_external_id" in exc.value.detail

  @pytest.mark.asyncio
  async def test_422_when_handler_hits_closed_period(self) -> None:
    from datetime import date

    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422

  @pytest.mark.asyncio
  async def test_422_when_handler_hits_unbalanced_entry(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      UnbalancedJournalEntryError,
    )

    body = self._request()
    with (
      patch(
        "robosystems.operations.event_block.commands.update_event_block",
        side_effect=UnbalancedJournalEntryError(total_debit=10000, total_credit=9999),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_event_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "balance" in exc.value.detail


class TestRegistrarWriteRoleGate:
  """Every registrar command handler enforces the member/admin
  write role, so a read-only viewer can't reach the OLTP command surface."""

  @pytest.mark.asyncio
  async def test_viewer_is_denied(self) -> None:
    body = UpdateJournalEntryRequest(entry_id="je_abc", memo="x")
    # Re-patch over the autouse no-op to simulate a read-only viewer.
    with patch(
      "robosystems.middleware.extensions.require_graph_write_role",
      side_effect=HTTPException(
        status_code=403, detail="Write access denied; your role is read-only."
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        await update_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 403


class TestHandWrittenWriteRoleGate:
  """The hand-written `@router.post` ops enforce the same write role the
  registrar does.

  `TestRegistrarWriteRoleGate` above only ever exercised registrar-mounted
  handlers, which enforce inside the handler body. The hand-written ops in
  this module enforce through a *dependency*, so a direct call never resolves
  it — which is precisely why no test in this file could catch the gap.
  """

  def test_every_hand_written_post_requires_the_write_dependency(self) -> None:
    """Structural, not behavioral: asserts the invariant rather than one
    instance, so a newly added hand-written op that forgets the gate fails
    here instead of shipping."""
    from robosystems.routers.extensions.roboledger.operations import (
      _require_roboledger_write,
      router,
    )

    def _dependency_calls(dependant) -> list:
      calls = []
      for dep in dependant.dependencies:
        if dep.call is not None:
          calls.append(dep.call)
        calls.extend(_dependency_calls(dep))
      return calls

    ungated = []
    for route in router.routes:
      if "POST" not in getattr(route, "methods", set()):
        continue
      # Registrar-mounted handlers call `require_graph_write_role` in the
      # handler body rather than via a dependency; they are covered by
      # `TestRegistrarWriteRoleGate`.
      if route.endpoint.__module__ == "robosystems.middleware.extensions":
        continue
      if _require_roboledger_write not in _dependency_calls(route.dependant):
        ungated.append(route.path)

    assert ungated == [], (
      f"hand-written write ops missing the write-role gate: {ungated}"
    )

  def test_write_dependency_denies_a_viewer(self) -> None:
    from robosystems.routers.extensions.roboledger.operations import (
      _require_roboledger_write,
    )

    with patch(
      "robosystems.routers.extensions.roboledger.operations.require_graph_write_role",
      side_effect=HTTPException(
        status_code=403, detail="Write access denied; your role is read-only."
      ),
    ):
      with pytest.raises(HTTPException) as exc:
        _require_roboledger_write(
          graph_id=GRAPH_ID, user=_make_user(), _ext=MagicMock()
        )

    assert exc.value.status_code == 403

  def test_write_dependency_returns_the_user_for_a_writer(self) -> None:
    from robosystems.routers.extensions.roboledger.operations import (
      _require_roboledger_write,
    )

    user = _make_user()
    with patch(
      "robosystems.routers.extensions.roboledger.operations.require_graph_write_role",
      return_value=None,
    ) as gate:
      resolved = _require_roboledger_write(
        graph_id=GRAPH_ID, user=user, _ext=MagicMock()
      )

    assert resolved is user
    gate.assert_called_once_with(str(user.id), GRAPH_ID)


class TestCrossGraphStalenessCallbacks:
  """The `on_fresh_success` hooks on share / revoke / block-with-purge.

  These are driven through the real handler rather than by calling the hook
  with a hand-made object, because the defect they regress lives *between*
  the two: `wrap_completed` runs the command's Pydantic result through
  `model_dump(mode="json")`, so the hook receives a dict. All three hooks
  shipped reading it with `getattr`, which does not raise — it reports every
  field absent, and each hook quietly marked nothing. A test that passed the
  hook a Pydantic model would have passed against the broken code.

  The demo cannot catch this either: it forces a rebuild after sharing.
  """

  @pytest.mark.asyncio
  async def test_share_marks_every_recipient_that_received_a_copy(self) -> None:
    body = ShareReportOperation(report_id="rpt_1", publish_list_id="pl_1")
    result = ShareReportResponse(
      report_id="rpt_1",
      results=[
        ShareResultItem(target_graph_id="kg0000000000000001", status="shared"),
        ShareResultItem(
          target_graph_id="kg0000000000000002",
          status="error",
          error="Recipient has blocked shares from this graph.",
        ),
        ShareResultItem(target_graph_id="kg0000000000000003", status="shared"),
      ],
    )

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_share_report",
        return_value=result,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.mark_graph_stale"
      ) as mark,
    ):
      envelope = await share_report_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.status == "completed"
    # The failed recipient is not marked: nothing landed in their schema.
    assert mark.call_args_list == [
      call("kg0000000000000001", "report_shared_in"),
      call("kg0000000000000003", "report_shared_in"),
    ]

  @pytest.mark.asyncio
  async def test_revoke_marks_the_recipient_when_a_copy_was_deleted(self) -> None:
    body = RevokeReportShareOperation(
      report_id="rpt_1", target_graph_id="kg0000000000000001"
    )
    result = RevokeReportShareResponse(
      report_id="rpt_1",
      target_graph_id="kg0000000000000001",
      revoked_at=datetime(2026, 3, 1, tzinfo=UTC),
      copy_deleted=True,
    )

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_revoke_report_share",
        return_value=result,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.user_is_graph_admin",
        return_value=True,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.mark_graph_stale"
      ) as mark,
    ):
      await revoke_report_share_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    mark.assert_called_once_with("kg0000000000000001", "report_share_revoked")

  @pytest.mark.asyncio
  async def test_revoke_does_not_mark_when_no_copy_was_found(self) -> None:
    """A recipient who already deleted the copy has nothing to re-project."""
    body = RevokeReportShareOperation(
      report_id="rpt_1", target_graph_id="kg0000000000000001"
    )
    result = RevokeReportShareResponse(
      report_id="rpt_1",
      target_graph_id="kg0000000000000001",
      revoked_at=datetime(2026, 3, 1, tzinfo=UTC),
      copy_deleted=False,
    )

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_revoke_report_share",
        return_value=result,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.user_is_graph_admin",
        return_value=True,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.mark_graph_stale"
      ) as mark,
    ):
      await revoke_report_share_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    mark.assert_not_called()

  @staticmethod
  def _block_result(purged: int) -> BlockSourceGraphResult:
    purged_ids = [f"rpt_purged_{n}" for n in range(purged)]
    return BlockSourceGraphResult(
      block=BlockedSourceGraphResponse(
        id="blk_1",
        source_graph_id="kg0000000000000009",
        blocked_by="usr_test123",
        blocked_at=datetime(2026, 3, 1, tzinfo=UTC),
      ),
      already_blocked=False,
      purged_report_count=purged,
      purged_report_ids=purged_ids,
    )

  @pytest.mark.asyncio
  async def test_block_marks_this_graph_when_the_purge_removed_reports(self) -> None:
    body = BlockSourceGraphOperation(source_graph_id="kg0000000000000009", purge=True)

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_block_source_graph",
        return_value=self._block_result(3),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.user_is_graph_admin",
        return_value=True,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
      patch(
        "robosystems.routers.extensions.roboledger.operations.mark_graph_stale"
      ) as mark,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      await block_source_graph_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    mark.assert_called_once_with(GRAPH_ID, "shared_reports_purged")

  @pytest.mark.asyncio
  async def test_purge_withdraws_the_stored_publications_after_the_rows(self) -> None:
    """A purge that deletes rows and leaves the senders' holons in this
    graph's bundle prefix has not withdrawn anything. The cleanup runs from
    the after-success hook so it can only follow a committed deletion."""
    body = BlockSourceGraphOperation(source_graph_id="kg0000000000000009", purge=True)

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_block_source_graph",
        return_value=self._block_result(3),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.user_is_graph_admin",
        return_value=True,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
      patch("robosystems.routers.extensions.roboledger.operations.mark_graph_stale"),
      patch(
        "robosystems.routers.extensions.roboledger.operations.delete_report_artifacts"
      ) as drop,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      await block_source_graph_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    drop.assert_called_once_with(
      GRAPH_ID, ["rpt_purged_0", "rpt_purged_1", "rpt_purged_2"]
    )

  @pytest.mark.asyncio
  async def test_block_without_a_purge_does_not_rebuild(self) -> None:
    """A block on its own removes nothing, so it must not trigger a rebuild."""
    body = BlockSourceGraphOperation(source_graph_id="kg0000000000000009")

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.cmd_block_source_graph",
        return_value=self._block_result(0),
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.user_is_graph_admin",
        return_value=True,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
      patch(
        "robosystems.routers.extensions.roboledger.operations.mark_graph_stale"
      ) as mark,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      await block_source_graph_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    mark.assert_not_called()
