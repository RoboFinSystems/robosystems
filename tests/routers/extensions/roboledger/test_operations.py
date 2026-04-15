"""Tests for the roboledger operation routes.

Direct-call tests (no FastAPI TestClient) matching the pattern used in
`tests/routers/ledger/test_entity.py`. The dispatch mechanics are
tested in `tests/middleware/test_extensions.py` — these tests verify
that each route function wires the right command + request model +
envelope shape.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.extensions import OperationEnvelope
from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.routers.extensions.roboledger.operations import (
  AutoMapElementsOperation,
  auto_map_elements_op,
  update_entity_op,
)

GRAPH_ID = "kg01234567890abcdef"


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
  """In-memory idempotency cache matching the real signature.

  Mirrors `IdempotencyCache.get/put` so route-handler tests can
  exercise the (user, graph, operation, key, fingerprint) tuple.
  """

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    from robosystems.middleware.extensions import IdempotencyKeyConflictError

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


class TestUpdateEntityOp:
  @pytest.mark.asyncio
  async def test_happy_path_wraps_result_in_envelope(self) -> None:
    body = UpdateEntityRequest(name="New Name", phone="555-1234")
    entity_resp = _make_entity_response()

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.update_parent_entity",
        return_value=entity_resp,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
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

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.update_parent_entity",
        return_value=None,
      ),
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
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
      "robosystems.routers.extensions.roboledger.operations.extensions_session",
      side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
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
  async def test_idempotency_key_cached_replay(self) -> None:
    body = UpdateEntityRequest(name="Same Name")
    entity_resp = _make_entity_response()
    cache = _FakeCache()

    with (
      patch(
        "robosystems.routers.extensions.roboledger.operations.update_parent_entity",
        return_value=entity_resp,
      ) as mock_cmd,
      patch(
        "robosystems.routers.extensions.roboledger.operations.extensions_session"
      ) as mock_session,
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
