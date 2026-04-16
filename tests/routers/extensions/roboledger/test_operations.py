"""Tests for the roboledger operation routes.

Direct-call tests (no FastAPI TestClient) matching the pattern used in
`tests/routers/ledger/test_entity.py`. The dispatch mechanics are
tested in `tests/middleware/test_extensions.py` — these tests verify
that each route function wires the right command + request model +
envelope shape.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.extensions import OperationEnvelope
from robosystems.models.api.extensions.entity import (
  LedgerEntityResponse,
  UpdateEntityRequest,
)
from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  DeleteJournalEntryRequest,
  JournalEntryLineItemInput,
  JournalEntryLineItemResponse,
  JournalEntryResponse,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.models.api.extensions.schedules import (
  DeleteScheduleRequest,
  ScheduleCreatedResponse,
  UpdateScheduleRequest,
)
from robosystems.models.api.extensions.taxonomies import (
  AssociationResponse,
  BulkAssociationItem,
  BulkCreateAssociationsRequest,
  BulkCreateAssociationsResponse,
  CreateElementRequest,
  DeleteAssociationRequest,
  DeleteElementRequest,
  DeleteStructureRequest,
  DeleteTaxonomyRequest,
  ElementResponse,
  StructureResponse,
  TaxonomyResponse,
  UpdateAssociationRequest,
  UpdateElementRequest,
  UpdateStructureRequest,
  UpdateTaxonomyRequest,
)
from robosystems.routers.extensions.roboledger.operations import (
  AutoMapElementsOperation,
  auto_map_elements_op,
  create_associations_op,
  create_element_op,
  create_journal_entry_op,
  delete_association_op,
  delete_element_op,
  delete_journal_entry_op,
  delete_schedule_op,
  delete_structure_op,
  delete_taxonomy_op,
  reverse_journal_entry_op,
  update_association_op,
  update_element_op,
  update_entity_op,
  update_journal_entry_op,
  update_schedule_op,
  update_structure_op,
  update_taxonomy_op,
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


# ═══════════════════════════════════════════════════════════════════════════
# Taxonomy CRUD route tests (native-accounting surface)
# ═══════════════════════════════════════════════════════════════════════════


def _make_taxonomy_response(taxonomy_id: str = "tax_abc") -> TaxonomyResponse:
  return TaxonomyResponse(
    id=taxonomy_id,
    name="Cascade CoA",
    description=None,
    taxonomy_type="chart_of_accounts",
    version=None,
    standard=None,
    namespace_uri=None,
    is_shared=False,
    is_active=True,
    is_locked=False,
  )


def _make_structure_response(
  structure_id: str = "struct_abc",
  is_active: bool = True,
) -> StructureResponse:
  return StructureResponse(
    id=structure_id,
    name="Balance Sheet",
    description=None,
    structure_type="balance_sheet",
    taxonomy_id="tax_abc",
    is_active=is_active,
  )


def _make_element_response(
  element_id: str = "elem_cash",
  is_active: bool = True,
) -> ElementResponse:
  return ElementResponse(
    id=element_id,
    code="1000",
    name="Cash",
    classification="asset",
    balance_type="debit",
    period_type="instant",
    is_abstract=False,
    element_type="concept",
    source="native",
    taxonomy_id="tax_abc",
    parent_id=None,
    depth=0,
    is_active=is_active,
  )


def _make_association_response(
  association_id: str = "assoc_abc",
) -> AssociationResponse:
  return AssociationResponse(
    id=association_id,
    structure_id="struct_abc",
    from_element_id="elem_parent",
    to_element_id="elem_child",
    association_type="presentation",
    order_value=0.0,
  )


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


class TestUpdateTaxonomyOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateTaxonomyRequest(taxonomy_id="tax_abc", name="Updated Name")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.update_taxonomy",
        return_value=_make_taxonomy_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_taxonomy_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "update-taxonomy"
    assert envelope.status == "completed"
    assert envelope.result is not None
    assert envelope.result["id"] == "tax_abc"

  @pytest.mark.asyncio
  async def test_404_when_taxonomy_missing(self) -> None:
    from robosystems.operations.roboledger.commands.taxonomies import (
      TaxonomyNotFoundError,
    )

    body = UpdateTaxonomyRequest(taxonomy_id="tax_missing", name="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.update_taxonomy",
        side_effect=TaxonomyNotFoundError("tax_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_taxonomy_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
    assert "tax_missing" in exc.value.detail


class TestDeleteTaxonomyOp:
  @pytest.mark.asyncio
  async def test_soft_delete_returns_inactive_response(self) -> None:
    body = DeleteTaxonomyRequest(taxonomy_id="tax_abc")
    inactive = _make_taxonomy_response()
    inactive.is_active = False

    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.delete_taxonomy",
        return_value=inactive,
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_taxonomy_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "delete-taxonomy"
    assert envelope.result["is_active"] is False


class TestUpdateStructureOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateStructureRequest(structure_id="struct_abc", description="Updated")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.update_structure",
        return_value=_make_structure_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_structure_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "update-structure"
    assert envelope.result["id"] == "struct_abc"

  @pytest.mark.asyncio
  async def test_404_when_structure_missing(self) -> None:
    from robosystems.operations.roboledger.commands.taxonomies import (
      StructureNotFoundError,
    )

    body = UpdateStructureRequest(structure_id="struct_missing", name="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.update_structure",
        side_effect=StructureNotFoundError("struct_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_structure_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


class TestDeleteStructureOp:
  @pytest.mark.asyncio
  async def test_soft_delete(self) -> None:
    body = DeleteStructureRequest(structure_id="struct_abc")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.delete_structure",
        return_value=_make_structure_response(is_active=False),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_structure_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.result["is_active"] is False


# ═══════════════════════════════════════════════════════════════════════════
# Element CRUD route tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCreateElementOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreateElementRequest(
      taxonomy_id="tax_abc",
      code="1000",
      name="Cash",
      classification="asset",
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.create_element",
        return_value=_make_element_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await create_element_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "create-element"
    assert envelope.result["code"] == "1000"

  @pytest.mark.asyncio
  async def test_404_when_taxonomy_missing(self) -> None:
    from robosystems.operations.roboledger.commands.taxonomies import (
      TaxonomyNotFoundError,
    )

    body = CreateElementRequest(
      taxonomy_id="tax_missing",
      name="Cash",
      classification="asset",
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.create_element",
        side_effect=TaxonomyNotFoundError("tax_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_element_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404

  @pytest.mark.asyncio
  async def test_400_when_parent_missing(self) -> None:
    from robosystems.operations.roboledger.commands.elements import (
      ElementNotFoundError as ElementMissingError,
    )

    body = CreateElementRequest(
      taxonomy_id="tax_abc",
      name="Cash",
      classification="asset",
      parent_id="elem_nonexistent",
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.create_element",
        side_effect=ElementMissingError("elem_nonexistent"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_element_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 400
    assert "Parent element" in exc.value.detail


class TestUpdateElementOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateElementRequest(element_id="elem_cash", name="Cash and Equivalents")
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.update_element",
        return_value=_make_element_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_element_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "update-element"

  @pytest.mark.asyncio
  async def test_422_on_cycle(self) -> None:
    from robosystems.operations.roboledger.commands.elements import (
      ElementCycleError,
    )

    body = UpdateElementRequest(element_id="elem_cash", parent_id="elem_child_of_cash")
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.update_element",
        side_effect=ElementCycleError("cycle"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_element_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422


class TestDeleteElementOp:
  @pytest.mark.asyncio
  async def test_soft_delete(self) -> None:
    body = DeleteElementRequest(element_id="elem_cash")
    with (
      patch(
        "robosystems.operations.roboledger.commands.elements.delete_element",
        return_value=_make_element_response(is_active=False),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_element_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.result["is_active"] is False


# ═══════════════════════════════════════════════════════════════════════════
# Association bulk route tests
# ═══════════════════════════════════════════════════════════════════════════


class TestCreateAssociationsOp:
  @pytest.mark.asyncio
  async def test_bulk_happy_path(self) -> None:
    body = BulkCreateAssociationsRequest(
      structure_id="struct_abc",
      associations=[
        BulkAssociationItem(
          from_element_id="elem_parent",
          to_element_id="elem_child1",
          association_type="presentation",
          order_value=1.0,
        ),
        BulkAssociationItem(
          from_element_id="elem_parent",
          to_element_id="elem_child2",
          association_type="presentation",
          order_value=2.0,
        ),
      ],
    )
    result = BulkCreateAssociationsResponse(
      structure_id="struct_abc",
      created=2,
      association_ids=["assoc_1", "assoc_2"],
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.bulk_create_associations",
        return_value=result,
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await create_associations_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "create-associations"
    assert envelope.result["created"] == 2
    assert envelope.result["association_ids"] == ["assoc_1", "assoc_2"]

  @pytest.mark.asyncio
  async def test_400_when_element_missing(self) -> None:
    from robosystems.operations.roboledger.commands.taxonomies import (
      ElementNotFoundError,
    )

    body = BulkCreateAssociationsRequest(
      structure_id="struct_abc",
      associations=[
        BulkAssociationItem(
          from_element_id="elem_bogus",
          to_element_id="elem_child",
        )
      ],
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.bulk_create_associations",
        side_effect=ElementNotFoundError("source", "elem_bogus"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_associations_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 400
    assert "Source" in exc.value.detail


class TestUpdateAssociationOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateAssociationRequest(association_id="assoc_abc", order_value=5.0)
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.update_association",
        return_value=_make_association_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_association_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "update-association"


class TestDeleteAssociationOp:
  @pytest.mark.asyncio
  async def test_hard_delete(self) -> None:
    body = DeleteAssociationRequest(association_id="assoc_abc")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.delete_association",
        return_value={"deleted": True},
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_association_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "delete-association"
    assert envelope.result["deleted"] is True

  @pytest.mark.asyncio
  async def test_404_when_association_missing(self) -> None:
    from robosystems.operations.roboledger.commands.taxonomies import (
      AssociationNotFoundError,
    )

    body = DeleteAssociationRequest(association_id="assoc_missing")
    with (
      patch(
        "robosystems.operations.roboledger.commands.taxonomies.delete_association",
        side_effect=AssociationNotFoundError("assoc_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await delete_association_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


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


class TestCreateJournalEntryOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = CreateJournalEntryRequest(
      posting_date=date(2026, 3, 31),
      memo="Record cash sale",
      line_items=_balanced_lines(),
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.create_journal_entry",
        return_value=_make_journal_entry_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await create_journal_entry_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "create-journal-entry"
    assert envelope.result["id"] == "je_abc"
    assert envelope.result["status"] == "draft"
    assert envelope.result["total_debit"] == 10000
    assert envelope.result["total_credit"] == 10000

  @pytest.mark.asyncio
  async def test_422_on_unbalanced(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      UnbalancedJournalEntryError,
    )

    body = CreateJournalEntryRequest(
      posting_date=date(2026, 3, 31),
      memo="Unbalanced",
      line_items=_balanced_lines(),
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.create_journal_entry",
        side_effect=UnbalancedJournalEntryError(10000, 5000),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "balance" in exc.value.detail.lower()


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


class TestReverseJournalEntryOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = ReverseJournalEntryRequest(entry_id="je_posted")
    reversal = _make_journal_entry_response(
      entry_id="je_reversal",
      status="posted",
      reversal_of="je_posted",
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.reverse_journal_entry",
        return_value=reversal,
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await reverse_journal_entry_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "reverse-journal-entry"
    assert envelope.result["reversal_of"] == "je_posted"
    assert envelope.result["status"] == "posted"

  @pytest.mark.asyncio
  async def test_422_when_draft(self) -> None:
    from robosystems.operations.roboledger.commands.journal_entries import (
      JournalEntryNotPostedError,
    )

    body = ReverseJournalEntryRequest(entry_id="je_draft")
    with (
      patch(
        "robosystems.operations.roboledger.commands.journal_entries.reverse_journal_entry",
        side_effect=JournalEntryNotPostedError("je_draft", "draft"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await reverse_journal_entry_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 422
    assert "posted" in exc.value.detail


# ═══════════════════════════════════════════════════════════════════════════
# Schedule update/delete route tests
# ═══════════════════════════════════════════════════════════════════════════


def _make_schedule_response(
  structure_id: str = "struct_sched_abc",
) -> ScheduleCreatedResponse:
  return ScheduleCreatedResponse(
    structure_id=structure_id,
    name="Computer Depreciation",
    taxonomy_id="tax_schedules",
    total_periods=36,
    total_facts=72,
  )


class TestUpdateScheduleOp:
  @pytest.mark.asyncio
  async def test_happy_path(self) -> None:
    body = UpdateScheduleRequest(
      structure_id="struct_sched_abc", name="Renamed Schedule"
    )
    with (
      patch(
        "robosystems.operations.roboledger.commands.schedules.update_schedule",
        return_value=_make_schedule_response(),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_schedule_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "update-schedule"
    assert envelope.result["structure_id"] == "struct_sched_abc"

  @pytest.mark.asyncio
  async def test_404_when_schedule_missing(self) -> None:
    from robosystems.operations.roboledger.commands.schedules import (
      ScheduleNotFoundError,
    )

    body = UpdateScheduleRequest(structure_id="struct_missing", name="X")
    with (
      patch(
        "robosystems.operations.roboledger.commands.schedules.update_schedule",
        side_effect=ScheduleNotFoundError("struct_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_schedule_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404


class TestDeleteScheduleOp:
  @pytest.mark.asyncio
  async def test_cascade_delete(self) -> None:
    body = DeleteScheduleRequest(structure_id="struct_sched_abc")
    with (
      patch(
        "robosystems.operations.roboledger.commands.schedules.delete_schedule",
        return_value={"deleted": True},
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_schedule_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )
    assert envelope.operation == "delete-schedule"
    assert envelope.result["deleted"] is True

  @pytest.mark.asyncio
  async def test_404_when_schedule_missing(self) -> None:
    from robosystems.operations.roboledger.commands.schedules import (
      ScheduleNotFoundError,
    )

    body = DeleteScheduleRequest(structure_id="struct_missing")
    with (
      patch(
        "robosystems.operations.roboledger.commands.schedules.delete_schedule",
        side_effect=ScheduleNotFoundError("struct_missing"),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await delete_schedule_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc.value.status_code == 404
