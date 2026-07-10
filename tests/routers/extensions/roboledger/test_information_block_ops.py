"""Integration tests for the ``create-information-block`` route handler.

These follow the same direct-call pattern as the rest of
``test_operations.py``: the route handler is called directly (no
TestClient), with the session factory and command patched at their
source modules so the registrar's late-binding picks up the mock.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.operations import OperationEnvelope
from robosystems.models.api.extensions.schedules import EntryTemplateRequest
from robosystems.models.api.information_block import (
  ArtifactResponse,
  CreateInformationBlockRequest,
  DeleteInformationBlockRequest,
  DeleteInformationBlockResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
  UpdateInformationBlockRequest,
)
from robosystems.routers.extensions.roboledger.operations import (
  create_information_block_op,
  delete_information_block_op,
  update_information_block_op,
)

_TEST_ENTRY_TEMPLATE = EntryTemplateRequest(
  debit_element_id="elem_dr", credit_element_id="elem_cr"
)

GRAPH_ID = "kg01234567890abcdef"


@pytest.fixture(autouse=True)
def _bypass_write_role():
  """Registrar handlers enforce the member/admin write role via
  ``require_graph_write_role`` (unit-tested in
  ``tests/middleware/auth/test_dependencies.py``). These direct-call wiring
  tests use a mock user with no DB-backed ``GraphUser`` row, so no-op it."""
  with patch(
    "robosystems.middleware.extensions.require_graph_write_role", return_value=None
  ):
    yield


CMD_PATH = "robosystems.operations.information_block.commands.create_information_block"
UPDATE_CMD_PATH = (
  "robosystems.operations.information_block.commands.update_information_block"
)
DELETE_CMD_PATH = (
  "robosystems.operations.information_block.commands.delete_information_block"
)


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test123"
  return user


def _envelope(structure_id: str = "struct_ib_abc") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name="Equipment Depreciation",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(
      mechanics=ScheduleMechanics(entry_template=_TEST_ENTRY_TEMPLATE)
    ),
  )


def _schedule_payload() -> dict:
  return {
    "name": "Equipment Depreciation",
    "taxonomy_id": None,
    "element_ids": ["elem_a", "elem_b"],
    "period_start": date(2026, 1, 1).isoformat(),
    "period_end": date(2026, 3, 31).isoformat(),
    "monthly_amount": 50000,
    "entry_template": {
      "debit_element_id": "elem_dep",
      "credit_element_id": "elem_accum",
    },
  }


class _FakeCache:
  def __init__(self) -> None:
    self.store: dict = {}

  async def get(self, user_id, graph_id, operation_name, idempotency_key, fingerprint):
    from robosystems.middleware.operations import IdempotencyKeyConflictError

    entry = self.store.get((user_id, graph_id, operation_name, idempotency_key))
    if entry is None:
      return None
    cached_envelope, cached_fp = entry
    if cached_fp != fingerprint:
      raise IdempotencyKeyConflictError(operation_name)
    return cached_envelope

  async def put(
    self,
    user_id,
    graph_id,
    operation_name,
    idempotency_key,
    envelope,
    fingerprint,
    ttl_seconds=86400,
  ):
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = (
      envelope,
      fingerprint,
    )


def _mock_session_ctx():
  return patch("robosystems.db.extensions.extensions_session")


class TestCreateInformationBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path_returns_operation_envelope(self) -> None:
    body = CreateInformationBlockRequest(
      block_type="schedule", payload=_schedule_payload()
    )
    expected = _envelope()

    with (
      patch(CMD_PATH, return_value=expected),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await create_information_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "create-information-block"
    assert envelope.status == "completed"
    assert envelope.operation_id.startswith("op_")
    assert envelope.result is not None
    assert envelope.result["id"] == "struct_ib_abc"
    assert envelope.result["block_type"] == "schedule"
    assert envelope.idempotent_replay is False

  def test_unknown_block_type_rejected_at_construction(self) -> None:
    """Unknown block_type fails the discriminated-union validation
    before the request body even reaches the dispatcher. FastAPI's
    default ValidationError handling still returns 422 on the wire,
    so the contract callers see is unchanged — they just get richer
    field-path errors than the dispatch-time ValueError produced."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
      CreateInformationBlockRequest(block_type="nonsense", payload={})

  @pytest.mark.asyncio
  async def test_statement_block_type_returns_501(self) -> None:
    """Statement block types raise NotImplementedError in dispatch_create
    (pointing callers to create-report). The registrar's error_map routes
    NotImplementedError → 501."""
    from fastapi import HTTPException

    body = CreateInformationBlockRequest(block_type="balance_sheet", payload={})
    with (
      patch(
        CMD_PATH,
        side_effect=NotImplementedError(
          "Statements are generated via create-report, not create-information-block."
        ),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await create_information_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 501
    assert "create-report" in str(exc.value.detail)

  @pytest.mark.asyncio
  async def test_idempotency_replay_returns_cached_envelope(self) -> None:
    """Same Idempotency-Key + same body → second call returns cached
    result without re-running the command."""
    body = CreateInformationBlockRequest(
      block_type="schedule", payload=_schedule_payload()
    )
    expected = _envelope()
    cache = _FakeCache()

    with (
      patch(CMD_PATH, return_value=expected) as mock_cmd,
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      first = await create_information_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="idem-key-1",
        cache=cache,
      )
      second = await create_information_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key="idem-key-1",
        cache=cache,
      )

    assert mock_cmd.call_count == 1
    assert first.operation_id == second.operation_id
    assert second.idempotent_replay is True


class TestUpdateInformationBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path_returns_envelope(self) -> None:
    body = UpdateInformationBlockRequest(
      block_type="schedule",
      payload={"structure_id": "struct_ib_abc", "name": "Renamed"},
    )
    expected = _envelope("struct_ib_abc")

    with (
      patch(UPDATE_CMD_PATH, return_value=expected),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await update_information_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "update-information-block"
    assert envelope.status == "completed"
    assert envelope.result is not None
    assert envelope.result["id"] == "struct_ib_abc"

  @pytest.mark.asyncio
  async def test_statement_block_type_returns_501(self) -> None:
    from fastapi import HTTPException

    body = UpdateInformationBlockRequest(block_type="balance_sheet", payload={})
    with (
      patch(
        UPDATE_CMD_PATH,
        side_effect=NotImplementedError(
          "Statement blocks are library-seeded and immutable."
        ),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await update_information_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 501


class TestDeleteInformationBlockOp:
  @pytest.mark.asyncio
  async def test_happy_path_returns_deleted_confirmation(self) -> None:
    body = DeleteInformationBlockRequest(
      block_type="schedule", payload={"structure_id": "struct_ib_abc"}
    )
    expected = DeleteInformationBlockResponse(
      deleted=True,
      structure_id="struct_ib_abc",
      block_type="schedule",
      name="Equipment Depreciation",
    )

    with (
      patch(DELETE_CMD_PATH, return_value=expected),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      envelope = await delete_information_block_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert isinstance(envelope, OperationEnvelope)
    assert envelope.operation == "delete-information-block"
    assert envelope.status == "completed"
    assert envelope.result is not None
    assert envelope.result["deleted"] is True
    assert envelope.result["structure_id"] == "struct_ib_abc"

  @pytest.mark.asyncio
  async def test_statement_block_type_returns_501(self) -> None:
    from fastapi import HTTPException

    body = DeleteInformationBlockRequest(block_type="income_statement", payload={})
    with (
      patch(
        DELETE_CMD_PATH,
        side_effect=NotImplementedError(
          "Statement blocks are library-seeded and cannot be deleted."
        ),
      ),
      _mock_session_ctx() as mock_session,
    ):
      mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
      mock_session.return_value.__exit__ = MagicMock(return_value=False)

      with pytest.raises(HTTPException) as exc:
        await delete_information_block_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 501
