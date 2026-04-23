"""Tests for the generic ``create_information_block`` command dispatcher.

The registry entries are ``@dataclass(frozen=True)`` instances that
capture their handler function references at import time, so patching
the handler module's attribute has no effect — the dataclass still
holds the original function object. Tests that need to intercept a
handler swap the entry via ``dataclasses.replace`` + ``patch.dict`` on
the registry mapping.
"""

from __future__ import annotations

import dataclasses
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

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
from robosystems.operations.information_block.commands import (
  create_information_block,
  delete_information_block,
  update_information_block,
)
from robosystems.operations.information_block.registry import SCHEDULE_BLOCK

_TEST_ENTRY_TEMPLATE = EntryTemplateRequest(
  debit_element_id="elem_dr", credit_element_id="elem_cr"
)

REGISTRY_PATH = "robosystems.operations.information_block.registry.REGISTRY"


def _schedule_payload() -> dict:
  return {
    "name": "Test Depreciation",
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


def _minimal_envelope(structure_id: str = "struct_new") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name="Test",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(
      mechanics=ScheduleMechanics(entry_template=_TEST_ENTRY_TEMPLATE)
    ),
  )


def _schedule_entry_with(
  mock_create: MagicMock | None = None,
  mock_build: MagicMock | None = None,
  mock_update: MagicMock | None = None,
  mock_delete: MagicMock | None = None,
):
  """Produce a patched Schedule entry with the given handlers."""
  kwargs = {}
  if mock_create is not None:
    kwargs["dispatch_create"] = mock_create
  if mock_build is not None:
    kwargs["dispatch_build_envelope"] = mock_build
  if mock_update is not None:
    kwargs["dispatch_update"] = mock_update
  if mock_delete is not None:
    kwargs["dispatch_delete"] = mock_delete
  return dataclasses.replace(SCHEDULE_BLOCK, **kwargs)


class TestCreateInformationBlock:
  def test_dispatches_to_schedule_handler_and_returns_envelope(self) -> None:
    session = MagicMock()
    body = CreateInformationBlockRequest(
      block_type="schedule", payload=_schedule_payload()
    )
    expected = _minimal_envelope("struct_new")
    mock_create = MagicMock(return_value="struct_new")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with(mock_create, mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = create_information_block(session, body, created_by="usr_1")

    assert result is expected
    mock_create.assert_called_once()
    args, _ = mock_create.call_args
    # Handler receives (session, typed_payload, created_by)
    assert args[0] is session
    assert args[2] == "usr_1"
    mock_build.assert_called_once_with(session, "struct_new")

  def test_unknown_block_type_raises_value_error(self) -> None:
    """Unknown block_type should surface as ValueError (→ 422 via error_map)."""
    session = MagicMock()
    body = CreateInformationBlockRequest(block_type="nonsense", payload={})
    with pytest.raises(ValueError, match="Unknown block_type"):
      create_information_block(session, body, created_by="usr_1")

  def test_malformed_payload_raises_validation_error(self) -> None:
    """A payload that fails the type-specific schema surfaces as
    Pydantic ValidationError (→ 422 at the FastAPI boundary)."""
    session = MagicMock()
    # Missing required fields — element_ids, period_start, etc.
    body = CreateInformationBlockRequest(
      block_type="schedule", payload={"name": "Broken"}
    )
    with pytest.raises(ValidationError):
      create_information_block(session, body, created_by="usr_1")

  def test_handler_returning_none_envelope_raises_runtime_error(self) -> None:
    """dispatch_build_envelope returning None for a freshly-created block
    signals a bug in the handler — we want it loud, not silent."""
    session = MagicMock()
    body = CreateInformationBlockRequest(
      block_type="schedule", payload=_schedule_payload()
    )
    patched = _schedule_entry_with(
      MagicMock(return_value="struct_new"),
      MagicMock(return_value=None),
    )
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      with pytest.raises(RuntimeError, match="dispatch_build_envelope returned None"):
        create_information_block(session, body, created_by="usr_1")

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_statement_block_type_raises_not_implemented(self, block_type: str) -> None:
    """Statement block types raise NotImplementedError in dispatch_create —
    the registrar's error_map translates this to HTTP 501. Statements are
    generated via ``create-report``, not ``create-information-block``."""
    session = MagicMock()
    body = CreateInformationBlockRequest(block_type=block_type, payload={})
    with pytest.raises(NotImplementedError, match="create-report"):
      create_information_block(session, body, created_by="usr_1")


def _update_payload(structure_id: str = "struct_existing") -> dict:
  return {
    "structure_id": structure_id,
    "name": "Renamed Schedule",
  }


def _delete_payload(structure_id: str = "struct_existing") -> dict:
  return {"structure_id": structure_id}


class TestUpdateInformationBlock:
  def test_dispatches_to_schedule_handler_and_returns_envelope(self) -> None:
    session = MagicMock()
    body = UpdateInformationBlockRequest(
      block_type="schedule", payload=_update_payload("struct_existing")
    )
    expected = _minimal_envelope("struct_existing")
    mock_update = MagicMock(return_value="struct_existing")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with(mock_update=mock_update, mock_build=mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = update_information_block(session, body, updated_by="usr_1")

    assert result is expected
    mock_update.assert_called_once()
    args, _ = mock_update.call_args
    assert args[0] is session
    assert args[2] == "usr_1"
    mock_build.assert_called_once_with(session, "struct_existing")

  def test_unknown_block_type_raises_value_error(self) -> None:
    session = MagicMock()
    body = UpdateInformationBlockRequest(block_type="nonsense", payload={})
    with pytest.raises(ValueError, match="Unknown block_type"):
      update_information_block(session, body, updated_by="usr_1")

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_statement_block_type_raises_not_implemented(self, block_type: str) -> None:
    session = MagicMock()
    body = UpdateInformationBlockRequest(block_type=block_type, payload={})
    with pytest.raises(NotImplementedError, match="library-seeded"):
      update_information_block(session, body, updated_by="usr_1")


class TestDeleteInformationBlock:
  def test_dispatches_and_returns_confirmation(self) -> None:
    session = MagicMock()
    body = DeleteInformationBlockRequest(
      block_type="schedule", payload=_delete_payload("struct_gone")
    )
    envelope_before = _minimal_envelope("struct_gone")
    envelope_before.name = "Equipment Depreciation"  # captured pre-delete
    mock_build = MagicMock(return_value=envelope_before)
    mock_delete = MagicMock(return_value="struct_gone")
    patched = _schedule_entry_with(mock_build=mock_build, mock_delete=mock_delete)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = delete_information_block(session, body, created_by="usr_1")

    assert isinstance(result, DeleteInformationBlockResponse)
    assert result.deleted is True
    assert result.structure_id == "struct_gone"
    assert result.block_type == "schedule"
    assert result.name == "Equipment Depreciation"
    # Envelope built BEFORE delete so name survives the deletion.
    mock_build.assert_called_once_with(session, "struct_gone")
    mock_delete.assert_called_once()

  def test_missing_pre_delete_envelope_surfaces_domain_not_found(self) -> None:
    """If build_envelope returns None pre-delete (e.g. the payload points
    at a stale id that delete_schedule will reject), the command still
    calls dispatch_delete so the domain's not-found exception surfaces
    to the caller instead of a silent no-op."""
    from robosystems.operations.roboledger.commands.schedules import (
      ScheduleNotFoundError,
    )

    session = MagicMock()
    body = DeleteInformationBlockRequest(
      block_type="schedule", payload=_delete_payload("struct_missing")
    )
    mock_build = MagicMock(return_value=None)
    mock_delete = MagicMock(side_effect=ScheduleNotFoundError("struct_missing"))
    patched = _schedule_entry_with(mock_build=mock_build, mock_delete=mock_delete)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      with pytest.raises(ScheduleNotFoundError):
        delete_information_block(session, body, created_by="usr_1")

  def test_unknown_block_type_raises_value_error(self) -> None:
    session = MagicMock()
    body = DeleteInformationBlockRequest(block_type="nonsense", payload={})
    with pytest.raises(ValueError, match="Unknown block_type"):
      delete_information_block(session, body, created_by="usr_1")

  @pytest.mark.parametrize(
    "block_type",
    ["balance_sheet", "income_statement", "cash_flow_statement", "equity_statement"],
  )
  def test_statement_block_type_raises_not_implemented(self, block_type: str) -> None:
    session = MagicMock()
    body = DeleteInformationBlockRequest(block_type=block_type, payload={})
    with pytest.raises(NotImplementedError, match="library-seeded"):
      delete_information_block(session, body, created_by="usr_1")
