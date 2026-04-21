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

from robosystems.models.api.information_block import (
  ArtifactResponse,
  CreateInformationBlockRequest,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
)
from robosystems.operations.information_block.commands import (
  create_information_block,
)
from robosystems.operations.information_block.registry import SCHEDULE_BLOCK

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
    artifact=ArtifactResponse(mechanics=ScheduleMechanics()),
  )


def _schedule_entry_with(mock_create: MagicMock, mock_build: MagicMock):
  """Produce a patched Schedule entry with the given handlers."""
  return dataclasses.replace(
    SCHEDULE_BLOCK,
    dispatch_create=mock_create,
    dispatch_build_envelope=mock_build,
  )


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
