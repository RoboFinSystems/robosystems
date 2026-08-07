"""Tests for the update-graph-metadata operation handler and its body model.

The command's own behaviour is covered in
`tests/operations/graph/commands/test_metadata.py`; this file covers the
request-model normalization and the router's envelope contract.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from robosystems.models.api.graphs.operations import (
  GraphMetadataResult,
  UpdateGraphMetadataOp,
)
from robosystems.routers.graphs.operations import update_graph_metadata_op

_CMD = "robosystems.operations.graph.commands.metadata.update_graph_metadata_cmd"


class _FakeCache:
  """In-memory idempotency cache matching the real signature."""

  def __init__(self) -> None:
    self.store: dict = {}

  async def get(
    self, user_id, graph_id, operation_name, idempotency_key, body_fingerprint
  ):
    return self.store.get((user_id, graph_id, operation_name, idempotency_key))

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
    self.store[(user_id, graph_id, operation_name, idempotency_key)] = envelope


def _user() -> MagicMock:
  u = MagicMock()
  u.id = "usr_test"
  return u


def _result(**overrides) -> GraphMetadataResult:
  defaults = {
    "graph_id": "kg1a2b3c4d5",
    "graph_name": "Renamed",
    "description": "",
    "tags": [],
    "updated_fields": ["graph_name"],
  }
  return GraphMetadataResult(**{**defaults, **overrides})


class TestUpdateGraphMetadataOpModel:
  def test_all_fields_optional(self):
    body = UpdateGraphMetadataOp()
    assert body.graph_name is None
    assert body.description is None
    assert body.tags is None

  def test_strips_surrounding_whitespace_from_name(self):
    assert UpdateGraphMetadataOp(graph_name="  Acme  ").graph_name == "Acme"

  def test_rejects_whitespace_only_name(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(graph_name="   ")

  def test_rejects_empty_name(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(graph_name="")

  def test_rejects_overlong_name(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(graph_name="x" * 256)

  def test_rejects_overlong_description(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(description="x" * 1001)

  def test_empty_description_is_allowed_as_a_clear(self):
    assert UpdateGraphMetadataOp(description="").description == ""

  def test_tags_are_trimmed_deduped_and_emptied_out(self):
    body = UpdateGraphMetadataOp(tags=["  alpha ", "alpha", "", "   ", "beta"])
    assert body.tags == ["alpha", "beta"]

  def test_empty_tag_list_is_allowed_as_a_clear(self):
    assert UpdateGraphMetadataOp(tags=[]).tags == []

  def test_rejects_overlong_tag(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(tags=["x" * 51])

  def test_rejects_too_many_tags(self):
    with pytest.raises(ValidationError):
      UpdateGraphMetadataOp(tags=[f"tag{i}" for i in range(21)])


class TestUpdateGraphMetadataOpHandler:
  @pytest.mark.asyncio
  async def test_empty_body_is_400(self):
    with pytest.raises(HTTPException) as exc:
      await update_graph_metadata_op(
        body=UpdateGraphMetadataOp(),
        graph_id="kg1a2b3c4d5",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )
    assert exc.value.status_code == 400
    assert "No fields provided" in exc.value.detail

  @pytest.mark.asyncio
  async def test_returns_completed_envelope_carrying_the_result(self):
    """`wrap_completed` dumps the model, so `result` is a dict at this layer;
    `response_model=OperationEnvelope[GraphMetadataResult]` types it on the
    wire."""
    with patch(_CMD, return_value=_result()) as cmd:
      envelope = await update_graph_metadata_op(
        body=UpdateGraphMetadataOp(graph_name="Renamed"),
        graph_id="kg1a2b3c4d5",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )

    assert envelope.operation == "update-graph-metadata"
    assert envelope.status == "completed"
    assert envelope.created_by == "usr_test"
    assert envelope.operation_id.startswith("op_")
    assert envelope.result["graph_name"] == "Renamed"
    assert envelope.result["updated_fields"] == ["graph_name"]
    cmd.assert_called_once()

  @pytest.mark.asyncio
  async def test_forwards_only_supplied_fields_to_the_command(self):
    with patch(_CMD, return_value=_result()) as cmd:
      await update_graph_metadata_op(
        body=UpdateGraphMetadataOp(description="just this"),
        graph_id="kg1a2b3c4d5",
        user=_user(),
        idempotency_key=None,
        cache=_FakeCache(),
        db=MagicMock(),
      )

    kwargs = cmd.call_args.kwargs
    assert kwargs["graph_name"] is None
    assert kwargs["description"] == "just this"
    assert kwargs["tags"] is None
    assert kwargs["user_id"] == "usr_test"

  @pytest.mark.asyncio
  async def test_idempotency_key_replays_without_rerunning_the_command(self):
    cache = _FakeCache()
    body = UpdateGraphMetadataOp(graph_name="Renamed")

    with patch(_CMD, return_value=_result()) as cmd:
      first = await update_graph_metadata_op(
        body=body,
        graph_id="kg1a2b3c4d5",
        user=_user(),
        idempotency_key="key-123",
        cache=cache,
        db=MagicMock(),
      )
      second = await update_graph_metadata_op(
        body=body,
        graph_id="kg1a2b3c4d5",
        user=_user(),
        idempotency_key="key-123",
        cache=cache,
        db=MagicMock(),
      )

    assert cmd.call_count == 1
    assert second.operation_id == first.operation_id
    assert second.idempotent_replay is True
