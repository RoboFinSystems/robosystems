"""Information Block GraphQL resolver tests.

Exercises ``informationBlock`` / ``informationBlocks`` via
``schema.execute_sync()`` with mocked reads operations. The resolver is
always-on (no extension gate) and uses ``open_library_session``, so the
session patch target is ``robosystems.db.extensions.extensions_session``.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from robosystems.graphql.schema import schema
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
)

GRAPH_ID = "kg01234567890abcdef"
LIBRARY_GRAPH_ID = "library"

GET_PATH = "robosystems.graphql.resolvers.information_block.get_information_block"
LIST_PATH = "robosystems.graphql.resolvers.information_block.list_information_blocks"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test"
  user.email = "test@example.com"
  return user


def _ctx(
  user: MagicMock | None = None,
  *,
  graph_id: str = GRAPH_ID,
  schema_extensions: tuple[str, ...] = ("roboledger",),
  graph_type: str = "entity",
) -> dict:
  return {
    "request": MagicMock(),
    "user": user or _make_user(),
    "graph_id": graph_id,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


@contextmanager
def _patch_session():
  mock_session = MagicMock()
  mock_ctx_mgr = MagicMock()
  mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
  with patch("robosystems.db.extensions.extensions_session", return_value=mock_ctx_mgr):
    yield mock_session


def _envelope(structure_id: str = "struct_ib_01") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name="Equipment Depreciation",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(mechanics=ScheduleMechanics()),
  )


_IB_FIELDS = (
  "{ id blockType name displayName category informationModel { conceptArrangement } }"
)


class TestInformationBlockQuery:
  def test_returns_envelope_when_found(self) -> None:
    expected = _envelope("struct_ib_01")
    with (
      _patch_session(),
      patch(GET_PATH, return_value=expected),
    ):
      result = schema.execute_sync(
        f'query {{ informationBlock(id: "struct_ib_01") {_IB_FIELDS} }}',
        context_value=_ctx(),
      )
    assert result.errors is None
    data = result.data["informationBlock"]
    assert data["id"] == "struct_ib_01"
    assert data["blockType"] == "schedule"
    assert data["name"] == "Equipment Depreciation"
    assert data["displayName"] == "Schedule"
    assert data["category"] == "Close"
    assert data["informationModel"]["conceptArrangement"] == "roll_forward"

  def test_returns_null_when_not_found(self) -> None:
    with (
      _patch_session(),
      patch(GET_PATH, return_value=None),
    ):
      result = schema.execute_sync(
        'query { informationBlock(id: "struct_missing") { id } }',
        context_value=_ctx(),
      )
    assert result.errors is None
    assert result.data["informationBlock"] is None

  def test_unauthenticated_returns_error(self) -> None:
    result = schema.execute_sync(
      'query { informationBlock(id: "struct_x") { id } }',
      context_value={
        "request": MagicMock(),
        "user": None,
        "graph_id": GRAPH_ID,
        "schema_extensions": (),
        "graph_type": "entity",
      },
    )
    assert result.errors is not None
    assert any("Authentication required" in str(e.message) for e in result.errors)

  def test_works_without_extension_flag(self) -> None:
    """InformationBlock is always-on — no extension gate, unlike entity/schedules."""
    expected = _envelope()
    with (
      _patch_session(),
      patch(GET_PATH, return_value=expected),
    ):
      result = schema.execute_sync(
        f'query {{ informationBlock(id: "struct_ib_01") {_IB_FIELDS} }}',
        context_value=_ctx(schema_extensions=()),
      )
    assert result.errors is None
    assert result.data["informationBlock"]["id"] == "struct_ib_01"


class TestInformationBlocksQuery:
  def test_returns_list_for_tenant_graph(self) -> None:
    envelopes = [_envelope("struct_ib_01"), _envelope("struct_ib_02")]
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=envelopes),
    ):
      result = schema.execute_sync(
        f"query {{ informationBlocks {_IB_FIELDS} }}",
        context_value=_ctx(),
      )
    assert result.errors is None
    items = result.data["informationBlocks"]
    assert len(items) == 2
    assert items[0]["id"] == "struct_ib_01"
    assert items[1]["id"] == "struct_ib_02"

  def test_returns_empty_list_on_library_sentinel(self) -> None:
    """Schedule has surfaces_in_library=False, so the sentinel returns []."""
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=[]) as mock_list,
    ):
      result = schema.execute_sync(
        'query { informationBlocks(blockType: "schedule") { id } }',
        context_value=_ctx(graph_id=LIBRARY_GRAPH_ID),
      )
    assert result.errors is None
    assert result.data["informationBlocks"] == []
    mock_list.assert_called_once()
    _, kwargs = mock_list.call_args
    assert kwargs.get("library_sentinel") is True

  def test_block_type_filter_passed_through(self) -> None:
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=[]) as mock_list,
    ):
      result = schema.execute_sync(
        'query { informationBlocks(blockType: "schedule") { id } }',
        context_value=_ctx(),
      )
    assert result.errors is None
    _, kwargs = mock_list.call_args
    assert kwargs.get("block_type") == "schedule"
    assert kwargs.get("library_sentinel") is False
