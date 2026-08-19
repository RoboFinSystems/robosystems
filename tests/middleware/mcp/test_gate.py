"""Tests for the MCP extension-gate helpers.

`require_graph_extension_mcp` and `require_not_repository_mcp` are the
call-time equivalents of the REST `require_graph_extension` dependency.
Every MCP write tool goes through one of them, and the error codes
(`repository_write_forbidden`, `extension_not_provisioned`,
`access_denied`) are the stable wire contract that agents branch on.

These tests cover the helper functions directly — integration coverage
(the gate firing through a real tool call) lives in the per-tool test
files.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.middleware.extensions import GraphExtensionContext
from robosystems.middleware.mcp.tools._gate import (
  MCPExtensionGateError,
  require_graph_extension_mcp,
  require_not_repository_mcp,
)


def _entity_meta(
  extensions=("roboledger",), graph_type="entity"
) -> GraphExtensionContext:
  return GraphExtensionContext(
    graph_type=graph_type,
    schema_extensions=tuple(extensions),
    is_repository=False,
  )


def _repo_meta() -> GraphExtensionContext:
  return GraphExtensionContext(
    graph_type="repository",
    schema_extensions=("roboledger",),
    is_repository=True,
  )


# ── require_graph_extension_mcp ───────────────────────────────────────────


class TestRequireGraphExtensionMCP:
  def test_happy_path_with_pre_loaded_meta(self) -> None:
    """When the manager pre-loads metadata via `meta_getter`, the gate
    must accept it without opening a new DB session."""
    result = require_graph_extension_mcp(
      extension="roboledger",
      graph_id="kg01ABC",
      meta=_entity_meta(),
    )
    assert result.graph_type == "entity"
    assert "roboledger" in result.schema_extensions

  def test_rejects_subgraph_ids_before_loading_metadata(self) -> None:
    """A subgraph has no tenant schema. The REST extension dependency and the
    GraphQL endpoint refuse it up front; the MCP gate must too, or a write
    tool called with a subgraph id fails inside the tool as a 404 or a 500."""
    with patch(
      "robosystems.middleware.mcp.tools._gate._load_with_short_lived_session"
    ) as loader:
      with pytest.raises(MCPExtensionGateError) as exc:
        require_graph_extension_mcp(
          extension="roboledger",
          graph_id="kg0123456789abcdef01_dev",
        )
    assert exc.value.code == "subgraph_not_addressable"
    assert "target the parent graph" in exc.value.message
    loader.assert_not_called()

  def test_rejects_repository_graph(self) -> None:
    with pytest.raises(MCPExtensionGateError) as exc:
      require_graph_extension_mcp(
        extension="roboledger",
        graph_id="sec",
        meta=_repo_meta(),
      )
    assert exc.value.code == "repository_write_forbidden"
    assert "roboledger" in exc.value.message

  def test_rejects_missing_extension(self) -> None:
    with pytest.raises(MCPExtensionGateError) as exc:
      require_graph_extension_mcp(
        extension="roboinvestor",
        graph_id="kg01ABC",
        meta=_entity_meta(extensions=("roboledger",)),
      )
    assert exc.value.code == "extension_not_provisioned"
    assert "roboinvestor" in exc.value.message

  def test_rejects_repo_before_missing_extension(self) -> None:
    """If a graph is a repo AND missing the extension, the repo code
    wins — matches REST message ordering."""
    repo_without_ext = GraphExtensionContext(
      graph_type="repository",
      schema_extensions=(),  # no extensions
      is_repository=True,
    )
    with pytest.raises(MCPExtensionGateError) as exc:
      require_graph_extension_mcp(
        extension="roboledger",
        graph_id="sec",
        meta=repo_without_ext,
      )
    assert exc.value.code == "repository_write_forbidden"

  def test_load_failure_translates_to_access_denied(self) -> None:
    """When `meta=None`, the gate opens its own DB session. If the
    platform `load_graph_metadata` raises (e.g., 403 on missing graph),
    the gate must translate to the MCP-native `access_denied` code so
    callers get a typed error envelope, not an HTTPException."""
    with patch(
      "robosystems.middleware.mcp.tools._gate._load_with_short_lived_session",
      side_effect=RuntimeError("graph not found"),
    ):
      with pytest.raises(MCPExtensionGateError) as exc:
        require_graph_extension_mcp(
          extension="roboledger",
          graph_id="kg_missing",
          meta=None,
        )
    assert exc.value.code == "access_denied"
    assert "kg_missing" in exc.value.message

  def test_loads_meta_when_not_provided(self) -> None:
    """No `meta` arg → gate fetches via the short-lived session helper."""
    loaded = _entity_meta()
    with patch(
      "robosystems.middleware.mcp.tools._gate._load_with_short_lived_session",
      return_value=loaded,
    ) as loader:
      result = require_graph_extension_mcp(
        extension="roboledger",
        graph_id="kg01ABC",
        meta=None,
      )
    loader.assert_called_once_with("kg01ABC")
    assert result is loaded


# ── require_not_repository_mcp ────────────────────────────────────────────


class TestRequireNotRepositoryMCP:
  """Subset gate for tools that aren't extension-bound but still must
  reject repos (e.g., document / memory writes)."""

  def test_allows_entity_graph(self) -> None:
    meta = _entity_meta(extensions=())  # no extensions, still not a repo
    result = require_not_repository_mcp(graph_id="kg01ABC", meta=meta)
    assert result is meta

  def test_rejects_repository(self) -> None:
    with pytest.raises(MCPExtensionGateError) as exc:
      require_not_repository_mcp(graph_id="sec", meta=_repo_meta())
    assert exc.value.code == "repository_write_forbidden"

  def test_loads_meta_when_not_provided(self) -> None:
    loaded = _entity_meta()
    with patch(
      "robosystems.middleware.mcp.tools._gate._load_with_short_lived_session",
      return_value=loaded,
    ):
      result = require_not_repository_mcp(graph_id="kg_01", meta=None)
    assert result is loaded


# ── MCPExtensionGateError ─────────────────────────────────────────────────


class TestMCPExtensionGateError:
  def test_carries_code_and_message_as_attrs(self) -> None:
    exc = MCPExtensionGateError("custom_code", "readable reason")
    assert exc.code == "custom_code"
    assert exc.message == "readable reason"
    # str(exc) matches message so logging/traceback doesn't lose info.
    assert str(exc) == "readable reason"


# ── _load_with_short_lived_session ────────────────────────────────────────


class TestLoadWithShortLivedSession:
  """The helper opens an INDEPENDENT platform-DB session (SessionFactory,
  never the request-scoped registry — it runs inside a request), runs one
  lookup, and closes it. §2.5: driving the scoped session here would close
  the endpoint's own session mid-request."""

  def test_independent_session_is_opened_and_closed(self) -> None:
    from robosystems.middleware.mcp.tools import _gate

    mock_session = MagicMock()

    with (
      patch(
        "robosystems.database.SessionFactory", return_value=mock_session
      ) as factory,
      patch.object(_gate, "load_graph_metadata", return_value=_entity_meta()) as loader,
    ):
      result = _gate._load_with_short_lived_session("kg01ABC")

    factory.assert_called_once_with()
    loader.assert_called_once_with("kg01ABC", mock_session)
    mock_session.close.assert_called_once_with()
    assert "roboledger" in result.schema_extensions

  def test_session_is_closed_even_when_the_lookup_raises(self) -> None:
    from robosystems.middleware.mcp.tools import _gate

    mock_session = MagicMock()

    with (
      patch("robosystems.database.SessionFactory", return_value=mock_session),
      patch.object(_gate, "load_graph_metadata", side_effect=RuntimeError("boom")),
    ):
      with pytest.raises(RuntimeError):
        _gate._load_with_short_lived_session("kg01ABC")

    mock_session.close.assert_called_once_with()
