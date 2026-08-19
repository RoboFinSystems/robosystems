"""Comprehensive unit tests for the MCP handlers module."""

import asyncio
import json
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.routers.graphs.mcp.handlers import (
  MCPHandler,
  execute_mcp_query_with_timeout,
  validate_mcp_access,
)


def _make_mock_user(user_id="user-123"):
  user = Mock()
  user.id = user_id
  return user


def _make_mock_repository(base_url="http://localhost:8001"):
  repo = Mock()
  repo.config = Mock()
  repo.config.base_url = base_url
  return repo


@pytest.mark.unit
class TestValidateMcpAccess:
  """Test the validate_mcp_access function."""

  @pytest.mark.asyncio
  async def test_shared_repo_access_granted(self):
    """Test that shared repository access is granted for valid user."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.auth.utils.validate_repository_access",
        return_value=True,
      ),
    ):
      # Should not raise
      await validate_mcp_access("sec", _make_mock_user(), Mock(), "read")

  @pytest.mark.asyncio
  async def test_shared_repo_access_denied(self):
    """Test that shared repository access is denied for invalid user."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.auth.utils.validate_repository_access",
        return_value=False,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await validate_mcp_access("sec", _make_mock_user(), Mock(), "read")
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_user_graph_access_granted(self):
    """Test that user graph access is granted with valid permissions."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_access",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access"
      ) as mock_access,
    ):
      await validate_mcp_access(
        "kg01234567890abcdef", _make_mock_user(), Mock(), "read"
      )
    # Membership proven → the lifecycle/subscription gate runs at read strength.
    mock_access.assert_called_once()
    assert mock_access.call_args.kwargs["require_write"] is False

  @pytest.mark.asyncio
  async def test_user_graph_read_blocked_by_lifecycle(self):
    """A member of a suspended / expired graph is refused even for reads —
    the same answer `/query` gives, so MCP is not the surface that still
    serves a graph the platform has closed."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_access",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access",
        side_effect=HTTPException(status_code=403, detail="Subscription has ended."),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await validate_mcp_access(
          "kg01234567890abcdef", _make_mock_user(), Mock(), "read"
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_user_graph_access_denied(self):
    """Test that user graph access is denied when user lacks permission."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_access",
        return_value=False,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await validate_mcp_access(
          "kg01234567890abcdef", _make_mock_user(), Mock(), "read"
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_user_graph_write_requires_write_role(self):
    """Write operations on a user graph require the write role, not bare membership."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_write_access",
        return_value=True,
      ) as mock_write,
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access"
      ) as mock_access,
    ):
      await validate_mcp_access(
        "kg01234567890abcdef", _make_mock_user(), Mock(), "write"
      )
      mock_write.assert_called_once()
    # Write tools run the lifecycle gate at write strength: a canceled grace
    # period or a tier upgrade blocks the MCP write like the REST write.
    assert mock_access.call_args.kwargs["require_write"] is True

  @pytest.mark.asyncio
  async def test_user_graph_write_blocked_by_lifecycle(self):
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_write_access",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.billing.enforcement.require_graph_access",
        side_effect=HTTPException(
          status_code=403, detail="Graph tier upgrade in progress."
        ),
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await validate_mcp_access(
          "kg01234567890abcdef", _make_mock_user(), Mock(), "write"
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_user_graph_write_denied_for_readonly_role(self):
    """A read-only (viewer) member is denied write access despite membership."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=False,
      ),
      patch(
        "robosystems.models.core.GraphUser.user_has_write_access",
        return_value=False,
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await validate_mcp_access(
          "kg01234567890abcdef", _make_mock_user(), Mock(), "write"
        )
      assert exc_info.value.status_code == 403

  @pytest.mark.asyncio
  async def test_shared_repo_subgraph_access(self):
    """Test access validation for shared repository subgraph."""
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.middleware.auth.utils.validate_repository_access",
        return_value=True,
      ),
    ):
      await validate_mcp_access("sec_historical", _make_mock_user(), Mock(), "read")


@pytest.mark.unit
class TestMCPHandler:
  """Test the MCPHandler class."""

  @pytest.mark.asyncio
  async def test_init_creates_task(self):
    """Test that MCPHandler creates an init task."""
    repo = _make_mock_repository()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=AsyncMock(),
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        # Wait for init task
        await handler._ensure_initialized()

        assert handler.graph_id == "kg01234567890abcdef"
        assert handler._closed is False

  @pytest.mark.asyncio
  async def test_backend_type_is_ladybug(self):
    """Test that backend_type property returns 'ladybug'."""
    repo = _make_mock_repository()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=AsyncMock(),
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        assert handler.backend_type == "ladybug"
        # Cancel the init task if still running
        if handler._init_task:
          handler._init_task.cancel()
          try:
            await handler._init_task
          except (asyncio.CancelledError, Exception):
            pass

  @pytest.mark.asyncio
  async def test_ensure_not_closed_raises_on_closed_handler(self):
    """Test that _ensure_not_closed raises RuntimeError when closed."""
    repo = _make_mock_repository()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=AsyncMock(),
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        handler._closed = True

        with pytest.raises(RuntimeError, match="is closed"):
          handler._ensure_not_closed()

        # Cleanup
        if handler._init_task:
          handler._init_task.cancel()
          try:
            await handler._init_task
          except (asyncio.CancelledError, Exception):
            pass

  @pytest.mark.asyncio
  async def test_close_sets_closed_flag(self):
    """Test that close() sets _closed to True."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()
        await handler.close()

        assert handler._closed is True
        mock_client.close.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_close_idempotent(self):
    """Test that calling close() twice doesn't error."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()
        await handler.close()
        await handler.close()

        # close should only be called on client once
        assert mock_client.close.await_count == 1

  @pytest.mark.asyncio
  async def test_async_context_manager(self):
    """Test that MCPHandler works as async context manager."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        async with handler as h:
          assert h is handler
          assert h._closed is False

        assert handler._closed is True

  @pytest.mark.asyncio
  async def test_call_tool_get_graph_info(self):
    """Test call_tool with get-graph-info custom tool."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.get_graph_info = AsyncMock(
      return_value={"graph_id": "kg01234567890abcdef", "nodes": 100}
    )

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()

        result = await handler.call_tool("get-graph-info", {})

        assert result["type"] == "text"
        parsed = json.loads(result["text"])
        assert parsed["graph_id"] == "kg01234567890abcdef"

        await handler.close()

  @pytest.mark.asyncio
  async def test_call_tool_timeout_returns_error_text(self):
    """Test that timeout in call_tool returns error text, not exception."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    async def slow_info():
      await asyncio.sleep(100)

    mock_client.get_graph_info = slow_info

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        with patch(
          "robosystems.routers.graphs.mcp.handlers.timeout_coordinator"
        ) as mock_tc:
          mock_tc.get_tool_timeout.return_value = 0.001
          mock_tc.get_instance_timeout.return_value = 0.001

          handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
          await handler._ensure_initialized()

          result = await handler.call_tool("get-graph-info", {})

          assert "Error" in result["text"]
          assert "timed out" in result["text"]
          # Marked so transports return MCP isError instead of a success.
          assert result["is_error"] is True
          assert result["error_kind"] == "timeout"

          await handler.close()

  @pytest.mark.asyncio
  async def test_call_tool_general_exception_returns_error(self):
    """Test that general exceptions in call_tool return error text."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()
    mock_client.get_graph_info = AsyncMock(side_effect=RuntimeError("unexpected"))

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()

        result = await handler.call_tool("get-graph-info", {})

        assert "Error" in result["text"]
        assert result["is_error"] is True
        assert result["error_kind"] == "backend"

        await handler.close()

  @pytest.mark.asyncio
  async def test_call_tool_regular_tool_uses_mcp_tools(self):
    """Test call_tool delegates regular tools to execute_mcp_query_with_timeout."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock()

    mock_mcp_tools = AsyncMock()
    mock_mcp_tools.call_tool = AsyncMock(
      return_value=[{"label": "Entity", "count": 100}]
    )

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()
        handler.mcp_tools = mock_mcp_tools

        result = await handler.call_tool("get-graph-schema", {})

        assert result["type"] == "text"

        await handler.close()

  @pytest.mark.asyncio
  async def test_close_error_raises_runtime_error(self):
    """Test that close() errors are collected and re-raised."""
    repo = _make_mock_repository()
    mock_client = AsyncMock()
    mock_client.close = AsyncMock(side_effect=RuntimeError("close failed"))

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=mock_client,
    ):
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()

        with pytest.raises(RuntimeError, match="Errors during MCP handler cleanup"):
          await handler.close()

        # _closed should still be set
        assert handler._closed is True

  @pytest.mark.asyncio
  async def test_repository_url_from_base_url_attr(self):
    """Test URL discovery from repository.base_url attribute."""
    repo = Mock()
    repo.base_url = "http://localhost:9000"
    del repo.config  # Remove config attribute
    del repo.api_base_url

    with patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=AsyncMock(),
    ) as mock_create:
      with patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ):
        handler = MCPHandler(repo, "kg01234567890abcdef", _make_mock_user())
        await handler._ensure_initialized()

        mock_create.assert_awaited_once_with(
          "kg01234567890abcdef", api_base_url="http://localhost:9000"
        )

        await handler.close()


@pytest.mark.unit
class TestExecuteMcpQueryWithTimeout:
  """Test the execute_mcp_query_with_timeout function."""

  @pytest.mark.asyncio
  async def test_successful_execution(self):
    """Test successful tool execution."""
    mock_tools = AsyncMock()
    mock_tools.call_tool = AsyncMock(return_value=[{"id": 1, "name": "Alice"}])

    result = await execute_mcp_query_with_timeout(
      mock_tools, "get-graph-schema", {}, timeout=30.0
    )

    assert result == [{"id": 1, "name": "Alice"}]
    mock_tools.call_tool.assert_awaited_once_with(
      "get-graph-schema", {}, return_raw=True
    )

  @pytest.mark.asyncio
  async def test_timeout_raises(self):
    """Test that slow execution raises TimeoutError."""
    mock_tools = AsyncMock()

    async def slow_call(*args, **kwargs):
      await asyncio.sleep(100)

    mock_tools.call_tool = slow_call

    with pytest.raises(TimeoutError):
      await execute_mcp_query_with_timeout(
        mock_tools, "get-graph-schema", {}, timeout=0.01
      )

  @pytest.mark.asyncio
  async def test_adds_timeout_to_cypher_tool_arguments(self):
    """Test that tool_timeout is added to arguments for cypher tools."""
    mock_tools = AsyncMock()
    mock_tools.call_tool = AsyncMock(return_value=[])

    await execute_mcp_query_with_timeout(
      mock_tools,
      "read-graph-cypher",
      {"query": "MATCH (n) RETURN n"},
      timeout=60.0,
      tool_timeout=30.0,
    )

    # Should have added timeout to arguments
    call_args = mock_tools.call_tool.call_args
    assert call_args[0][1]["timeout"] == 30

  @pytest.mark.asyncio
  async def test_does_not_add_timeout_for_non_cypher_tools(self):
    """Test that tool_timeout is not added for non-cypher tools."""
    mock_tools = AsyncMock()
    mock_tools.call_tool = AsyncMock(return_value=[])

    await execute_mcp_query_with_timeout(
      mock_tools,
      "get-graph-schema",
      {},
      timeout=60.0,
      tool_timeout=30.0,
    )

    # Arguments should not contain timeout
    call_args = mock_tools.call_tool.call_args
    assert "timeout" not in call_args[0][1]

  @pytest.mark.asyncio
  async def test_general_exception_propagates(self):
    """Test that general exceptions propagate from tool execution."""
    mock_tools = AsyncMock()
    mock_tools.call_tool = AsyncMock(side_effect=RuntimeError("DB error"))

    with pytest.raises(RuntimeError, match="DB error"):
      await execute_mcp_query_with_timeout(
        mock_tools, "get-graph-schema", {}, timeout=30.0
      )
