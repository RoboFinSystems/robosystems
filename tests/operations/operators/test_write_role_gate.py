"""The operator execution path gates write-capable operators on the graph role.

Operators drive MCP tools through a tool-access layer that carries no user
identity, so the per-tool write classification the MCP router applies never runs
here. The gate lives in the two execution adapters rather than the routers,
because the SSE and background-queue handlers call the adapters directly and
bypass the orchestrator entirely — a router-level check would miss them.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.operations.operators.base import (
  OperatorCapability,
  OperatorResult,
  OperatorSpec,
  enforce_operator_write_role,
)

GRAPH_ID = "kg01234567890abcdef"
USER_ID = "usr_test123"


def _operator(*, read_only: bool) -> MagicMock:
  operator = MagicMock()
  operator.spec = OperatorSpec(
    name="Test Operator",
    description="test",
    capabilities=[OperatorCapability.CUSTOM],
    read_only=read_only,
  )
  # Awaitable and harmless, so that removing the gate lets the adapter run to
  # completion and the test fails with "DID NOT RAISE HTTPException" — the
  # signal a reader can act on — rather than an incidental TypeError.
  operator.run = AsyncMock(return_value=OperatorResult(content="ok"))
  return operator


class TestEnforceOperatorWriteRole:
  def test_read_only_operator_skips_the_role_check(self) -> None:
    with patch(
      "robosystems.middleware.auth.dependencies.require_graph_write_role"
    ) as gate:
      enforce_operator_write_role(_operator(read_only=True), GRAPH_ID, USER_ID)

    gate.assert_not_called()

  def test_write_capable_operator_is_checked(self) -> None:
    with patch(
      "robosystems.middleware.auth.dependencies.require_graph_write_role"
    ) as gate:
      enforce_operator_write_role(_operator(read_only=False), GRAPH_ID, USER_ID)

    gate.assert_called_once_with(USER_ID, GRAPH_ID)

  def test_viewer_is_denied(self) -> None:
    with patch(
      "robosystems.middleware.auth.dependencies.require_graph_write_role",
      side_effect=HTTPException(status_code=403, detail="read-only"),
    ):
      with pytest.raises(HTTPException) as exc:
        enforce_operator_write_role(_operator(read_only=False), GRAPH_ID, USER_ID)

    assert exc.value.status_code == 403

  def test_spec_default_is_fail_closed(self) -> None:
    """An operator that declares nothing is treated as write-capable."""
    spec = OperatorSpec(
      name="Undeclared",
      description="test",
      capabilities=[OperatorCapability.CUSTOM],
    )
    assert spec.read_only is False


class TestAdaptersEnforceBeforeToolAccess:
  """Both adapters check the role *before* constructing tool access.

  Asserted through the adapter rather than by reading it, and by proving tool
  access was never constructed — an ordering regression that left the gate in
  place but moved it after initialization would still leak graph reads.
  """

  @pytest.mark.asyncio
  async def test_api_adapter_denies_a_viewer(self) -> None:
    from robosystems.operations.operators.adapters import api

    user = MagicMock()
    user.id = USER_ID

    # AIClient is patched so that removing the gate fails this test with a
    # clean "DID NOT RAISE HTTPException" rather than an unrelated credentials
    # error from further down the adapter.
    with (
      patch.object(
        api,
        "enforce_operator_write_role",
        side_effect=HTTPException(status_code=403, detail="read-only"),
      ),
      patch.object(api, "HttpToolAccess") as tool_access,
      patch.object(api, "get_ai_client"),
      patch.object(api, "TrackedAIClient"),
    ):
      with pytest.raises(HTTPException) as exc:
        await api.run_operator_api(
          operator=_operator(read_only=False),
          graph_id=GRAPH_ID,
          user=user,
          query="anything",
        )

    assert exc.value.status_code == 403
    tool_access.assert_not_called()

  @pytest.mark.asyncio
  async def test_worker_adapter_denies_a_viewer(self) -> None:
    from robosystems.operations.operators.adapters import worker

    with (
      patch.object(
        worker,
        "enforce_operator_write_role",
        side_effect=HTTPException(status_code=403, detail="read-only"),
      ),
      patch.object(worker, "DirectToolAccess") as tool_access,
      patch.object(worker, "get_ai_client"),
      patch.object(worker, "TrackedAIClient"),
      patch.object(worker, "FactoryCreditConsumer"),
    ):
      with pytest.raises(HTTPException) as exc:
        await worker.run_operator_worker(
          operator=_operator(read_only=False),
          task_id="task_1",
          graph_id=GRAPH_ID,
          user_id=USER_ID,
          params={},
          manager=AsyncMock(),
        )

    assert exc.value.status_code == 403
    tool_access.assert_not_called()


class TestRegisteredOperatorDeclarations:
  """The read_only flag is a security decision, so each registered operator's
  value is asserted explicitly — flipping one has to break a test that says why.
  """

  def test_cypher_operator_is_read_only(self) -> None:
    from robosystems.operations.operators.implementations.cypher import CypherOperator

    assert CypherOperator.spec.read_only is True, (
      "CypherOperator is viewer-accessible because READ_ONLY_TOOLS contains no "
      "write tool; adding one means this flag must go"
    )

  def test_mapping_operator_is_write_capable(self) -> None:
    from robosystems.operations.operators.implementations.mapping.operator import (
      MappingOperator,
    )

    assert MappingOperator.spec.read_only is False, (
      "MappingOperator persists mapping associations, so it must stay gated"
    )


class TestToolSurfaceMatchesSpec:
  """The tool surface handed to an operator must not exceed what the role
  gate checked for. A read-only operator skips the write-role gate, so its
  tool access must be built read-only — otherwise the flag that skips the
  gate is also the flag that unlocks the write tools.
  """

  @pytest.mark.asyncio
  async def test_api_adapter_builds_tool_access_from_the_spec(self) -> None:
    from robosystems.operations.operators.adapters import api

    user = MagicMock()
    user.id = USER_ID

    with (
      patch.object(api, "enforce_operator_write_role"),
      patch.object(api, "enforce_operator_credits"),
      patch.object(api, "HttpToolAccess") as tool_access,
      patch.object(api, "get_ai_client"),
      patch.object(api, "TrackedAIClient"),
    ):
      tool_access.return_value = MagicMock(close=AsyncMock())
      await api.run_operator_api(
        operator=_operator(read_only=True),
        graph_id=GRAPH_ID,
        user=user,
        query="anything",
      )

    tool_access.assert_called_once_with(GRAPH_ID, read_only=True, user_id=str(USER_ID))

  @pytest.mark.asyncio
  async def test_api_adapter_grants_writes_only_to_write_capable_specs(self) -> None:
    from robosystems.operations.operators.adapters import api

    user = MagicMock()
    user.id = USER_ID

    with (
      patch.object(api, "enforce_operator_write_role"),
      patch.object(api, "enforce_operator_credits"),
      patch.object(api, "HttpToolAccess") as tool_access,
      patch.object(api, "get_ai_client"),
      patch.object(api, "TrackedAIClient"),
    ):
      tool_access.return_value = MagicMock(close=AsyncMock())
      await api.run_operator_api(
        operator=_operator(read_only=False),
        graph_id=GRAPH_ID,
        user=user,
        query="anything",
      )

    tool_access.assert_called_once_with(GRAPH_ID, read_only=False, user_id=str(USER_ID))

  @pytest.mark.asyncio
  async def test_http_tool_access_wires_read_only_into_the_tool_manager(self) -> None:
    from robosystems.operations.operators.tool_access import HttpToolAccess

    with (
      patch("robosystems.middleware.mcp.GraphMCPTools") as tools_cls,
      patch(
        "robosystems.middleware.mcp.create_graph_mcp_client",
        new=AsyncMock(return_value=MagicMock()),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      access = HttpToolAccess(GRAPH_ID, read_only=True)
      await access.initialize()

    assert tools_cls.call_args.kwargs["read_only"] is True

  @pytest.mark.asyncio
  async def test_http_tool_access_defaults_to_read_only(self) -> None:
    """A caller that says nothing gets no write tools — the same fail-closed
    default as OperatorSpec.read_only, in the opposite direction."""
    from robosystems.operations.operators.tool_access import HttpToolAccess

    with (
      patch("robosystems.middleware.mcp.GraphMCPTools") as tools_cls,
      patch(
        "robosystems.middleware.mcp.create_graph_mcp_client",
        new=AsyncMock(return_value=MagicMock()),
      ),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      await HttpToolAccess(GRAPH_ID).initialize()

    assert tools_cls.call_args.kwargs["read_only"] is True


class TestToolAccessCarriesTheActingUser:
  def test_direct_tool_access_exposes_user_id_to_tools(self) -> None:
    from robosystems.operations.operators.tool_access import DirectToolAccess

    access = DirectToolAccess(GRAPH_ID, user_id="usr_worker")
    assert access.user_id == "usr_worker"
    assert DirectToolAccess(GRAPH_ID).user_id is None


class TestGraphScopeGate:
  """The orchestrator checks `graph_scope` when it routes; the SSE, queued and
  worker paths call the adapters directly and used to skip it."""

  def _scoped_operator(self) -> MagicMock:
    from robosystems.operations.operators.base import GraphScope

    operator = MagicMock()
    operator.spec = OperatorSpec(
      name="Ledger-only Operator",
      description="test",
      capabilities=[OperatorCapability.CUSTOM],
      graph_scope=GraphScope(schema_extension="roboledger"),
    )
    operator.run = AsyncMock(return_value=OperatorResult(content="ok"))
    return operator

  def test_refuses_a_graph_outside_the_scope(self) -> None:
    from fastapi import HTTPException

    from robosystems.operations.operators.base import enforce_operator_graph_scope

    with patch(
      "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
      return_value=["roboinvestor"],
    ):
      with pytest.raises(HTTPException) as exc_info:
        enforce_operator_graph_scope(self._scoped_operator(), GRAPH_ID)
    assert exc_info.value.status_code == 403

  def test_passes_a_graph_inside_the_scope_and_unscoped_operators(self) -> None:
    from robosystems.operations.operators.base import enforce_operator_graph_scope

    with patch(
      "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
      return_value=["roboledger"],
    ) as resolve:
      enforce_operator_graph_scope(self._scoped_operator(), GRAPH_ID)
      resolve.assert_called_once_with(GRAPH_ID)
      resolve.reset_mock()
      enforce_operator_graph_scope(_operator(read_only=False), GRAPH_ID)
      resolve.assert_not_called()

  @pytest.mark.asyncio
  async def test_api_adapter_applies_the_scope_gate(self) -> None:
    from fastapi import HTTPException

    from robosystems.operations.operators.adapters import api

    user = MagicMock()
    user.id = USER_ID
    with (
      patch.object(api, "enforce_operator_write_role"),
      patch.object(api, "enforce_operator_credits"),
      patch(
        "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
        return_value=[],
      ),
    ):
      with pytest.raises(HTTPException) as exc_info:
        await api.run_operator_api(
          operator=self._scoped_operator(),
          graph_id=GRAPH_ID,
          user=user,
          query="anything",
        )
    assert exc_info.value.status_code == 403
