"""The graph-agnostic transport dispatches on the grant's graph."""

from unittest.mock import AsyncMock, Mock, patch

from robosystems.middleware.auth.oauth import OAuthPrincipal
from robosystems.models.core import User
from robosystems.routers.graphs.mcp import remote

KG = "kg19fb490f76871d22e835"


async def test_agnostic_transport_dispatches_on_the_grant_graph():
  user = User(id="user-1", email="u@example.com", is_active=True)
  principal = OAuthPrincipal(
    user=user,
    token_id="oat_1",
    grant_id="oag_1",
    client_row_id="oac_1",
    graph_id=KG,
    resource="https://api.test.example/v1/mcp",
    scope="mcp",
  )
  request = Mock()
  with patch.object(
    remote, "dispatch_jsonrpc", new=AsyncMock(return_value="resp")
  ) as dispatch:
    result = await remote.mcp_agnostic_transport(
      request, _transport=None, principal=principal, _rate_limit=None
    )
  assert result == "resp"
  dispatch.assert_awaited_once_with(request, KG, user)


def test_agnostic_router_is_exported():
  from robosystems.routers.graphs.mcp import agnostic_router

  assert agnostic_router is remote.agnostic_router
