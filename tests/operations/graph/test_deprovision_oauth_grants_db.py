"""Deleting a graph revokes the MCP clients' grants on it.

A grant names one graph with no foreign key to it. Left live, a connected
client's token stays valid and every call is refused with 403, so the client
never asks to authorize again. Runs against the real platform test database.
"""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.models.core import OAuthClient, OAuthGrant, OAuthToken
from robosystems.operations.graph.deprovision_service import (
  DeprovisionResult,
  GraphDeprovisionService,
)

pytestmark = pytest.mark.integration

RESOURCE = "https://api.test.example/v1/mcp"


def _client(session):
  client, _ = OAuthClient.register_preregistered(
    client_name="Claude",
    redirect_uris=["https://claude.ai/api/mcp/auth_callback"],
    confidential=False,
    session=session,
  )
  return client


def _grant_with_tokens(session, user, client, graph_id):
  grant = OAuthGrant.create(
    user_id=str(user.id),
    oauth_client_id=str(client.id),
    graph_id=graph_id,
    resource=RESOURCE,
    scope="mcp offline_access",
    session=session,
  )
  OAuthToken.mint_pair(grant_id=str(grant.id), user_id=str(user.id), session=session)
  return str(grant.id)


def _revoked(session, grant_id):
  session.expire_all()
  grant = OAuthGrant.get_by_id(grant_id, session)
  live_tokens = (
    session.query(OAuthToken)
    .filter(OAuthToken.grant_id == grant_id, OAuthToken.revoked_at.is_(None))
    .count()
  )
  return grant.revoked_at is not None and live_tokens == 0


def test_a_graph_and_its_subgraphs_lose_their_grants(test_db, sample_graph, test_user):
  client = _client(test_db)
  parent = sample_graph.graph_id
  on_parent = _grant_with_tokens(test_db, test_user, client, parent)
  on_subgraph = _grant_with_tokens(test_db, test_user, client, f"{parent}_dev")
  # Shares the prefix but not the separator: the underscore is not a wildcard.
  look_alike = _grant_with_tokens(test_db, test_user, client, f"{parent}Xdev")
  elsewhere = _grant_with_tokens(test_db, test_user, client, "sec")

  result = DeprovisionResult(status="success", graph_id=parent)
  GraphDeprovisionService._revoke_oauth_grants(
    parent, test_db, result, include_subgraphs=True
  )

  assert _revoked(test_db, on_parent)
  assert _revoked(test_db, on_subgraph)
  assert not _revoked(test_db, look_alike)
  assert not _revoked(test_db, elsewhere)
  assert result.oauth_grants_revoked == 2
  assert result.oauth_tokens_revoked == 4
  assert result.errors == []


@pytest.mark.asyncio
async def test_deleting_a_subgraph_revokes_only_its_grants(
  test_db, sample_graph, test_user
):
  client = _client(test_db)
  parent = sample_graph.graph_id
  on_parent = _grant_with_tokens(test_db, test_user, client, parent)
  on_subgraph = _grant_with_tokens(test_db, test_user, client, f"{parent}_dev")

  with patch(
    "robosystems.operations.providers.registry.provider_registry.cleanup_connection",
    new=AsyncMock(),
  ):
    result = await GraphDeprovisionService("test").release_subgraph_records(
      f"{parent}_dev", test_db
    )

  assert _revoked(test_db, on_subgraph)
  assert not _revoked(test_db, on_parent)
  assert result.oauth_grants_revoked == 1
