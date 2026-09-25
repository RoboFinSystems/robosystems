"""The analytical views pass the same read gauntlet as their sibling surfaces.

A view read counts against a shared repository's per-plan limits, and keeps
nothing in the shared idempotency cache. Runs against real Valkey and the
real platform test database.
"""

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.operations import (
  IdempotencyCache,
  compute_idempotency_cache_key,
)
from robosystems.models.api.views.view_config import CreateViewRequest
from robosystems.routers.extensions.roboledger.views import (
  _require_readable_graph,
  build_fact_grid_op,
)

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_a_view_result_is_not_kept_in_the_idempotency_cache(test_user):
  cache = IdempotencyCache()
  graph_id = "kg" + uuid.uuid4().hex[:18]
  key = f"view-{uuid.uuid4().hex}"
  body = CreateViewRequest(elements=["rs-gaap:Revenues"], period_type="annual")

  with patch(
    "robosystems.routers.extensions.roboledger.views.query_fact_grid",
    new=AsyncMock(return_value=([], False)),
  ):
    envelope = await build_fact_grid_op(
      body=body, graph_id=graph_id, user=test_user, idempotency_key=key, cache=cache
    )

  assert envelope.status == "completed"
  stored = await cache._client.get(
    compute_idempotency_cache_key(str(test_user.id), graph_id, "build-fact-grid", key)
  )
  assert stored is None


@pytest.mark.asyncio
async def test_views_count_against_the_shared_repository_plan(test_db, test_user):
  from robosystems.models.core.graph.graph import Graph
  from robosystems.models.core.user.user_repository import (
    RepositoryAccessLevel,
    RepositoryType,
    UserRepository,
  )

  if test_db.query(Graph).filter(Graph.graph_id == "sec").first() is None:
    Graph.create(
      graph_id="sec",
      org_id=None,
      graph_name="SEC",
      graph_type="repository",
      session=test_db,
    )
  UserRepository.create_access(
    user_id=test_user.id,
    repository_type=RepositoryType.SEC,
    repository_name="sec",
    access_level=RepositoryAccessLevel.READ,
    repository_plan="starter",
    session=test_db,
  )
  with (
    patch("robosystems.config.env.env.RATE_LIMIT_ENABLED", True),
    patch(
      "robosystems.routers.extensions.roboledger.views.require_graph_access",
    ),
  ):
    refused = None
    for _ in range(500):
      try:
        await _require_readable_graph(graph_id="sec", user=test_user, session=test_db)
      except HTTPException as exc:
        refused = exc
        break

  assert refused is not None and refused.status_code == 429
