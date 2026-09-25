"""A teardown whose data disposal failed is retried, not forgotten.

The graph still reaches DEPROVISIONED, but it carries a mark the teardown
sensor selects, and a clean re-run clears it. Runs against the real platform
test database.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_sensor_context

from robosystems.dagster.sensors.graph_lifecycle import (
  suspended_graph_deprovisioning_sensor,
)
from robosystems.models.core.graph.graph import GraphStatus
from robosystems.operations.graph.deprovision_service import (
  RESIDUAL_PENDING_KEY,
  GraphDeprovisionService,
)

pytestmark = pytest.mark.integration


def _deprovisioned(test_db, graph):
  graph.status = GraphStatus.DEPROVISIONED.value
  graph.deleted_at = datetime.now(UTC) - timedelta(hours=2)
  test_db.commit()
  return graph


def _sensor_graph_ids(test_db) -> list[str]:
  factory = MagicMock(side_effect=lambda: test_db)
  factory.remove = MagicMock()
  with patch("robosystems.database.session", factory):
    runs = list(suspended_graph_deprovisioning_sensor(build_sensor_context()))
  if not runs:
    return []
  return runs[0].run_config["ops"]["deprovision_suspended_graphs"]["config"][
    "graph_ids"
  ]


@pytest.mark.asyncio
async def test_a_failed_disposal_is_marked_retried_and_cleared(test_db, sample_graph):
  graph = _deprovisioned(test_db, sample_graph)
  service = GraphDeprovisionService("test")

  def fail(graph_id, result):
    result.errors.append("Search index purge failed: timeout")

  with patch.object(service, "_purge_search_index", side_effect=fail):
    await service.deprovision_graph(graph.graph_id, test_db)
  test_db.refresh(graph)
  assert graph.graph_metadata.get(RESIDUAL_PENDING_KEY) is True
  assert graph.graph_id in _sensor_graph_ids(test_db)

  with patch.object(service, "_purge_search_index"):
    await service.deprovision_graph(graph.graph_id, test_db)
  test_db.refresh(graph)
  assert RESIDUAL_PENDING_KEY not in (graph.graph_metadata or {})
  assert graph.graph_id not in _sensor_graph_ids(test_db)
