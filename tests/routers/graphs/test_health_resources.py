"""Tests for instance CPU/memory exposure on the graph health endpoint.

The exposure guard is a tenant-isolation boundary: instance load must only be
visible to the owner of a genuinely single-tenant instance. These tests pin
both halves of that guard, plus the status derivation that keeps a normal
materialization from reading as a fault.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from robosystems.routers.graphs.health import (
  _derive_resource_status,
  _resources_exposable,
  get_database_health,
)


class TestResourcesExposable:
  """Both halves of the guard are required; neither is redundant."""

  def test_dedicated_tier_is_exposable(self):
    assert _resources_exposable("kg01234567890abcdef", "ladybug-standard") is True

  @pytest.mark.parametrize(
    "tier", ["ladybug-standard", "ladybug-large", "ladybug-xlarge"]
  )
  def test_all_dedicated_product_tiers_are_exposable(self, tier):
    assert _resources_exposable("kg01234567890abcdef", tier) is True

  def test_shared_repository_is_not_exposable(self):
    """sec runs one database per box but is read by every tenant."""
    assert _resources_exposable("sec", "ladybug-shared") is False

  def test_shared_repository_subgraph_is_not_exposable(self):
    """sec_historical serves every tenant from the shared master, like sec.

    The exact-only check let it through to the databases_per_instance branch,
    which is 1 for ladybug-shared — leaking shared-master metrics.
    """
    assert _resources_exposable("sec_historical", "ladybug-shared") is False

  def test_packed_tier_is_not_exposable(self):
    """A tier that co-locates tenants closes the guard automatically.

    This is the property that makes a future packed tier safe by
    construction rather than by remembering to revoke exposure.
    """
    with patch(
      "robosystems.config.graph_tier.GraphTierConfig.get_databases_per_instance",
      return_value=8,
    ):
      assert _resources_exposable("kg01234567890abcdef", "ladybug-community") is False

  def test_shared_repo_check_is_independent_of_packing(self):
    """A shared repo stays hidden even when its tier reports dedicated."""
    with patch(
      "robosystems.config.graph_tier.GraphTierConfig.get_databases_per_instance",
      return_value=1,
    ):
      assert _resources_exposable("sec", "ladybug-shared") is False


class TestDeriveResourceStatus:
  """`constrained` is anchored to the admission controller's own thresholds."""

  ADMISSION = {"cpu_threshold": 90.0, "memory_threshold": 85.0}

  def test_low_load_is_idle(self):
    assert _derive_resource_status(5.0, 30.0, self.ADMISSION) == "idle"

  def test_moderate_load_is_busy(self):
    assert _derive_resource_status(55.0, 60.0, self.ADMISSION) == "busy"

  def test_materialization_boost_reads_as_busy_not_constrained(self):
    """Materialization deliberately boosts toward the whole instance.

    Below the admission threshold that is the engine working as designed,
    so it must not surface as a fault the tenant would file a ticket about.
    """
    assert _derive_resource_status(45.0, 80.0, self.ADMISSION) == "busy"

  def test_memory_at_admission_threshold_is_constrained(self):
    assert _derive_resource_status(10.0, 85.0, self.ADMISSION) == "constrained"

  def test_cpu_at_admission_threshold_is_constrained(self):
    assert _derive_resource_status(90.0, 10.0, self.ADMISSION) == "constrained"

  def test_missing_thresholds_never_report_constrained(self):
    """A node that reports no thresholds should degrade, not cry wolf."""
    assert _derive_resource_status(99.0, 99.0, {}) == "busy"

  def test_no_readings_yields_no_status(self):
    assert _derive_resource_status(None, None, self.ADMISSION) is None

  def test_partial_reading_still_classifies(self):
    assert _derive_resource_status(None, 95.0, self.ADMISSION) == "constrained"


def _make_user():
  user = Mock()
  user.id = "user-123"
  return user


def _make_graph_client(system=None, admission=None):
  client = AsyncMock()
  client.get_database_metrics = AsyncMock(return_value={"size_mb": 12.5})
  client.health_check = AsyncMock(return_value={"uptime_seconds": 3600.0})
  client.get_metrics = AsyncMock(
    return_value={
      "system": system
      or {
        "cpu": {"usage_percent": 7.5},
        "memory": {"usage_percent": 22.0, "used_gb": 1.5},
      },
      "admission_control": admission
      or {"cpu_threshold": 90.0, "memory_threshold": 85.0},
    }
  )
  client.close = AsyncMock()
  return client


def _make_graph_record(tier="ladybug-standard"):
  record = Mock()
  record.graph_tier = tier
  record.graph_stale = False
  record.graph_stale_reason = None
  record.graph_stale_at = None
  record.graph_metadata = {}
  return record


async def _call_health(graph_id, client, graph_record):
  with patch("robosystems.routers.graphs.health.get_universal_repository", AsyncMock()):
    with patch(
      "robosystems.routers.graphs.health._get_graph_client",
      AsyncMock(return_value=client),
    ):
      with patch("robosystems.models.core.Graph.get_by_id", return_value=graph_record):
        return await get_database_health(
          graph_id=graph_id,
          current_user=_make_user(),
          session=Mock(),
          _=None,
        )


@pytest.mark.asyncio
class TestHealthEndpointResourceExposure:
  """End-to-end through the handler, since the guard is what matters."""

  async def test_dedicated_graph_reports_resources(self):
    client = _make_graph_client()
    result = await _call_health(
      "kg01234567890abcdef", client, _make_graph_record("ladybug-standard")
    )

    assert result.cpu_usage_percent == 7.5
    assert result.memory_usage_percent == 22.0
    assert result.memory_usage_mb == pytest.approx(1536.0)
    assert result.resource_status == "idle"
    client.get_metrics.assert_awaited_once()

  async def test_shared_repository_reports_no_resources(self):
    client = _make_graph_client()
    result = await _call_health("sec", client, _make_graph_record("ladybug-shared"))

    assert result.cpu_usage_percent is None
    assert result.memory_usage_percent is None
    assert result.memory_usage_mb is None
    assert result.resource_status is None

  async def test_shared_repository_never_queries_the_instance(self):
    """The guard short-circuits before the call, not after."""
    client = _make_graph_client()
    await _call_health("sec", client, _make_graph_record("ladybug-shared"))

    client.get_metrics.assert_not_awaited()

  async def test_resource_failure_does_not_fail_health_check(self):
    """Resource collection is best-effort decoration on a health check."""
    client = _make_graph_client()
    client.get_metrics = AsyncMock(side_effect=RuntimeError("node unreachable"))

    result = await _call_health(
      "kg01234567890abcdef", client, _make_graph_record("ladybug-standard")
    )

    assert result.status == "healthy"
    assert result.cpu_usage_percent is None
    assert result.resource_status is None

  async def test_constrained_instance_surfaces_status(self):
    client = _make_graph_client(
      system={
        "cpu": {"usage_percent": 12.0},
        "memory": {"usage_percent": 97.0, "used_gb": 7.6},
      }
    )
    result = await _call_health(
      "kg01234567890abcdef", client, _make_graph_record("ladybug-standard")
    )

    assert result.resource_status == "constrained"
