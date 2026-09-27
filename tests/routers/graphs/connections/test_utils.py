"""Connection robustness components share one circuit breaker."""

import pytest

from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.routers.graphs.connections import utils
from robosystems.routers.graphs.connections.utils import (
  create_robustness_components,
  record_operation_failure,
)


@pytest.fixture
def breaker(monkeypatch):
  fresh = CircuitBreakerManager(failure_threshold=2, recovery_timeout=60)
  monkeypatch.setattr(utils, "circuit_breaker", fresh)
  return fresh


def _fail(components, **kwargs):
  record_operation_failure(
    components=components,
    operation_name="sync_connection",
    endpoint="/v1/graphs/{graph_id}/connections/{connection_id}/sync",
    graph_id="kg1",
    user_id="user_1",
    **kwargs,
  )


@pytest.mark.unit
def test_requests_share_one_breaker(breaker):
  assert create_robustness_components()["circuit_breaker"] is breaker
  assert create_robustness_components()["circuit_breaker"] is breaker


@pytest.mark.unit
def test_failures_across_requests_trip_the_breaker(breaker):
  for _ in range(2):
    _fail(create_robustness_components(), error=RuntimeError("provider down"))

  with pytest.raises(Exception) as exc:
    breaker.check_circuit("kg1", "sync_connection")
  assert getattr(exc.value, "status_code", None) == 503


@pytest.mark.unit
def test_disabled_provider_does_not_count(breaker):
  for _ in range(3):
    _fail(
      create_robustness_components(),
      error_type="provider_disabled",
      counts_toward_breaker=False,
    )

  assert breaker.check_circuit("kg1", "sync_connection") is True
