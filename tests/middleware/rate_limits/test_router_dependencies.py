"""Guard tests: the rate-limit dependency stays wired on graph routers.

These routers had (or gained) router-level rate limiting. Because
``tests/conftest.py`` overrides ``subscription_aware_rate_limit_dependency``
to a no-op, the request-path integration suites would NOT catch an accidental
removal of the wiring — so assert the declaration directly.
"""

import pytest

from robosystems.middleware.rate_limits import subscription_aware_rate_limit_dependency


def _declares_rate_limit(router) -> bool:
  """True if the router declares the subscription-aware rate-limit dependency."""
  return any(
    getattr(dep, "dependency", None) is subscription_aware_rate_limit_dependency
    for dep in router.dependencies
  )


def _import_router(module_path: str):
  import importlib

  return importlib.import_module(module_path).router


@pytest.mark.parametrize(
  "module_path",
  [
    "robosystems.routers.graphs.search",
    "robosystems.routers.graphs.documents",
    "robosystems.routers.graphs.subgraphs.main",
    "robosystems.routers.graphs.subgraphs.info",
    "robosystems.routers.graphs.subgraphs.quota",
  ],
)
def test_router_declares_subscription_rate_limit(module_path):
  router = _import_router(module_path)
  assert _declares_rate_limit(router), (
    f"{module_path} lost its subscription_aware_rate_limit_dependency — "
    "search/documents/subgraphs endpoints must stay rate-limited on user graphs"
  )
