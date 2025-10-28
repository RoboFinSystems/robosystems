import pytest

from robosystems.config.billing.repositories import (
  RepositoryBillingConfig,
  RepositoryPlan,
  SharedRepository,
)


def test_get_plan_details_returns_plan_metadata():
  details = RepositoryBillingConfig.get_plan_details(RepositoryPlan.ADVANCED)

  assert details is not None
  assert details["price_cents"] == 9900
  assert "AI agent credits" in details["features"][0]


def test_get_plan_details_returns_none_for_unknown():
  assert RepositoryBillingConfig.get_plan_details("invalid") is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
  "repository,plan,expected_key",
  [
    (SharedRepository.SEC, RepositoryPlan.STARTER, "queries_per_hour"),
    (SharedRepository.SEC, RepositoryPlan.UNLIMITED, "agent_calls_per_day"),
  ],
)
def test_get_rate_limits_returns_config(repository, plan, expected_key):
  limits = RepositoryBillingConfig.get_rate_limits(repository, plan)

  assert limits is not None
  assert expected_key in limits


def test_get_rate_limits_returns_none_for_unknown_repo():
  assert (
    RepositoryBillingConfig.get_rate_limits(
      SharedRepository.INDUSTRY, RepositoryPlan.STARTER
    )
    is None
  )


@pytest.mark.parametrize(
  "endpoint,expected",
  [
    ("query", True),
    ("graphs/agent/status", True),
    ("graphs/subgraph/backup", False),
    ("graphs/subgraph/delete", False),
  ],
)
def test_is_endpoint_allowed_matches_blocklists(endpoint, expected):
  assert RepositoryBillingConfig.is_endpoint_allowed(endpoint) is expected


def test_get_all_repository_pricing_contains_repos():
  pricing = RepositoryBillingConfig.get_all_repository_pricing()

  assert pricing["plans"][RepositoryPlan.STARTER]["price_display"] == "$29/month"
  assert pricing["repositories"][SharedRepository.SEC]["status"] == "available"
  assert pricing["billing_model"].startswith("No credit consumption")
