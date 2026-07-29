import pytest

from robosystems.config.shared_repositories import (
  get_all_repository_pricing,
  get_credit_costs,
  get_plan_details,
  get_rate_limits,
  is_endpoint_allowed,
)


def test_get_plan_details_returns_plan_metadata():
  details = get_plan_details("advanced")

  assert details is not None
  assert details["price_cents"] == 9900
  assert "Everything in Starter" in details["features"]


def test_get_plan_details_returns_none_for_unknown():
  assert get_plan_details("invalid") is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
  "repository,plan,expected_key",
  [
    ("sec", "starter", "queries_per_hour"),
    ("sec", "advanced", "agent_calls_per_day"),
  ],
)
def test_get_rate_limits_returns_config(repository, plan, expected_key):
  limits = get_rate_limits(repository, plan)

  assert limits is not None
  assert expected_key in limits


def test_get_rate_limits_returns_none_for_unknown_repo():
  assert get_rate_limits("nonexistent", "starter") is None


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
  assert is_endpoint_allowed(endpoint) is expected


def test_get_plan_details_with_explicit_repo_id():
  details = get_plan_details("starter", repo_id="sec")

  assert details is not None
  assert details["price_cents"] == 2900
  assert details["name"] == "Starter"


def test_get_plan_details_with_unknown_repo_id():
  assert get_plan_details("starter", repo_id="nonexistent") is None


def test_get_rate_limits_values_are_integers():
  limits = get_rate_limits("sec", "starter")

  assert limits is not None
  assert all(isinstance(v, int) for v in limits.values())


def test_is_endpoint_allowed_with_repo_id():
  assert is_endpoint_allowed("query", repo_id="sec") is True
  assert is_endpoint_allowed("backup", repo_id="sec") is False


def test_get_credit_costs_returns_sec_costs():
  costs = get_credit_costs("sec")

  assert costs is not None
  assert costs["query"] == 0
  assert costs["ai_tokens"] is None  # dynamic pricing


def test_get_credit_costs_returns_none_for_unknown():
  assert get_credit_costs("nonexistent") is None


def test_get_all_repository_pricing_contains_repos():
  pricing = get_all_repository_pricing()

  assert pricing["plans"]["starter"]["price_display"] == "$29/month"
  assert pricing["repositories"]["sec"]["status"] == "available"
  assert pricing["billing_model"].startswith("No credit consumption")


def test_get_repository_plan_returns_the_canonical_plan_key():
  """The returned name is the manifest plan key, never the caller's raw string.

  'sec-advanced' is accepted for lookup, but persisting it verbatim broke
  every downstream lookup keyed on the plan: rate limits returned {} (read as
  "no access"), and credit/price config zeroed out.
  """
  from robosystems.config import BillingConfig

  prefixed = BillingConfig.get_repository_plan("sec", "sec-advanced")
  plain = BillingConfig.get_repository_plan("sec", "advanced")

  assert prefixed is not None and plain is not None
  assert prefixed["name"] == "advanced"
  assert plain["name"] == "advanced"
  assert prefixed["price_cents"] == plain["price_cents"]
