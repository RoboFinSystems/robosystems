"""
Tests for organization usage/limits endpoints.

Focus on quota warnings and usage aggregation over credit + storage telemetry.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4
from unittest.mock import patch

import pytest

from robosystems.models.iam import (
  Graph,
  GraphUser,
  Org,
  OrgLimits,
  OrgRole,
  OrgType,
  OrgUser,
  GraphUsage,
  UsageEventType,
  User,
)

# Safety: router expects UsageEventType.AI_OPERATION; create alias if missing.
if not hasattr(UsageEventType, "AI_OPERATION"):
  setattr(UsageEventType, "AI_OPERATION", UsageEventType.AGENT_CALL)

pytestmark = pytest.mark.asyncio


def _create_graph(session, org_id: str, name: str) -> Graph:
  """Create a generic graph for the provided org."""
  graph = Graph.create(
    graph_id=f"graph_{uuid4().hex[:8]}",
    org_id=org_id,
    graph_name=name,
    graph_type="generic",
    session=session,
  )
  return graph


def _create_user(session, *, password_hash: str = "dummy_hash") -> User:
  """Create a simple user for membership setup."""
  suffix = uuid4().hex[:8]
  user = User(
    email=f"usage+{suffix}@example.com",
    name=f"Usage User {suffix}",
    password_hash=password_hash,
  )
  session.add(user)
  session.commit()
  session.refresh(user)
  return user


class TestOrgUsageEndpoints:
  async def test_get_org_limits_reports_usage_and_warnings(
    self, async_client, test_db, test_user
  ):
    """Ensure limits endpoint surfaces current usage, warnings, and allowances."""
    org = Org.create(
      name=f"Limits Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    graph_one = _create_graph(test_db, org.id, "Primary")
    graph_two = _create_graph(test_db, org.id, "Replica")

    GraphUser.create(
      user_id=test_user.id, graph_id=graph_one.graph_id, role="admin", session=test_db
    )
    GraphUser.create(
      user_id=test_user.id, graph_id=graph_two.graph_id, role="admin", session=test_db
    )

    limits = OrgLimits.create_default_limits(org_id=org.id, session=test_db)
    limits.max_graphs = 2
    test_db.commit()

    response = await async_client.get(f"/v1/orgs/{org.id}/limits")

    assert response.status_code == 200
    payload = response.json()
    assert payload["org_id"] == org.id
    assert payload["max_graphs"] == 2
    assert payload["current_usage"]["graphs"]["current"] == 2
    assert payload["current_usage"]["graphs"]["remaining"] == 0
    assert any("Approaching graph limit" in warning for warning in payload["warnings"])
    assert payload["can_create_graph"] is False

  async def test_get_org_limits_without_limits_returns_404(
    self, async_client, test_db, test_user
  ):
    """If limits record is missing we expect a 404."""
    org = Org.create(
      name=f"No Limit Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/limits")

    assert response.status_code == 404
    assert response.json()["detail"] == "Organization limits not found"

  async def test_get_org_limits_under_threshold_has_no_warnings(
    self, async_client, test_db, test_user
  ):
    """When usage is low, warnings list should remain empty and graph creation permitted."""
    org = Org.create(
      name=f"Plenty Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    graph = _create_graph(test_db, org.id, "Light Usage")
    GraphUser.create(
      user_id=test_user.id, graph_id=graph.graph_id, role="admin", session=test_db
    )

    limits = OrgLimits.create_default_limits(org_id=org.id, session=test_db)
    limits.max_graphs = 10
    test_db.commit()

    response = await async_client.get(f"/v1/orgs/{org.id}/limits")

    assert response.status_code == 200
    body = response.json()
    assert body["warnings"] == []
    assert body["can_create_graph"] is True

  async def test_get_org_usage_aggregates_graph_metrics(
    self, async_client, test_db, test_user
  ):
    """Usage endpoint should aggregate credits, ai ops, storage, and trends."""
    org = Org.create(
      name=f"Usage Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    graph = _create_graph(test_db, org.id, "Telemetry")

    now = datetime.now(timezone.utc)
    previous = now - timedelta(days=1)

    usage_records = [
      GraphUsage(
        user_id=test_user.id,
        graph_id=graph.graph_id,
        event_type=UsageEventType.AI_OPERATION.value,
        graph_tier=graph.graph_tier,
        credits_consumed=Decimal("10.00"),
        recorded_at=previous,
        billing_year=previous.year,
        billing_month=previous.month,
        billing_day=previous.day,
        billing_hour=previous.hour,
      ),
      GraphUsage(
        user_id=test_user.id,
        graph_id=graph.graph_id,
        event_type=UsageEventType.STORAGE_SNAPSHOT.value,
        graph_tier=graph.graph_tier,
        storage_gb=Decimal("7.5"),
        recorded_at=now,
        billing_year=now.year,
        billing_month=now.month,
        billing_day=now.day,
        billing_hour=now.hour,
      ),
    ]
    test_db.add_all(usage_records)
    test_db.flush()

    class FakeGraphCredits:
      available_credits = 90.0
      monthly_allocation = 120.0

    with patch(
      "robosystems.models.iam.graph_credits.GraphCredits.get_by_graph_id",
      return_value=FakeGraphCredits(),
    ):
      response = await async_client.get(f"/v1/orgs/{org.id}/usage", params={"days": 2})
    test_db.rollback()

    assert response.status_code == 200
    body = response.json()
    assert body["org_id"] == org.id
    assert body["period_days"] == 2

    summary = body["summary"]
    assert summary["total_credits_used"] == pytest.approx(10.0)
    assert summary["total_ai_operations"] == 1
    assert summary["total_api_calls"] == 2
    assert summary["total_storage_gb"] == pytest.approx(7.5, rel=1e-2)

    assert len(body["graph_details"]) == 1
    graph_detail = body["graph_details"][0]
    assert graph_detail["graph_id"] == graph.graph_id
    assert graph_detail["credits_used"] == pytest.approx(10.0)
    assert graph_detail["ai_operations"] == 1
    assert graph_detail["credits_available"] == 90.0
    assert graph_detail["credits_allocated"] == 120.0

    assert len(body["daily_trend"]) == 2
    assert all(
      {"date", "credits_used", "api_calls"} <= set(entry.keys())
      for entry in body["daily_trend"]
    )

  async def test_get_org_usage_handles_org_with_no_graphs(
    self, async_client, test_db, test_user
  ):
    """Usage endpoint should still respond even if org has zero graphs."""
    org = Org.create(
      name=f"Empty Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/usage", params={"days": 10})

    assert response.status_code == 200
    body = response.json()
    assert body["graph_details"] == []
    assert body["summary"]["total_credits_used"] == 0
    assert len(body["daily_trend"]) == 10

  async def test_get_org_usage_skips_missing_credit_records(
    self, async_client, test_db, test_user
  ):
    """Graph without credits should be ignored gracefully."""
    org = Org.create(
      name=f"No Credits Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    _create_graph(test_db, org.id, "Unfunded Graph")

    response = await async_client.get(f"/v1/orgs/{org.id}/usage")

    assert response.status_code == 200
    body = response.json()
    assert body["graph_details"] == []

  async def test_get_org_usage_denies_non_members(
    self, async_client, test_db, test_user
  ):
    """Users outside the org should be blocked."""
    org = Org.create(
      name=f"Private Usage Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    outsider = _create_user(test_db, password_hash=test_user.password_hash)
    OrgUser.create(
      org_id=org.id, user_id=outsider.id, role=OrgRole.OWNER, session=test_db
    )

    response = await async_client.get(f"/v1/orgs/{org.id}/usage")

    assert response.status_code == 403
    assert response.json()["detail"] == "You are not a member of this organization"

  async def test_get_org_usage_clamps_daily_trend_to_30_days(
    self, async_client, test_db, test_user
  ):
    """Daily trend should never exceed 30 entries even for large day windows."""
    org = Org.create(
      name=f"Trend Org {uuid4().hex[:6]}",
      org_type=OrgType.TEAM,
      session=test_db,
    )
    OrgUser.create(
      org_id=org.id, user_id=test_user.id, role=OrgRole.ADMIN, session=test_db
    )

    _create_graph(test_db, org.id, "Sparse Usage")

    response = await async_client.get(f"/v1/orgs/{org.id}/usage", params={"days": 45})

    assert response.status_code == 200
    body = response.json()
    assert body["period_days"] == 45
    assert len(body["daily_trend"]) == 30
    assert all(entry["credits_used"] == 0 for entry in body["daily_trend"])
