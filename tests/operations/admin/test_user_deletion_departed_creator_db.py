"""A removed member who created an org graph can still be deleted.

The org keeps the graph; its credit pool, which names the creator, passes to
the org's owner. Runs against the real platform test database.
"""

import uuid
from decimal import Decimal

import pytest

from robosystems.models.core import GraphCredits, User
from robosystems.operations.admin.user_deletion import (
  execute_user_deletion,
  plan_user_deletion,
)

pytestmark = pytest.mark.integration


def _departed_creator(test_db, sample_graph) -> tuple[User, GraphCredits]:
  """A former member, no longer in any org, still named on the org graph's pool."""
  user = User(
    id=f"departed-{uuid.uuid4().hex[:8]}",
    email=f"departed+{uuid.uuid4().hex[:8]}@example.com",
    name="Departed Admin",
  )
  test_db.add(user)
  test_db.flush()
  pool = GraphCredits.create_for_graph(
    graph_id=sample_graph.graph_id,
    user_id=user.id,
    billing_admin_id=user.id,
    monthly_allocation=Decimal("100"),
    session=test_db,
  )
  test_db.commit()
  return user, pool


def test_the_pool_passes_to_the_org_owner(test_db, sample_graph, test_user):
  departed, pool = _departed_creator(test_db, sample_graph)

  plan = plan_user_deletion(departed.id, test_db)
  assert plan.can_delete, [b.code for b in plan.blockers]

  execute_user_deletion(departed.id, test_db)
  test_db.refresh(pool)
  assert pool.user_id == test_user.id
  assert pool.billing_admin_id == test_user.id
