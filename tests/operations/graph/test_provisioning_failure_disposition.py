"""A provisioning failure must not decide the subscription's fate.

Disposition after a failed attempt belongs to the caller's protocol: the
webhook path holds the provisioning claim, so the row stays claim-held and
non-terminal — retryable by the provider's redelivery once the staleness
window passes, and written off only by the stalled-provisioning reaper. The
subscription router, which has no redelivery behind it, records the failure
itself.

The regression these pin: the provisioning services used to mark the row
``failed`` in their own exception handlers — a terminal status with no
``ends_at``, which simultaneously discarded the redelivery retry (the claim
refuses terminal rows) and left any infrastructure the attempt had created
invisible to the lifecycle sensors (which filter on ``ends_at IS NOT NULL``).
The webhook-handler tests never saw it because they mock these services;
these tests run the real functions against a real Postgres.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from robosystems.models.core import User
from robosystems.models.core.billing import BillingSubscription, SubscriptionStatus
from robosystems.operations.graph.provisioning_service import (
  run_graph_provisioning,
  run_user_repository_provisioning,
)


@pytest.fixture
def provisioning_fixture(db_session: Session) -> tuple[str, str]:
  """An org with an owner and a subscription mid-claim, as the webhook stages it.

  Returns ``(subscription_id, user_id)``. Status is ``provisioning`` because
  the claim — not the service — owns that transition; the service receives
  the row already claimed.
  """
  from robosystems.models.core import Org, OrgRole, OrgType, OrgUser

  unique_id = str(uuid.uuid4())[:8]

  org = Org(
    id=f"disp_org_{unique_id}",
    name=f"Disposition Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.flush()

  user = User(
    id=f"disp_user_{unique_id}",
    email=f"disp+{unique_id}@example.com",
    name="Disposition Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.flush()
  db_session.add(OrgUser(org_id=org.id, user_id=user.id, role=OrgRole.OWNER))

  subscription = BillingSubscription(
    org_id=org.id,
    resource_type="graph",
    resource_id=None,
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=SubscriptionStatus.PROVISIONING.value,
    subscription_metadata={},
  )
  db_session.add(subscription)
  db_session.commit()

  return subscription.id, user.id


def _reload(db_session: Session, subscription_id: str) -> BillingSubscription:
  """Read the row's committed state, not this session's cached copy."""
  db_session.expire_all()
  return (
    db_session.query(BillingSubscription)
    .filter(BillingSubscription.id == subscription_id)
    .one()
  )


async def test_failed_graph_provisioning_leaves_the_claim_held(
  db_session: Session, provisioning_fixture: tuple[str, str]
):
  """The composed path, not a mock of it: creation fails, the row survives.

  ``failed`` here would be terminal — the claim refuses terminal rows, so the
  provider's redelivery would bounce off and the customer's payment would buy
  nothing retryable.
  """
  subscription_id, user_id = provisioning_fixture

  with patch(
    "robosystems.operations.graph.graph_creation_service.GraphCreationService"
  ) as mock_service_cls:
    mock_service_cls.return_value = MagicMock(
      create=AsyncMock(side_effect=RuntimeError("no capacity"))
    )

    with pytest.raises(RuntimeError, match="no capacity"):
      await run_graph_provisioning(
        operation_id=None,
        subscription_id=subscription_id,
        user_id=user_id,
        tier="ladybug-standard",
      )

  row = _reload(db_session, subscription_id)
  assert row.status == SubscriptionStatus.PROVISIONING.value
  assert row.ends_at is None
  assert row.resource_id is None


async def test_successful_graph_provisioning_activates_and_links(
  db_session: Session, provisioning_fixture: tuple[str, str]
):
  """The success path the failure tests never covered: creation succeeds, so the
  subscription must end ACTIVE with resource_id linked to the new graph."""
  subscription_id, user_id = provisioning_fixture
  graph_id = "kg" + uuid.uuid4().hex[:20]

  with (
    patch(
      "robosystems.operations.graph.graph_creation_service.GraphCreationService"
    ) as mock_service_cls,
    patch(
      "robosystems.operations.graph.provisioning_service.report_asset_materialization",
      new=AsyncMock(),
    ),
  ):
    mock_service_cls.return_value = MagicMock(
      create=AsyncMock(return_value=MagicMock(graph_id=graph_id))
    )
    result = await run_graph_provisioning(
      operation_id=None,
      subscription_id=subscription_id,
      user_id=user_id,
      tier="ladybug-standard",
    )

  assert result["graph_id"] == graph_id
  row = _reload(db_session, subscription_id)
  assert row.status == SubscriptionStatus.ACTIVE.value
  assert row.resource_id == graph_id


async def test_graph_provisioning_activates_despite_session_detach(
  db_session: Session, provisioning_fixture: tuple[str, str]
):
  """The prod failure, reproduced deterministically.

  The long async graph build invalidated the session that loaded the
  subscription, detaching the instance; the completion then ran on that detached
  instance and activate()'s ``session.refresh()`` raised "not persistent within
  this Session", stranding the row at 'provisioning' with resource_id unset even
  though the graph was live and paid. Here we detach the outer session mid-build
  (``expunge_all``) as the deterministic stand-in for that invalidation. The
  completion must still link the graph and activate the subscription — pre-fix
  this raised; the fix re-fetches into a fresh session.
  """
  subscription_id, user_id = provisioning_fixture
  graph_id = "kg" + uuid.uuid4().hex[:20]

  from robosystems.database import get_db_session as _real_get_db_session

  created_sessions: list[Session] = []

  def _recording_get_db_session():
    gen = _real_get_db_session()
    session = next(gen)
    created_sessions.append(session)
    try:
      yield session
    finally:
      try:
        next(gen)
      except StopIteration:
        pass

  async def _succeed_and_detach(_config):
    # The first session opened is the one run_graph_provisioning loaded the
    # subscription into; expunging it detaches the instance, reproducing the
    # connection invalidation the long real build caused in prod.
    created_sessions[0].expunge_all()
    return MagicMock(graph_id=graph_id)

  with (
    patch("robosystems.database.get_db_session", _recording_get_db_session),
    patch(
      "robosystems.operations.graph.graph_creation_service.GraphCreationService"
    ) as mock_service_cls,
    patch(
      "robosystems.operations.graph.provisioning_service.report_asset_materialization",
      new=AsyncMock(),
    ),
  ):
    mock_service_cls.return_value = MagicMock(
      create=AsyncMock(side_effect=_succeed_and_detach)
    )
    result = await run_graph_provisioning(
      operation_id=None,
      subscription_id=subscription_id,
      user_id=user_id,
      tier="ladybug-standard",
    )

  assert result["graph_id"] == graph_id
  row = _reload(db_session, subscription_id)
  assert row.status == SubscriptionStatus.ACTIVE.value
  assert row.resource_id == graph_id


async def test_failed_repository_provisioning_leaves_the_claim_held(
  db_session: Session, provisioning_fixture: tuple[str, str]
):
  """Same property on the repository path, with no mocks at all.

  The user has no billing customer, so the service raises on its own
  validation — the earliest real failure the composed path produces.
  """
  subscription_id, user_id = provisioning_fixture

  with pytest.raises(ValueError, match="Customer not found"):
    await run_user_repository_provisioning(
      operation_id=None,
      subscription_id=subscription_id,
      user_id=user_id,
      repository_name="sec",
    )

  row = _reload(db_session, subscription_id)
  assert row.status == SubscriptionStatus.PROVISIONING.value
  assert row.ends_at is None
