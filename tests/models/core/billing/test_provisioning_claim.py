"""Tests for the provisioning claim — the idempotency gate at the sink.

Provisioning has more than one legitimate trigger, and a provider can also
redeliver an event whose first attempt never finished. Both converge on
``claim_for_provisioning``, which has to hand the work to exactly one caller
while still letting a genuinely dead attempt be retried.

These run against a real Postgres because the guarantee being tested is a
database one: the predicate is re-evaluated after the winner's row lock
clears, and no mock reproduces that. The concurrency case uses real threads on
independent connections for the same reason — asserting the mutex against a
mocked session would only assert that the mock was called.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session, sessionmaker

from robosystems.models.core import User
from robosystems.models.core.billing import BillingSubscription, SubscriptionStatus
from robosystems.models.core.billing.subscription import STALE_PROVISIONING_MINUTES


@pytest.fixture
def test_org_id(db_session: Session) -> str:
  """Create an org with an owner, and return the org id."""
  from robosystems.models.core import Org, OrgRole, OrgType, OrgUser

  unique_id = str(uuid.uuid4())[:8]

  org = Org(
    id=f"claim_org_{unique_id}",
    name=f"Claim Org {unique_id}",
    org_type=OrgType.PERSONAL,
  )
  db_session.add(org)
  db_session.flush()

  user = User(
    id=f"claim_user_{unique_id}",
    email=f"claim+{unique_id}@example.com",
    name="Claim Test User",
    password_hash="test_hash",
  )
  db_session.add(user)
  db_session.flush()

  db_session.add(OrgUser(org_id=org.id, user_id=user.id, role=OrgRole.OWNER))
  db_session.commit()
  return org.id


def _make_subscription(
  session: Session,
  org_id: str,
  *,
  status: str = SubscriptionStatus.PENDING_PAYMENT.value,
  resource_id: str | None = None,
  updated_at: datetime | None = None,
) -> BillingSubscription:
  subscription = BillingSubscription(
    org_id=org_id,
    resource_type="graph",
    resource_id=resource_id,
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=status,
    subscription_metadata={},
  )
  session.add(subscription)
  session.commit()

  if updated_at is not None:
    # onupdate would stamp "now" on an ORM write, so age the row in SQL.
    session.query(BillingSubscription).filter(
      BillingSubscription.id == subscription.id
    ).update({BillingSubscription.updated_at: updated_at}, synchronize_session=False)
    session.commit()
    session.refresh(subscription)

  return subscription


class TestClaimAdmitsExactlyOneCaller:
  """The core property: two triggers, one provisioning run."""

  def test_second_sequential_caller_is_refused(
    self, db_session: Session, test_org_id: str
  ):
    """The shape the two provider events actually take in production.

    ``checkout.session.completed`` and ``invoice.payment_succeeded`` arrive
    close together but usually not simultaneously; the second must find the
    door shut even though the row it sees is in a status its own caller
    considers provisionable.
    """
    subscription = _make_subscription(db_session, test_org_id)

    assert subscription.claim_for_provisioning(db_session) is True
    assert subscription.status == SubscriptionStatus.PROVISIONING.value

    assert subscription.claim_for_provisioning(db_session) is False

  def test_concurrent_callers_resolve_to_one_winner(
    self, db_session: Session, test_org_id: str
  ):
    """Two threads on independent connections, one winner.

    This is the case the per-event-id idempotency cannot cover, because the
    two events are different events. Without the gate both callers pass the
    status check and both provision.
    """
    import threading

    subscription = _make_subscription(db_session, test_org_id)
    subscription_id = subscription.id

    engine = db_session.get_bind()
    SessionFactory = sessionmaker(bind=engine)

    results: list[bool] = []
    results_lock = threading.Lock()
    start = threading.Barrier(2)

    def claim() -> None:
      session = SessionFactory()
      try:
        row = (
          session.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .one()
        )
        start.wait(timeout=10)
        won = row.claim_for_provisioning(session)
        with results_lock:
          results.append(won)
      finally:
        session.close()

    threads = [threading.Thread(target=claim) for _ in range(2)]
    for thread in threads:
      thread.start()
    for thread in threads:
      thread.join(timeout=30)

    assert len(results) == 2, "both threads must complete"
    assert sum(results) == 1, f"exactly one claim must win, got {results}"

  def test_many_concurrent_callers_still_resolve_to_one(
    self, db_session: Session, test_org_id: str
  ):
    """The gate is at the sink, so adding callers must not widen the hole."""
    import threading

    subscription = _make_subscription(db_session, test_org_id)
    subscription_id = subscription.id

    engine = db_session.get_bind()
    SessionFactory = sessionmaker(bind=engine)

    caller_count = 6
    results: list[bool] = []
    results_lock = threading.Lock()
    start = threading.Barrier(caller_count)

    def claim() -> None:
      session = SessionFactory()
      try:
        row = (
          session.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .one()
        )
        start.wait(timeout=10)
        won = row.claim_for_provisioning(session)
        with results_lock:
          results.append(won)
      finally:
        session.close()

    threads = [threading.Thread(target=claim) for _ in range(caller_count)]
    for thread in threads:
      thread.start()
    for thread in threads:
      thread.join(timeout=30)

    assert len(results) == caller_count
    assert sum(results) == 1, f"exactly one claim must win, got {results}"


class TestTerminalCondition:
  """Once the resource exists, nothing re-provisions."""

  def test_claim_refused_when_resource_already_provisioned(
    self, db_session: Session, test_org_id: str
  ):
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PENDING_PAYMENT.value,
      resource_id="kg01234567890abcdef",
    )

    assert subscription.claim_for_provisioning(db_session) is False
    assert subscription.status == SubscriptionStatus.PENDING_PAYMENT.value

  def test_stale_provisioning_with_a_resource_is_still_refused(
    self, db_session: Session, test_org_id: str
  ):
    """Staleness must not override the terminal condition.

    A provisioning run that created the resource and then died is exactly the
    row that is both stale and already provisioned. Re-claiming it would build
    a second graph for one payment.
    """
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      resource_id="kg01234567890abcdef",
      updated_at=datetime.now(UTC) - timedelta(hours=6),
    )

    assert subscription.claim_for_provisioning(db_session) is False

  def test_active_subscription_is_refused(self, db_session: Session, test_org_id: str):
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.ACTIVE.value,
      resource_id="kg01234567890abcdef",
    )

    assert subscription.claim_for_provisioning(db_session) is False
    assert subscription.status == SubscriptionStatus.ACTIVE.value


class TestStaleReclaim:
  """A dead attempt has to be retryable, or a paid customer gets nothing."""

  def test_stale_provisioning_row_can_be_reclaimed(
    self, db_session: Session, test_org_id: str
  ):
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
    )

    assert subscription.claim_for_provisioning(db_session) is True

  def test_fresh_provisioning_row_is_not_reclaimed(
    self, db_session: Session, test_org_id: str
  ):
    """An attempt still in flight must not be duplicated by a redelivery."""
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=1),
    )

    assert subscription.claim_for_provisioning(db_session) is False

  def test_claim_stamps_updated_at_as_its_heartbeat(
    self, db_session: Session, test_org_id: str
  ):
    """The staleness window is measured against the claim's own stamp."""
    stale_at = datetime.now(UTC) - timedelta(hours=2)
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=stale_at,
    )

    assert subscription.claim_for_provisioning(db_session) is True

    refreshed_at = subscription.updated_at
    assert refreshed_at is not None
    assert refreshed_at.replace(tzinfo=UTC) > stale_at

    # Freshly stamped, so the next caller is refused.
    assert subscription.claim_for_provisioning(db_session) is False
