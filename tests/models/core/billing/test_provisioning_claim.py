"""Tests for the provisioning claim and its terminal counterpart.

Provisioning has more than one legitimate trigger, and a provider can also
redeliver an event whose first attempt never finished. Both converge on
``claim_for_provisioning``, which has to hand the work to exactly one caller
while still letting a genuinely dead attempt be retried.
``write_off_stalled_provisioning`` is the other half of the same protocol:
the one place a dead attempt is finally declared dead, guarded by the same
staleness predicate so it can never write off a run that a redelivery has
legitimately re-claimed in the meantime.

These run against a real Postgres because the guarantee being tested is a
database one: the predicate is re-evaluated after the winner's row lock
clears, and no mock reproduces that. The concurrency cases use real threads on
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
  metadata: dict | None = None,
) -> BillingSubscription:
  subscription = BillingSubscription(
    org_id=org_id,
    resource_type="graph",
    resource_id=resource_id,
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=status,
    subscription_metadata=metadata or {},
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


class TestWriteOffStalledProvisioning:
  """The terminal half of the protocol: only a still-stale attempt dies.

  The reaper's sensor read and its write are minutes apart. In that gap a
  redelivery may legitimately re-claim the row — status still ``provisioning``
  but heartbeat fresh — so a status check alone would write off a live run,
  and the expired-graph sensor could then suspend the graph that run had just
  created. The write-off's own predicate re-checks staleness at write time,
  which is what these pin.
  """

  def test_stale_attempt_is_written_off(self, db_session: Session, test_org_id: str):
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
    )

    assert subscription.write_off_stalled_provisioning(db_session) is True
    assert subscription.status == SubscriptionStatus.FAILED.value
    assert subscription.ends_at is not None
    assert subscription.subscription_metadata["error"]
    assert subscription.subscription_metadata["failed_at"]

  def test_fresh_attempt_is_left_alone(self, db_session: Session, test_org_id: str):
    """An attempt inside the staleness window is a live run, not a stall."""
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=1),
    )

    assert subscription.write_off_stalled_provisioning(db_session) is False
    assert subscription.status == SubscriptionStatus.PROVISIONING.value
    assert subscription.ends_at is None

  def test_reclaimed_attempt_is_not_written_off(
    self, db_session: Session, test_org_id: str
  ):
    """The race the predicate exists for.

    The sensor saw the row stale; before the reaper ran, a redelivery
    re-claimed it. The claim stamped a fresh heartbeat, so the write-off must
    find nothing — that run is live and about to build the resource.
    """
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
    )

    assert subscription.claim_for_provisioning(db_session) is True

    assert subscription.write_off_stalled_provisioning(db_session) is False
    assert subscription.status == SubscriptionStatus.PROVISIONING.value

  def test_written_off_row_is_not_claimable(
    self, db_session: Session, test_org_id: str
  ):
    """Once written off, a late redelivery must not resurrect the attempt."""
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
    )

    assert subscription.write_off_stalled_provisioning(db_session) is True
    assert subscription.claim_for_provisioning(db_session) is False
    assert subscription.status == SubscriptionStatus.FAILED.value

  def test_write_off_preserves_prior_metadata(
    self, db_session: Session, test_org_id: str
  ):
    """The error a failed attempt recorded must survive the write-off."""
    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
      metadata={"last_provisioning_error": "no capacity"},
    )

    assert subscription.write_off_stalled_provisioning(db_session) is True
    assert subscription.subscription_metadata["last_provisioning_error"] == (
      "no capacity"
    )
    assert subscription.subscription_metadata["error"]

  def test_concurrent_claim_and_write_off_resolve_to_one_winner(
    self, db_session: Session, test_org_id: str
  ):
    """A stale row contested by a redelivery and the reaper goes to exactly one.

    Both operations carry the same staleness predicate, so whichever commits
    first flips the row out of the other's reach: a won claim stamps a fresh
    heartbeat, a won write-off leaves ``failed``. Either order is correct;
    both winning is the bug.
    """
    import threading

    subscription = _make_subscription(
      db_session,
      test_org_id,
      status=SubscriptionStatus.PROVISIONING.value,
      updated_at=datetime.now(UTC) - timedelta(minutes=STALE_PROVISIONING_MINUTES + 5),
    )
    subscription_id = subscription.id

    engine = db_session.get_bind()
    SessionFactory = sessionmaker(bind=engine)

    results: dict[str, bool] = {}
    results_lock = threading.Lock()
    start = threading.Barrier(2)

    def run(name: str, method: str) -> None:
      session = SessionFactory()
      try:
        row = (
          session.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .one()
        )
        start.wait(timeout=10)
        won = getattr(row, method)(session)
        with results_lock:
          results[name] = won
      finally:
        session.close()

    threads = [
      threading.Thread(target=run, args=("claim", "claim_for_provisioning")),
      threading.Thread(
        target=run, args=("write_off", "write_off_stalled_provisioning")
      ),
    ]
    for thread in threads:
      thread.start()
    for thread in threads:
      thread.join(timeout=30)

    assert len(results) == 2, "both threads must complete"
    assert sum(results.values()) == 1, f"exactly one side must win, got {results}"


class TestOneCreditPoolPerGraph:
  """The stated blast radius of a double provisioning was *two instances and
  two credit pools*. The claim tests above prove the first half; this pins
  the second: the pool is created downstream of the gate, and the database
  itself refuses a second pool for the same graph even if a caller got past
  it."""

  def test_second_pool_for_the_same_graph_is_refused_by_the_database(
    self, db_session: Session, test_org_id: str
  ):
    from decimal import Decimal

    from sqlalchemy.exc import IntegrityError

    from robosystems.models.core import Graph, OrgUser
    from robosystems.models.core.graph.graph_credits import GraphCredits

    owner_id = (
      db_session.query(OrgUser.user_id).filter(OrgUser.org_id == test_org_id).scalar()
    )
    graph = Graph.create(
      graph_id=f"kg{uuid.uuid4().hex[:16]}",
      org_id=test_org_id,
      graph_name="Claimed Graph",
      graph_type="generic",
      graph_tier="ladybug-standard",
      session=db_session,
    )

    GraphCredits.create_for_graph(
      graph_id=graph.graph_id,
      user_id=owner_id,
      billing_admin_id=owner_id,
      monthly_allocation=Decimal("1000"),
      session=db_session,
    )
    assert (
      db_session.query(GraphCredits).filter_by(graph_id=graph.graph_id).count() == 1
    )

    with pytest.raises(IntegrityError):
      GraphCredits.create_for_graph(
        graph_id=graph.graph_id,
        user_id=owner_id,
        billing_admin_id=owner_id,
        monthly_allocation=Decimal("1000"),
        session=db_session,
      )
    db_session.rollback()

    assert (
      db_session.query(GraphCredits).filter_by(graph_id=graph.graph_id).count() == 1
    )

  def test_losing_claimant_provisions_nothing_so_no_second_pool_exists(
    self, db_session: Session, test_org_id: str
  ):
    """End to end on the claim: the winner provisions a graph with a pool and
    records the resource; the loser's claim is refused at the terminal
    condition, so no second graph and no second pool can be created."""
    from decimal import Decimal

    from robosystems.models.core import Graph, OrgUser
    from robosystems.models.core.graph.graph_credits import GraphCredits

    owner_id = (
      db_session.query(OrgUser.user_id).filter(OrgUser.org_id == test_org_id).scalar()
    )
    subscription = _make_subscription(db_session, test_org_id)

    assert subscription.claim_for_provisioning(db_session) is True
    graph = Graph.create(
      graph_id=f"kg{uuid.uuid4().hex[:16]}",
      org_id=test_org_id,
      graph_name="Winner's Graph",
      graph_type="generic",
      graph_tier="ladybug-standard",
      session=db_session,
    )
    GraphCredits.create_for_graph(
      graph_id=graph.graph_id,
      user_id=owner_id,
      billing_admin_id=owner_id,
      monthly_allocation=Decimal("1000"),
      session=db_session,
    )
    subscription.resource_id = graph.graph_id
    subscription.status = SubscriptionStatus.ACTIVE.value
    db_session.commit()

    # A redelivered event arrives after the winner finished
    assert subscription.claim_for_provisioning(db_session) is False

    pools = (
      db_session.query(GraphCredits)
      .join(Graph, Graph.graph_id == GraphCredits.graph_id)
      .filter(Graph.org_id == test_org_id)
      .count()
    )
    assert pools == 1
