"""Test GraphCredits and GraphCreditTransaction models functionality."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from robosystems.models.core import Graph, GraphCredits, User
from robosystems.models.core.graph.graph_credits import (
  CreditTransactionType,
  GraphCreditTransaction,
  GraphTier,
  safe_float,
)


class TestGraphCredits:
  """Test cases for GraphCredits model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create test users with unique emails
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    self.user = User(
      email=f"graph_credits_user_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.billing_admin = User(
      email=f"graph_credits_billing_{unique_id}@example.com",
      name="Billing Admin",
      password_hash="hashed_password",
    )
    self.session.add_all([self.user, self.billing_admin])
    self.session.commit()

    # Create a test graph with unique ID
    self.graph = Graph(
      graph_id=f"test_graph_credits_{unique_id}",
      graph_name="Test Graph",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_STANDARD.value,
    )
    self.session.add(self.graph)
    self.session.commit()

  def test_safe_float_helper(self):
    """Test the safe_float helper function."""
    assert safe_float(None) == 0.0
    assert safe_float(5) == 5.0
    assert safe_float(Decimal("10.5")) == 10.5
    assert safe_float("20") == 20.0

  def test_graph_tier_enum_values(self):
    """Test GraphTier enum values."""
    assert GraphTier.LADYBUG_STANDARD.value == "ladybug-standard"
    assert GraphTier.LADYBUG_LARGE.value == "ladybug-large"
    assert GraphTier.LADYBUG_XLARGE.value == "ladybug-xlarge"

  def test_credit_transaction_type_enum_values(self):
    """Test CreditTransactionType enum values."""
    assert CreditTransactionType.ALLOCATION.value == "allocation"
    assert CreditTransactionType.CONSUMPTION.value == "consumption"
    assert CreditTransactionType.BONUS.value == "bonus"
    assert CreditTransactionType.REFUND.value == "refund"
    assert CreditTransactionType.EXPIRATION.value == "expiration"

  def test_create_graph_credits(self):
    """Test creating GraphCredits instance."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
      storage_limit_gb=Decimal("500"),
    )

    assert credits.graph_id == self.graph.graph_id
    assert credits.user_id == self.user.id
    assert credits.billing_admin_id == self.billing_admin.id
    assert credits.current_balance == Decimal("1000")
    assert credits.monthly_allocation == Decimal("1000")

    self.session.add(credits)
    self.session.commit()

    assert credits.id is not None
    assert credits.created_at is not None
    assert credits.updated_at is not None

  def test_get_by_graph_id(self):
    """Test getting credits by graph ID."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
    )
    self.session.add(credits)
    self.session.commit()

    result = GraphCredits.get_by_graph_id(self.graph.graph_id, self.session)
    assert result is not None
    assert result.id == credits.id

    # Test non-existent graph
    result = GraphCredits.get_by_graph_id("non_existent", self.session)
    assert result is None

  def test_create_for_graph(self):
    """Test creating credits for a new graph."""
    # Create another graph with unique ID
    import uuid

    unique_id2 = str(uuid.uuid4())[:8]
    graph2 = Graph(
      graph_id=f"test_graph_credits_2_{unique_id2}",
      graph_name="Test Graph 2",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_LARGE.value,
    )
    self.session.add(graph2)
    self.session.commit()

    credits = GraphCredits.create_for_graph(
      graph_id=graph2.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      monthly_allocation=Decimal("5000"),
      session=self.session,
    )

    assert credits.graph_id == graph2.graph_id
    assert credits.monthly_allocation == Decimal("5000")
    assert credits.current_balance == Decimal("5000")
    # Storage limit now comes from GraphTierConfig backup limits (safety cap)
    assert credits.storage_limit_gb > 0
    assert credits.last_allocation_date is not None

    # Check that initial allocation transaction was created
    transactions = (
      self.session.query(GraphCreditTransaction)
      .filter_by(graph_credits_id=credits.id)
      .all()
    )
    assert len(transactions) == 1
    assert transactions[0].transaction_type == CreditTransactionType.ALLOCATION.value
    assert transactions[0].amount == Decimal("5000")

  def test_create_for_graph_nonexistent(self):
    """Test creating credits for non-existent graph."""
    with pytest.raises(ValueError, match="Graph non_existent not found"):
      GraphCredits.create_for_graph(
        graph_id="non_existent",
        user_id=self.user.id,
        billing_admin_id=self.billing_admin.id,
        monthly_allocation=Decimal("1000"),
        session=self.session,
      )

  def test_graph_tier_property(self):
    """Test the graph_tier property."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
    )
    credits.graph = self.graph

    assert credits.graph_tier == GraphTier.LADYBUG_STANDARD.value

    # Test fallback when graph is None
    credits.graph = None
    assert credits.graph_tier == GraphTier.LADYBUG_STANDARD.value

  def test_consume_credits_atomic_success(self):
    """Test successful atomic credit consumption."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("1000"),
    )
    self.session.add(credits)
    self.session.commit()

    result = credits.consume_credits_atomic(
      amount=Decimal("100"),
      operation_type="agent_call",
      operation_description="AI Agent API call",
      session=self.session,
      request_id="req_123",
      user_id=self.user.id,
    )

    assert result["success"] is True
    assert result["credits_consumed"] == 100.0
    assert result["base_cost"] == 100.0
    assert result["old_balance"] == 1000.0
    assert result["new_balance"] == 900.0
    assert "transaction_id" in result

    # Check that transaction was recorded
    transaction = (
      self.session.query(GraphCreditTransaction)
      .filter_by(
        graph_credits_id=credits.id,
        transaction_type=CreditTransactionType.CONSUMPTION.value,
      )
      .first()
    )
    assert transaction is not None
    assert transaction.amount == Decimal("-100")

  def test_consume_credits_atomic_insufficient(self):
    """Test atomic credit consumption with insufficient balance."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("50"),
    )
    self.session.add(credits)
    self.session.commit()

    result = credits.consume_credits_atomic(
      amount=Decimal("100"),
      operation_type="agent_call",
      operation_description="AI Agent API call",
      session=self.session,
    )

    assert result["success"] is False
    assert result["error"] == "Insufficient credits"
    assert result["required_credits"] == 100.0
    assert result["available_credits"] == 50.0

  def test_allocate_monthly_credits(self):
    """Test monthly credit allocation."""
    now = datetime.now(UTC)
    last_month = now - timedelta(days=35)

    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=last_month,
    )
    self.session.add(credits)
    self.session.commit()

    # Should allocate since more than 30 days have passed
    result = credits.allocate_monthly_credits(self.session)
    assert result is True
    # Credits do not roll over — the unspent 500 is not carried forward. This
    # asserted 1500 (a top-up) while the offering page tells customers
    # "Credits do not roll over between billing periods" and
    # `UserRepositoryCredits` resets, documenting itself as "no rollover, same
    # as user graphs". Graph credits were the only ones that accumulated.
    assert credits.current_balance == Decimal("1000")
    assert credits.last_allocation_date > last_month

    # Check allocation transaction was created
    transaction = (
      self.session.query(GraphCreditTransaction)
      .filter_by(
        graph_credits_id=credits.id,
        transaction_type=CreditTransactionType.ALLOCATION.value,
      )
      .first()
    )
    assert transaction is not None
    assert transaction.amount == Decimal("1000")

  def test_allocate_monthly_credits_not_due(self):
    """An allocation earlier in the same calendar month is not repeated."""
    now = datetime.now(UTC)
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=start_of_month,
    )
    self.session.add(credits)
    self.session.commit()

    result = credits.allocate_monthly_credits(self.session)
    assert result is False
    assert credits.current_balance == Decimal("500")

  def test_march_allocation_after_february_first_is_not_skipped(self):
    """February is 28 days, so a day-count gate refuses the 1-Mar run for a
    1-Feb allocation and every graph on the fleet misses March. The gate is
    calendar-month equality, matching the cron and the idempotency key."""
    from unittest.mock import patch

    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("100"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=datetime(2027, 2, 1, tzinfo=UTC),
    )
    self.session.add(credits)
    self.session.commit()

    march_first = datetime(2027, 3, 1, tzinfo=UTC)
    with patch(
      "robosystems.models.core.graph.graph_credits.datetime", wraps=datetime
    ) as mock_dt:
      mock_dt.now.return_value = march_first
      result = credits.allocate_monthly_credits(self.session)

    assert result is True
    assert credits.current_balance == Decimal("1000")
    assert credits.last_allocation_date == march_first

  def test_second_run_in_the_same_calendar_month_is_refused_even_after_30_days(self):
    """1-Jan → 31-Jan is 30 elapsed days, which the old day-count gate let
    through: the idempotency key suppressed the duplicate ledger row but the
    reset still silently restored a month of consumed credits. Same-month is
    refused outright now, so the balance never moves."""
    from unittest.mock import patch

    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("100"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=datetime(2027, 1, 1, tzinfo=UTC),
    )
    self.session.add(credits)
    self.session.commit()

    with patch(
      "robosystems.models.core.graph.graph_credits.datetime", wraps=datetime
    ) as mock_dt:
      mock_dt.now.return_value = datetime(2027, 1, 31, tzinfo=UTC)
      result = credits.allocate_monthly_credits(self.session)

    assert result is False
    assert credits.current_balance == Decimal("100")

  def test_reset_forfeiture_keeps_the_ledger_footing(self):
    """SUM(transactions) == current_balance through creation, consumption,
    and the monthly reset. The discarded remainder gets an EXPIRATION row
    instead of silently vanishing — without it every reset drifts the
    ledger away from the balance permanently."""
    from sqlalchemy import func

    credits = GraphCredits.create_for_graph(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      monthly_allocation=Decimal("1000"),
      session=self.session,
    )
    consumption = credits.consume_credits_atomic(
      amount=Decimal("300"),
      operation_type="agent_call",
      operation_description="AI call",
      session=self.session,
    )
    assert consumption["success"] is True

    credits.last_allocation_date = datetime.now(UTC) - timedelta(days=35)
    self.session.commit()

    assert credits.allocate_monthly_credits(self.session) is True
    self.session.commit()

    expiration = (
      self.session.query(GraphCreditTransaction)
      .filter_by(
        graph_credits_id=credits.id,
        transaction_type=CreditTransactionType.EXPIRATION.value,
      )
      .one()
    )
    assert expiration.amount == Decimal("-700")

    ledger_total = (
      self.session.query(func.sum(GraphCreditTransaction.amount))
      .filter(GraphCreditTransaction.graph_credits_id == credits.id)
      .scalar()
    )
    assert ledger_total == credits.current_balance == Decimal("1000")

  def test_zero_remainder_reset_writes_no_expiration_row(self):
    """A fully-spent pool has nothing to forfeit — no EXPIRATION noise."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("0"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=datetime.now(UTC) - timedelta(days=35),
    )
    self.session.add(credits)
    self.session.commit()

    assert credits.allocate_monthly_credits(self.session) is True

    expirations = (
      self.session.query(GraphCreditTransaction)
      .filter_by(
        graph_credits_id=credits.id,
        transaction_type=CreditTransactionType.EXPIRATION.value,
      )
      .count()
    )
    assert expirations == 0
    assert credits.current_balance == Decimal("1000")

  def test_manual_reset_forfeits_refills_and_leaves_the_monthly_gate_alone(self):
    """An admin mid-month reset forfeits the remainder and refills, with
    both movements in the ledger — and because it does not touch
    last_allocation_date, the scheduled reset still fires when the month
    turns."""
    from unittest.mock import patch

    june_first = datetime(2027, 6, 1, tzinfo=UTC)
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("400"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=june_first,
    )
    self.session.add(credits)
    self.session.commit()

    forfeited = credits.reset_pool(self.session, initiated_by="admin:test")
    self.session.commit()

    assert forfeited == Decimal("400")
    assert credits.current_balance == Decimal("1000")
    assert credits.last_allocation_date == june_first

    expiration = (
      self.session.query(GraphCreditTransaction)
      .filter_by(
        graph_credits_id=credits.id,
        transaction_type=CreditTransactionType.EXPIRATION.value,
      )
      .one()
    )
    assert expiration.amount == Decimal("-400")
    assert expiration.transaction_metadata is not None

    with patch(
      "robosystems.models.core.graph.graph_credits.datetime", wraps=datetime
    ) as mock_dt:
      mock_dt.now.return_value = datetime(2027, 7, 1, tzinfo=UTC)
      assert credits.allocate_monthly_credits(self.session) is True

    assert credits.current_balance == Decimal("1000")

  def test_allocation_never_accumulates_past_the_monthly_amount(self):
    """A large prior balance is replaced, not added to.

    Replaces the old overflow-protection test. That guard capped
    `current_balance + monthly_allocation` at the `Numeric(10, 2)` ceiling —
    a condition only reachable *because* allocation accumulated. With the
    balance replaced, both values share one column type, so the sum can no
    longer be formed and there is nothing to overflow. What matters now is
    the invariant the guard was compensating for.
    """
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("99999999"),
      monthly_allocation=Decimal("2000000"),
      last_allocation_date=None,
    )
    self.session.add(credits)
    self.session.commit()

    result = credits.allocate_monthly_credits(self.session)

    assert result is True
    assert credits.current_balance == Decimal("2000000")

  def test_get_effective_storage_limit(self):
    """Test getting effective storage limit."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      storage_limit_gb=Decimal("500"),
      storage_override_gb=None,
    )

    # Without override
    assert credits.get_effective_storage_limit() == Decimal("500")

    # With override
    credits.storage_override_gb = Decimal("1000")
    assert credits.get_effective_storage_limit() == Decimal("1000")

  def test_check_storage_limit(self):
    """Test storage limit checking."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      storage_limit_gb=Decimal("100"),
      storage_warning_threshold=Decimal("0.8"),
    )

    # Within limit
    result = credits.check_storage_limit(Decimal("50"))
    assert result["within_limit"] is True
    assert result["approaching_limit"] is False
    assert result["usage_percentage"] == 50.0

    # Approaching limit
    result = credits.check_storage_limit(Decimal("85"))
    assert result["within_limit"] is True
    assert result["approaching_limit"] is True
    assert result["needs_warning"] is True

    # Over limit
    result = credits.check_storage_limit(Decimal("150"))
    assert result["within_limit"] is False
    assert result["approaching_limit"] is True

  def test_set_storage_override(self):
    """Test setting storage override limit."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      storage_limit_gb=Decimal("500"),
    )
    self.session.add(credits)
    self.session.commit()

    credits.set_storage_override(
      new_limit_gb=Decimal("1000"),
      admin_user_id=self.billing_admin.id,
      reason="Customer requested increase",
      session=self.session,
    )

    assert credits.storage_override_gb == Decimal("1000")
    assert credits.get_effective_storage_limit() == Decimal("1000")

    # Check audit transaction was created
    transaction = (
      self.session.query(GraphCreditTransaction)
      .filter_by(graph_credits_id=credits.id)
      .first()
    )
    assert transaction is not None
    assert "Storage limit override" in transaction.description

  def test_update_storage_warning(self):
    """Test updating storage warning timestamp."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
    )
    self.session.add(credits)
    self.session.commit()

    assert credits.last_storage_warning_at is None

    credits.update_storage_warning(self.session)
    assert credits.last_storage_warning_at is not None

  def test_get_usage_summary(self):
    """Test getting usage summary."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      monthly_allocation=Decimal("1000"),
      current_balance=Decimal("700"),
      storage_limit_gb=Decimal("500"),
      last_allocation_date=datetime.now(UTC),
    )
    credits.graph = self.graph
    self.session.add(credits)
    self.session.commit()

    # Create some consumption transactions
    for i in range(3):
      transaction = GraphCreditTransaction(
        graph_credits_id=credits.id,
        graph_id=self.graph.graph_id,
        transaction_type=CreditTransactionType.CONSUMPTION.value,
        amount=Decimal("-100"),
        description=f"Test consumption {i}",
      )
      self.session.add(transaction)
    self.session.commit()

    summary = credits.get_usage_summary(self.session)

    assert summary["graph_id"] == self.graph.graph_id
    assert summary["graph_tier"] == GraphTier.LADYBUG_STANDARD.value
    assert summary["monthly_allocation"] == 1000.0
    assert summary["consumed_this_month"] == 300.0
    assert summary["transaction_count"] == 3
    assert summary["storage_limit_gb"] == 500.0
    assert summary["effective_storage_limit_gb"] == 500.0

  def test_repr_method(self):
    """Test string representation."""
    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("1500"),
    )

    repr_str = repr(credits)
    assert f"<GraphCredits(graph_id={self.graph.graph_id}" in repr_str
    assert "balance=1500" in repr_str


class TestGraphCreditTransaction:
  """Test cases for GraphCreditTransaction model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create test users with unique emails
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    self.user = User(
      email=f"graph_transaction_user_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

    # Create test graph with unique ID
    self.graph = Graph(
      graph_id=f"test_graph_trans_{unique_id}",
      graph_name="Test Graph",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_STANDARD.value,
    )
    self.session.add(self.graph)
    self.session.commit()

    # Create test credits
    self.credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.user.id,
      current_balance=Decimal("1000"),
    )
    self.session.add(self.credits)
    self.session.commit()

  def test_create_transaction(self):
    """Test creating a transaction."""
    transaction = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.ALLOCATION.value,
      amount=Decimal("1000"),
      description="Test allocation",
    )

    assert transaction.graph_credits_id == self.credits.id
    assert transaction.graph_id == self.graph.graph_id
    assert transaction.amount == Decimal("1000")

    self.session.add(transaction)
    self.session.commit()

    assert transaction.id is not None
    assert transaction.created_at is not None

  def test_transaction_with_metadata(self):
    """Test transaction with metadata."""
    metadata = {"test": "data", "nested": {"value": 123}}

    transaction = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.CONSUMPTION.value,
      amount=Decimal("-50"),
      description="Test consumption",
      transaction_metadata=json.dumps(metadata),
    )

    self.session.add(transaction)
    self.session.commit()

    # Verify metadata was stored
    assert transaction.transaction_metadata is not None
    stored_metadata = json.loads(transaction.transaction_metadata)
    assert stored_metadata == metadata

  def test_idempotency_key(self):
    """Test idempotency key prevents duplicates."""
    idempotency_key = "unique_key_123"

    # Create first transaction
    transaction1 = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.ALLOCATION.value,
      amount=Decimal("1000"),
      description="Test allocation",
      idempotency_key=idempotency_key,
    )
    self.session.add(transaction1)
    self.session.commit()

    # Try to create duplicate with same idempotency key
    transaction2 = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.ALLOCATION.value,
      amount=Decimal("1000"),
      description="Duplicate allocation",
      idempotency_key=idempotency_key,
    )
    self.session.add(transaction2)

    with pytest.raises(Exception):  # Should raise integrity error
      self.session.commit()

  def test_transaction_relationships(self):
    """Test transaction relationships."""
    transaction = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.BONUS.value,
      amount=Decimal("500"),
      description="Bonus credits",
    )
    self.session.add(transaction)
    self.session.commit()

    # Test relationship
    assert transaction.graph_credits == self.credits
    assert transaction in self.credits.transactions

  def test_repr_method(self):
    """Test string representation."""
    transaction = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.REFUND.value,
      amount=Decimal("100"),
      description="Test refund",
    )
    self.session.add(transaction)
    self.session.commit()

    repr_str = repr(transaction)
    assert f"<GraphCreditTransaction(id={transaction.id}" in repr_str
    assert "type=refund" in repr_str
    assert "amount=100" in repr_str

  def test_optional_fields(self):
    """Test optional fields can be None."""
    transaction = GraphCreditTransaction(
      graph_credits_id=self.credits.id,
      graph_id=self.graph.graph_id,
      transaction_type=CreditTransactionType.ALLOCATION.value,
      amount=Decimal("1000"),
      description="Test allocation",
      idempotency_key=None,
      request_id=None,
      operation_id=None,
      user_id=None,
      transaction_metadata=None,
    )

    self.session.add(transaction)
    self.session.commit()

    assert transaction.idempotency_key is None
    assert transaction.request_id is None
    assert transaction.operation_id is None
    assert transaction.user_id is None
    assert transaction.transaction_metadata is None


class TestSingleBalanceDefinition:
  """`current_balance` is the one definition of a graph's spendable credits.

  Two definitions previously coexisted and were cached under the same key:
  `consume_credits_atomic` decremented and gated on the `current_balance`
  column, while the read paths recomputed
  `monthly_allocation - consumed_this_month`. Because allocation *added* to the
  balance, the two drifted apart permanently — so the same pool answered
  differently depending on which one you asked, and a caller could be refused
  at zero while the column it actually spends from held thousands.
  """

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    import uuid

    self.session = db_session
    unique_id = str(uuid.uuid4())[:8]

    self.user = User(
      email=f"balance_def_user_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

    self.graph = Graph(
      graph_id=f"test_balance_def_{unique_id}",
      graph_name="Test Graph",
      graph_type="entity",
      graph_tier=GraphTier.LADYBUG_STANDARD.value,
    )
    self.session.add(self.graph)
    self.session.commit()

    self.credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.user.id,
      current_balance=Decimal("250"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(self.credits)
    self.session.commit()

  def test_usage_summary_reports_the_column_the_consume_path_gates_on(self):
    """A balance that has diverged from `allocation - consumed` is reported
    as-is; recomputing it here is what made the two disagree."""
    summary = self.credits.get_usage_summary(self.session)

    assert summary["current_balance"] == 250.0
    assert summary["monthly_allocation"] == 1000.0

  def test_allocation_makes_the_two_definitions_agree(self):
    """After a reset with nothing consumed since, the real column and the
    derived figure coincide — the property that stops them drifting again."""
    self.credits.last_allocation_date = datetime.now(UTC) - timedelta(days=35)
    self.session.commit()

    assert self.credits.allocate_monthly_credits(self.session) is True

    summary = self.credits.get_usage_summary(self.session)
    derived = float(self.credits.monthly_allocation) - summary["consumed_this_month"]
    assert summary["current_balance"] == derived == 1000.0

  def test_consuming_moves_both_in_step(self):
    """Consumption decrements the column and is reflected in the summary, so
    the two stay equal through a normal spend."""
    self.credits.current_balance = Decimal("1000")
    self.credits.last_allocation_date = datetime.now(UTC)
    self.session.commit()

    self.credits.consume_credits_atomic(
      amount=Decimal("300"),
      operation_type="agent_call",
      operation_description="AI call",
      session=self.session,
      user_id=self.user.id,
    )

    summary = self.credits.get_usage_summary(self.session)
    assert summary["current_balance"] == 700.0
    assert summary["consumed_this_month"] == 300.0
