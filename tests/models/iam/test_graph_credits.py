"""Test GraphCredits and GraphCreditTransaction models functionality."""

import pytest
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from robosystems.models.iam import GraphCredits, Graph, User
from robosystems.models.iam.graph_credits import (
  GraphCreditTransaction,
  CreditTransactionType,
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
      graph_tier=GraphTier.STANDARD.value,
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
    assert GraphTier.STANDARD.value == "standard"
    assert GraphTier.ENTERPRISE.value == "enterprise"
    assert GraphTier.PREMIUM.value == "premium"

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
      credit_multiplier=Decimal("1.0"),
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

  @patch("robosystems.models.iam.graph_credits.get_tier_storage_limit")
  def test_create_for_graph(self, mock_get_tier_limit):
    """Test creating credits for a new graph."""
    mock_get_tier_limit.return_value = 500

    # Create another graph with unique ID
    import uuid

    unique_id2 = str(uuid.uuid4())[:8]
    graph2 = Graph(
      graph_id=f"test_graph_credits_2_{unique_id2}",
      graph_name="Test Graph 2",
      graph_type="entity",
      graph_tier=GraphTier.ENTERPRISE.value,
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
    assert credits.credit_multiplier == Decimal("1.0")  # Always 1.0 in simplified model
    assert credits.storage_limit_gb == Decimal("500")
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

    assert credits.graph_tier == GraphTier.STANDARD.value

    # Test fallback when graph is None
    credits.graph = None
    assert credits.graph_tier == GraphTier.STANDARD.value

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
    now = datetime.now(timezone.utc)
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
    assert credits.current_balance == Decimal("1500")
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
    """Test monthly allocation when not due yet."""
    now = datetime.now(timezone.utc)
    recent = now - timedelta(days=15)

    credits = GraphCredits(
      graph_id=self.graph.graph_id,
      user_id=self.user.id,
      billing_admin_id=self.billing_admin.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
      last_allocation_date=recent,
    )
    self.session.add(credits)
    self.session.commit()

    # Should not allocate since less than 30 days
    result = credits.allocate_monthly_credits(self.session)
    assert result is False
    assert credits.current_balance == Decimal("500")

  def test_allocate_monthly_credits_overflow_protection(self):
    """Test overflow protection during allocation."""
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

    with patch("robosystems.models.iam.graph_credits.logger") as mock_logger:
      result = credits.allocate_monthly_credits(self.session)

    assert result is True
    assert credits.current_balance == Decimal("99999999.99")  # Capped at MAX_BALANCE
    mock_logger.warning.assert_called_once()

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
      last_allocation_date=datetime.now(timezone.utc),
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
    assert summary["graph_tier"] == GraphTier.STANDARD.value
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
      graph_tier=GraphTier.STANDARD.value,
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
