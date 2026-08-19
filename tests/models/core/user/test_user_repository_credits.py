"""Test UserRepositoryCredits and UserRepositoryCreditTransaction models functionality."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest
from sqlalchemy.exc import SQLAlchemyError

from robosystems.models.core import User, UserRepository, UserRepositoryCredits
from robosystems.models.core.user.user_repository import (
  RepositoryAccessLevel,
  RepositoryType,
)
from robosystems.models.core.user.user_repository_credits import (
  UserRepositoryCreditTransaction,
  UserRepositoryCreditTransactionType,
  safe_bool,
  safe_float,
  safe_str,
)


class TestUserRepositoryCredits:
  """Test cases for UserRepositoryCredits model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create test user with unique email
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    self.user = User(
      email=f"user_repo_credits_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

    # Create Graph repository (required for foreign key)
    from robosystems.models.core import Graph

    Graph.find_or_create_repository(
      graph_id="sec",
      graph_name="SEC Public Filings",
      repository_type="sec",
      session=self.session,
    )

    # Create test user repository access
    self.repo_access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="starter",
      is_active=True,
      monthly_credit_allocation=5000,
    )
    self.session.add(self.repo_access)
    self.session.commit()

  def test_safe_helper_functions(self):
    """Test safe conversion helper functions."""
    # safe_float
    assert safe_float(None) == 0.0
    assert safe_float(5) == 5.0
    assert safe_float(Decimal("10.5")) == 10.5

    # safe_str
    assert safe_str(None) == ""
    assert safe_str("test") == "test"
    assert safe_str(123) == "123"

    # safe_bool
    assert safe_bool(None) is False
    assert safe_bool(True) is True
    assert safe_bool(False) is False
    assert safe_bool(1) is True

  def test_transaction_type_enum(self):
    """Test UserRepositoryCreditTransactionType enum values."""
    assert UserRepositoryCreditTransactionType.ALLOCATION.value == "allocation"
    assert UserRepositoryCreditTransactionType.CONSUMPTION.value == "consumption"
    assert UserRepositoryCreditTransactionType.BONUS.value == "bonus"
    assert UserRepositoryCreditTransactionType.REFUND.value == "refund"
    assert UserRepositoryCreditTransactionType.ROLLOVER.value == "rollover"
    assert UserRepositoryCreditTransactionType.EXPIRATION.value == "expiration"

  def test_create_user_repository_credits(self):
    """Test creating UserRepositoryCredits instance."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
      credits_consumed_this_month=Decimal("100"),
    )

    assert credits.user_repository_id == self.repo_access.id
    assert credits.current_balance == Decimal("1000")
    assert credits.monthly_allocation == Decimal("1000")
    assert credits.credits_consumed_this_month == Decimal("100")

    self.session.add(credits)
    self.session.commit()

    assert credits.id is not None
    assert credits.created_at is not None
    assert credits.updated_at is not None

  def test_create_for_access(self):
    """Test creating credits for a new access record."""
    # Create Graph repository (required for foreign key)
    from robosystems.models.core import Graph

    Graph.find_or_create_repository(
      graph_id="industry_tech",
      graph_name="Industry Tech Repository",
      repository_type="industry",
      session=self.session,
    )

    # Create another repo access
    repo_access2 = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.INDUSTRY,
      repository_name="industry_tech",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="advanced",
      is_active=True,
    )
    self.session.add(repo_access2)
    self.session.commit()

    credits = UserRepositoryCredits.create_for_access(
      access_id=repo_access2.id,
      repository_type=RepositoryType.INDUSTRY,
      repository_plan="advanced",
      monthly_allocation=10000,
      session=self.session,
    )

    assert credits.user_repository_id == repo_access2.id
    assert credits.current_balance == Decimal("10000")
    assert credits.monthly_allocation == Decimal("10000")
    assert credits.allows_rollover is False  # No rollover for repository credits
    assert credits.last_allocation_date is not None
    assert credits.next_allocation_date is not None

    # Check initial allocation transaction was created
    transactions = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .all()
    )
    assert len(transactions) == 1
    assert (
      transactions[0].transaction_type
      == UserRepositoryCreditTransactionType.ALLOCATION.value
    )
    assert transactions[0].amount == Decimal("10000")

  def test_create_for_access_rollback_on_error(self):
    """Test rollback on error during credit creation."""
    # Create Graph repository (required for foreign key)
    from robosystems.models.core import Graph

    Graph.find_or_create_repository(
      graph_id="sec2",
      graph_name="SEC Repository 2",
      repository_type="sec",
      session=self.session,
    )

    repo_access2 = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec2",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="starter",
    )
    self.session.add(repo_access2)
    self.session.commit()

    with patch.object(self.session, "commit", side_effect=SQLAlchemyError("DB error")):
      with pytest.raises(SQLAlchemyError):
        UserRepositoryCredits.create_for_access(
          access_id=repo_access2.id,
          repository_type=RepositoryType.SEC,
          repository_plan="starter",
          monthly_allocation=5000,
          session=self.session,
        )

  def test_consume_credits_success(self):
    """Test successful credit consumption."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
      is_active=True,
    )
    credits.user_repository = self.repo_access  # Set relationship
    self.session.add(credits)
    self.session.commit()

    with patch("robosystems.security.SecurityAuditLogger") as mock_logger:
      result = credits.consume_credits(
        amount=Decimal("100"),
        repository_name="sec",
        operation_type="query",
        session=self.session,
        metadata={"query_type": "cypher"},
      )

    assert result is True

    # Refresh to get updated values
    self.session.refresh(credits)
    assert credits.current_balance == Decimal("900")
    assert credits.credits_consumed_this_month == Decimal("100")
    assert credits.last_consumption_at is not None

    # Check transaction was created
    transaction = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(
        credit_pool_id=credits.id,
        transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION.value,
      )
      .first()
    )
    assert transaction is not None
    assert transaction.amount == Decimal("-100")

    # Check audit log was called
    mock_logger.log_financial_transaction.assert_called_once()

  def test_consume_credits_insufficient_balance(self):
    """Test credit consumption with insufficient balance."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("50"),
      monthly_allocation=Decimal("1000"),
      is_active=True,
    )
    credits.user_repository = self.repo_access
    self.session.add(credits)
    self.session.commit()

    with patch(
      "robosystems.models.core.user.user_repository_credits.logger"
    ) as mock_logger:
      result = credits.consume_credits(
        amount=Decimal("100"),
        repository_name="sec",
        operation_type="query",
        session=self.session,
      )

    # Billing is post-hoc: the call already happened, so the short pool is
    # drained to zero (what was there is billed, the shortfall recorded) and
    # the call still returns False so the run stops.
    assert result is False
    self.session.refresh(credits)
    assert credits.current_balance == Decimal("0")
    assert credits.credits_consumed_this_month == Decimal("50")
    mock_logger.warning.assert_called()

    drain_tx = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .order_by(UserRepositoryCreditTransaction.created_at.desc())
      .first()
    )
    assert drain_tx is not None
    assert drain_tx.amount == Decimal("-50")
    meta = drain_tx.get_metadata()
    assert meta["drained_to_zero"] is True
    assert Decimal(meta["shortfall"]) == Decimal("50")

  def test_consume_credits_on_an_empty_pool_records_nothing(self):
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("0"),
      monthly_allocation=Decimal("1000"),
      is_active=True,
    )
    credits.user_repository = self.repo_access
    self.session.add(credits)
    self.session.commit()

    result = credits.consume_credits(
      amount=Decimal("100"),
      repository_name="sec",
      operation_type="query",
      session=self.session,
    )

    assert result is False
    self.session.refresh(credits)
    assert credits.current_balance == Decimal("0")
    assert (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .count()
      == 0
    )

  def test_consume_credits_inactive_pool(self):
    """Test credit consumption from inactive pool."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
      is_active=False,
    )
    self.session.add(credits)
    self.session.commit()

    with patch(
      "robosystems.models.core.user.user_repository_credits.logger"
    ) as mock_logger:
      result = credits.consume_credits(
        amount=Decimal("100"),
        repository_name="sec",
        operation_type="query",
        session=self.session,
      )

    assert result is False
    mock_logger.warning.assert_called()

  def test_allocate_monthly_credits(self):
    """Test monthly credit allocation."""
    past_date = datetime.now(UTC) - timedelta(days=35)

    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
      credits_consumed_this_month=Decimal("800"),
      last_allocation_date=past_date,
      next_allocation_date=past_date,
    )
    self.session.add(credits)
    self.session.commit()

    # Should allocate since due
    result = credits.allocate_monthly_credits(self.session)

    assert result is True
    assert credits.current_balance == Decimal("1000")  # Reset to monthly allocation
    assert credits.credits_consumed_this_month == Decimal("0")  # Reset
    assert credits.rollover_credits == Decimal("0")  # No rollover
    assert credits.last_allocation_date > past_date
    assert credits.next_allocation_date > datetime.now(UTC)

    # Check allocation transaction was created
    transaction = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(
        credit_pool_id=credits.id,
        transaction_type=UserRepositoryCreditTransactionType.ALLOCATION.value,
      )
      .first()
    )
    assert transaction is not None
    assert transaction.amount == Decimal("1000")

  def test_allocate_monthly_credits_not_due(self):
    """Test allocation when not due yet."""
    future_date = datetime.now(UTC) + timedelta(days=15)

    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
      next_allocation_date=future_date,
    )
    self.session.add(credits)
    self.session.commit()

    result = credits.allocate_monthly_credits(self.session)

    assert result is False
    assert credits.current_balance == Decimal("500")  # Unchanged

  def test_allocate_monthly_credits_overflow_protection(self):
    """Test overflow protection during allocation."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("99999999"),
      monthly_allocation=Decimal("2000000"),
      next_allocation_date=None,
    )
    self.session.add(credits)
    self.session.commit()

    with patch("robosystems.models.core.user.user_repository_credits.logger"):
      result = credits.allocate_monthly_credits(self.session)

    assert result is True
    # Should be reset to monthly allocation (no rollover)
    assert credits.current_balance == Decimal("2000000")
    # No warning since we don't add to existing balance

  def test_update_monthly_allocation_immediate_credit(self):
    """Test updating monthly allocation with immediate credit."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(credits)
    self.session.commit()

    credits.update_monthly_allocation(
      new_allocation=Decimal("2000"), session=self.session, immediate_credit=True
    )

    assert credits.monthly_allocation == Decimal("2000")
    assert credits.current_balance == Decimal("1500")  # 500 + 1000 difference

    # Check bonus transaction was created
    transaction = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(
        credit_pool_id=credits.id,
        transaction_type=UserRepositoryCreditTransactionType.BONUS.value,
      )
      .first()
    )
    assert transaction is not None
    assert transaction.amount == Decimal("1000")

  def test_update_monthly_allocation_no_immediate_credit(self):
    """Test updating monthly allocation without immediate credit."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("500"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(credits)
    self.session.commit()

    credits.update_monthly_allocation(
      new_allocation=Decimal("2000"), session=self.session, immediate_credit=False
    )

    assert credits.monthly_allocation == Decimal("2000")
    assert credits.current_balance == Decimal("500")  # Unchanged

    # No bonus transaction
    transaction = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(
        credit_pool_id=credits.id,
        transaction_type=UserRepositoryCreditTransactionType.BONUS.value,
      )
      .first()
    )
    assert transaction is None

  def test_update_monthly_allocation_overflow_protection(self):
    """Test overflow protection when updating allocation."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("99999000"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(credits)
    self.session.commit()

    with patch(
      "robosystems.models.core.user.user_repository_credits.logger"
    ) as mock_logger:
      credits.update_monthly_allocation(
        new_allocation=Decimal("10000"), session=self.session, immediate_credit=True
      )

    assert credits.current_balance == Decimal("99999999.99")  # Capped at MAX_BALANCE
    mock_logger.warning.assert_called_once()

  def _make_pool(self, *, balance: str, allocation: str) -> UserRepositoryCredits:
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal(balance),
      monthly_allocation=Decimal(allocation),
    )
    self.session.add(credits)
    self.session.commit()
    return credits

  def _transaction_sum(self, credits: UserRepositoryCredits) -> Decimal:
    rows = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .all()
    )
    return sum((row.amount for row in rows), Decimal("0"))

  def test_downgrade_deducts_the_allocation_difference(self):
    """A downgrade takes back what the mirror upgrade would have granted."""
    credits = self._make_pool(balance="2000", allocation="2000")

    credits.update_monthly_allocation(
      new_allocation=Decimal("1000"), session=self.session, immediate_credit=True
    )

    assert credits.monthly_allocation == Decimal("1000")
    assert credits.current_balance == Decimal("1000")
    assert self._transaction_sum(credits) == Decimal("-1000")

  def test_cycling_plans_does_not_accumulate_credits(self):
    """The property the whole fix exists for.

    Granting on the way up without deducting on the way down compounds: each
    upgrade's delta is computed against the lowered allocation, so the same
    net position can be bought over and over. Both the balance and the
    transaction ledger must come back to the single-upgrade value no matter
    how many times the cycle runs.
    """
    credits = self._make_pool(balance="1000", allocation="1000")

    for _ in range(3):
      credits.update_monthly_allocation(
        new_allocation=Decimal("5000"), session=self.session, immediate_credit=True
      )
      credits.update_monthly_allocation(
        new_allocation=Decimal("1000"), session=self.session, immediate_credit=True
      )

    # One last upgrade, so the end state is the same net change as a single
    # 1000 -> 5000 move.
    credits.update_monthly_allocation(
      new_allocation=Decimal("5000"), session=self.session, immediate_credit=True
    )

    assert credits.monthly_allocation == Decimal("5000")
    assert credits.current_balance == Decimal("5000")
    assert self._transaction_sum(credits) == Decimal("4000")

  def test_downgrade_cannot_drive_the_balance_negative(self):
    """Credits already spent are not clawed back into an overage."""
    credits = self._make_pool(balance="200", allocation="5000")

    credits.update_monthly_allocation(
      new_allocation=Decimal("1000"), session=self.session, immediate_credit=True
    )

    assert credits.current_balance == Decimal("0")
    assert self._transaction_sum(credits) == Decimal("-200")

    transaction = (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .one()
    )
    uncollected = json.loads(transaction.transaction_metadata)["uncollected"]
    assert Decimal(uncollected) == Decimal("3800")

  def test_downgrade_on_an_empty_pool_writes_no_transaction(self):
    credits = self._make_pool(balance="0", allocation="5000")

    credits.update_monthly_allocation(
      new_allocation=Decimal("1000"), session=self.session, immediate_credit=True
    )

    assert credits.current_balance == Decimal("0")
    assert (
      self.session.query(UserRepositoryCreditTransaction)
      .filter_by(credit_pool_id=credits.id)
      .count()
      == 0
    )

  def test_downgrade_without_immediate_credit_leaves_the_balance_alone(self):
    """The opt-out has to be symmetric too, or it becomes a free downgrade."""
    credits = self._make_pool(balance="2000", allocation="2000")

    credits.update_monthly_allocation(
      new_allocation=Decimal("1000"), session=self.session, immediate_credit=False
    )

    assert credits.monthly_allocation == Decimal("1000")
    assert credits.current_balance == Decimal("2000")
    assert self._transaction_sum(credits) == Decimal("0")

  def test_repeating_the_same_change_is_a_no_op(self):
    """A retried plan change must not adjust twice.

    Idempotency falls out of the delta being computed against the stored
    allocation rather than being tracked separately.
    """
    credits = self._make_pool(balance="1000", allocation="1000")

    for _ in range(3):
      credits.update_monthly_allocation(
        new_allocation=Decimal("3000"), session=self.session, immediate_credit=True
      )

    assert credits.current_balance == Decimal("3000")
    assert self._transaction_sum(credits) == Decimal("2000")

  def test_get_summary(self):
    """Test getting credit summary."""
    now = datetime.now(UTC)
    future = now + timedelta(days=30)

    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("700"),
      monthly_allocation=Decimal("1000"),
      credits_consumed_this_month=Decimal("300"),
      rollover_credits=Decimal("0"),
      allows_rollover=False,
      is_active=True,
      last_allocation_date=now,
      next_allocation_date=future,
    )

    summary = credits.get_summary()

    assert summary["current_balance"] == 700.0
    assert summary["monthly_allocation"] == 1000.0
    assert summary["consumed_this_month"] == 300.0
    assert summary["usage_percentage"] == 30.0
    assert summary["rollover_credits"] == 0.0
    assert summary["allows_rollover"] is False
    assert summary["is_active"] is True
    assert summary["last_allocation_date"] == now.isoformat()
    assert summary["next_allocation_date"] == future.isoformat()

  def test_get_summary_with_nulls(self):
    """Test getting summary with null dates."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("0"),
      monthly_allocation=Decimal("0"),
      last_allocation_date=None,
      next_allocation_date=None,
    )

    summary = credits.get_summary()

    assert summary["last_allocation_date"] is None
    assert summary["next_allocation_date"] is None
    assert summary["usage_percentage"] == 0.0

  def test_repr_method(self):
    """Test string representation."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id, current_balance=Decimal("1500")
    )

    repr_str = repr(credits)
    assert f"<UserRepositoryCredits(user_repo={self.repo_access.id}" in repr_str
    assert "balance=1500" in repr_str

  def test_relationship_with_user_repository(self):
    """Test relationship with UserRepository."""
    credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(credits)
    self.session.commit()

    # Set up relationship
    self.repo_access.user_credits = credits
    credits.user_repository = self.repo_access
    self.session.commit()

    # Test relationship access
    assert credits.user_repository == self.repo_access
    assert self.repo_access.user_credits == credits

  def test_unique_constraint_on_user_repository_id(self):
    """Test unique constraint on user_repository_id."""
    # Create first credits
    credits1 = UserRepositoryCredits(
      user_repository_id=self.repo_access.id, current_balance=Decimal("1000")
    )
    self.session.add(credits1)
    self.session.commit()

    # Try to create duplicate
    credits2 = UserRepositoryCredits(
      user_repository_id=self.repo_access.id, current_balance=Decimal("2000")
    )
    self.session.add(credits2)

    with pytest.raises(Exception):  # Should raise integrity error
      self.session.commit()


class TestUserRepositoryCreditTransaction:
  """Test cases for UserRepositoryCreditTransaction model."""

  @pytest.fixture(autouse=True)
  def setup(self, db_session):
    """Set up test fixtures."""
    self.session = db_session

    # Create test user with unique email
    import uuid

    unique_id = str(uuid.uuid4())[:8]
    self.user = User(
      email=f"user_repo_credit_trans_{unique_id}@example.com",
      name="Test User",
      password_hash="hashed_password",
    )
    self.session.add(self.user)
    self.session.commit()

    # Create Graph repository (required for foreign key)
    from robosystems.models.core import Graph

    Graph.find_or_create_repository(
      graph_id="sec",
      graph_name="SEC Public Filings",
      repository_type="sec",
      session=self.session,
    )

    # Create test user repository access
    self.repo_access = UserRepository(
      user_id=self.user.id,
      repository_type=RepositoryType.SEC,
      repository_name="sec",
      access_level=RepositoryAccessLevel.READ,
      repository_plan="starter",
      is_active=True,
    )
    self.session.add(self.repo_access)
    self.session.commit()

    # Create test credits
    self.credits = UserRepositoryCredits(
      user_repository_id=self.repo_access.id,
      current_balance=Decimal("1000"),
      monthly_allocation=Decimal("1000"),
    )
    self.session.add(self.credits)
    self.session.commit()

  def test_create_transaction(self):
    """Test creating a transaction."""
    transaction = UserRepositoryCreditTransaction(
      credit_pool_id=self.credits.id,
      transaction_type=UserRepositoryCreditTransactionType.ALLOCATION.value,
      amount=Decimal("1000"),
      description="Test allocation",
    )

    assert transaction.credit_pool_id == self.credits.id
    assert (
      transaction.transaction_type
      == UserRepositoryCreditTransactionType.ALLOCATION.value
    )
    assert transaction.amount == Decimal("1000")

    self.session.add(transaction)
    self.session.commit()

    assert transaction.id is not None
    assert transaction.created_at is not None

  def test_transaction_with_metadata(self):
    """Test transaction with metadata."""
    metadata = {"test": "data", "operation": "query"}

    transaction = UserRepositoryCreditTransaction(
      credit_pool_id=self.credits.id,
      transaction_type=UserRepositoryCreditTransactionType.CONSUMPTION.value,
      amount=Decimal("-50"),
      description="Test consumption",
      metadata=json.dumps(metadata),
    )

    self.session.add(transaction)
    self.session.commit()

    # Verify metadata was stored
    assert transaction.metadata is not None
    stored_metadata = json.loads(transaction.metadata)
    assert stored_metadata == metadata

  def test_transaction_optional_fields(self):
    """Test optional fields can be None."""
    transaction = UserRepositoryCreditTransaction(
      credit_pool_id=self.credits.id,
      transaction_type=UserRepositoryCreditTransactionType.BONUS.value,
      amount=Decimal("100"),
      description="Bonus credits",
      transaction_metadata=None,
    )

    self.session.add(transaction)
    self.session.commit()

    assert transaction.transaction_metadata is None

  def test_transaction_relationships(self):
    """Test transaction relationships."""
    transaction = UserRepositoryCreditTransaction(
      credit_pool_id=self.credits.id,
      transaction_type=UserRepositoryCreditTransactionType.REFUND.value,
      amount=Decimal("200"),
      description="Test refund",
    )
    self.session.add(transaction)
    self.session.commit()

    # Set up relationship
    self.credits.transactions.append(transaction)
    transaction.credit_pool = self.credits
    self.session.commit()

    # Test relationship
    assert transaction.credit_pool == self.credits
    assert transaction in self.credits.transactions
