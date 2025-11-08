"""
Tests for storage limit functionality.

Tests the storage limit system including:
- Storage limit checking and validation
- Admin override mechanisms
- Storage violation detection
- Credit service integration
"""

import pytest
from decimal import Decimal
from datetime import datetime, timezone

from robosystems.config.graph_tier import GraphTier
from robosystems.models.iam.graph_credits import GraphCredits
from robosystems.models.iam.graph_usage import (
  GraphUsage,
  UsageEventType,
)
from robosystems.operations.graph.credit_service import CreditService


class TestStorageLimitChecking:
  """Test storage limit checking functionality."""

  def test_check_storage_limit_within_limit(self, db_session, sample_graph_credits):
    """Test storage check when within limits."""
    credit_service = CreditService(db_session)

    # Test with 30GB usage against 100GB limit (30%, below 80% threshold)
    result = credit_service.check_storage_limit(
      sample_graph_credits.graph_id, current_storage_gb=Decimal("30")
    )

    assert result["within_limit"] is True
    assert result["approaching_limit"] is False
    assert result["current_storage_gb"] == 30.0
    assert result["effective_limit_gb"] == 100.0
    assert result["usage_percentage"] == 30.0
    assert result["has_override"] is False

  def test_check_storage_limit_approaching_threshold(
    self, db_session, sample_graph_credits
  ):
    """Test storage check when approaching the warning threshold."""
    credit_service = CreditService(db_session)

    # Test with 90GB usage (90% of 100GB limit, above 80% threshold)
    result = credit_service.check_storage_limit(
      sample_graph_credits.graph_id, current_storage_gb=Decimal("90")
    )

    assert result["within_limit"] is True
    assert result["approaching_limit"] is True
    assert result["usage_percentage"] == 90.0
    assert result["needs_warning"] is True

  def test_check_storage_limit_exceeds_limit(self, db_session, sample_graph_credits):
    """Test storage check when exceeding limits."""
    credit_service = CreditService(db_session)

    # Test with 120GB usage against 100GB limit
    result = credit_service.check_storage_limit(
      sample_graph_credits.graph_id, current_storage_gb=Decimal("120")
    )

    assert result["within_limit"] is False
    assert result["approaching_limit"] is True  # Still approaching since we're over
    assert result["usage_percentage"] == 120.0
    assert "recommendations" in result
    assert len(result["recommendations"]) > 0

  def test_check_storage_limit_with_override(self, db_session, sample_graph_credits):
    """Test storage check with admin override applied."""
    credit_service = CreditService(db_session)

    # Set an override to 200GB
    sample_graph_credits.storage_override_gb = Decimal("200")
    db_session.commit()

    # Test with 120GB usage against 200GB override limit
    result = credit_service.check_storage_limit(
      sample_graph_credits.graph_id, current_storage_gb=Decimal("120")
    )

    assert result["within_limit"] is True
    assert result["effective_limit_gb"] == 200.0
    assert result["usage_percentage"] == 60.0
    assert result["has_override"] is True

  def test_check_storage_limit_fetches_usage_data(
    self, db_session, sample_graph_credits
  ):
    """Test storage check fetches latest usage data when not provided."""
    # Create usage tracking record
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="usage_test_1",
      graph_id=sample_graph_credits.graph_id,
      user_id=sample_graph_credits.user_id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=sample_graph_credits.graph_tier.value
      if hasattr(sample_graph_credits.graph_tier, "value")
      else sample_graph_credits.graph_tier,
      storage_gb=Decimal("70"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    credit_service = CreditService(db_session)

    # Call without providing current_storage_gb
    result = credit_service.check_storage_limit(sample_graph_credits.graph_id)

    assert result["current_storage_gb"] == 70.0
    assert result["within_limit"] is True

  def test_check_storage_limit_no_credit_pool(self, db_session):
    """Test storage check with non-existent graph."""
    credit_service = CreditService(db_session)

    result = credit_service.check_storage_limit("nonexistent_graph")

    assert "error" in result
    assert result["error"] == "No credit pool found for graph"


class TestStorageOverride:
  """Test storage override functionality."""

  def test_set_storage_override_success(self, db_session, sample_graph_credits):
    """Test successful storage override."""
    credit_service = CreditService(db_session)

    result = credit_service.set_storage_override(
      graph_id=sample_graph_credits.graph_id,
      new_limit_gb=Decimal("200"),
      admin_user_id="admin_123",
      reason="Emergency capacity increase",
    )

    assert result["success"] is True
    assert result["old_limit_gb"] == 100.0
    assert result["new_limit_gb"] == 200.0
    assert result["admin_user_id"] == "admin_123"
    assert result["reason"] == "Emergency capacity increase"

    # Verify database was updated
    db_session.refresh(sample_graph_credits)
    assert sample_graph_credits.storage_override_gb == Decimal("200")

  def test_set_storage_override_creates_audit_transaction(
    self, db_session, sample_graph_credits
  ):
    """Test that storage override creates audit transaction."""
    credit_service = CreditService(db_session)

    credit_service.set_storage_override(
      graph_id=sample_graph_credits.graph_id,
      new_limit_gb=Decimal("300"),
      admin_user_id="admin_456",
      reason="Special project requirements",
    )

    # Check that a transaction was created
    from robosystems.models.iam.graph_credits import GraphCreditTransaction

    transaction = (
      db_session.query(GraphCreditTransaction)
      .filter(GraphCreditTransaction.graph_credits_id == sample_graph_credits.id)
      .filter(GraphCreditTransaction.description.contains("Storage limit override"))
      .first()
    )

    assert transaction is not None
    assert transaction.amount == Decimal("0")  # No credit change
    metadata = transaction.get_metadata()
    assert metadata["admin_user_id"] == "admin_456"
    assert metadata["reason"] == "Special project requirements"
    assert metadata["action_type"] == "storage_override"

  def test_set_storage_override_no_credit_pool(self, db_session):
    """Test storage override with non-existent graph."""
    credit_service = CreditService(db_session)

    result = credit_service.set_storage_override(
      graph_id="nonexistent_graph",
      new_limit_gb=Decimal("1000"),
      admin_user_id="admin_123",
      reason="Test",
    )

    assert "error" in result
    assert result["error"] == "No credit pool found for graph"


class TestStorageViolationDetection:
  """Test storage violation detection."""

  def test_get_storage_limit_violations_empty(self, db_session):
    """Test violation detection with no violations."""
    credit_service = CreditService(db_session)

    violations = credit_service.get_storage_limit_violations()

    # Since other tests may create violations, just check that the method works
    # and returns a list (empty or with items from other test data)
    assert isinstance(violations, list)
    # Each violation should have the expected structure
    for violation in violations:
      assert "graph_id" in violation
      assert "user_id" in violation
      assert "current_storage_gb" in violation
      assert "effective_limit_gb" in violation
      assert "usage_percentage" in violation
      assert "exceeds_limit" in violation
      assert "approaching_limit" in violation

  def test_get_storage_limit_violations_with_violations(self, db_session, sample_user):
    """Test violation detection with actual violations."""
    import uuid

    # First create organization and add user to it
    from robosystems.models.iam import Graph, Org, OrgUser, OrgType

    org = Org.create(
      name="Test Organization",
      org_type=OrgType.PERSONAL,
      session=db_session,
    )

    OrgUser.create(
      org_id=org.id,
      user_id=sample_user.id,
      role="OWNER",
      session=db_session,
    )

    # Then create the graph
    graph_id = f"violation_test_graph_{uuid.uuid4().hex[:8]}"
    Graph.create(
      graph_id=graph_id,
      graph_name="Test Graph",
      graph_type="generic",
      org_id=org.id,
      session=db_session,
      graph_tier=GraphTier.KUZU_STANDARD,
    )

    # Create graph credits with limit
    credits = GraphCredits.create_for_graph(
      graph_id=graph_id,
      user_id=sample_user.id,
      billing_admin_id=sample_user.id,
      monthly_allocation=Decimal("1000"),
      session=db_session,
    )

    # Create usage record that exceeds limit (120GB > 100GB limit)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="violation_usage_1",
      graph_id=credits.graph_id,
      user_id=sample_user.id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=credits.graph_tier.value
      if hasattr(credits.graph_tier, "value")
      else credits.graph_tier,
      storage_gb=Decimal("120"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    credit_service = CreditService(db_session)
    violations = credit_service.get_storage_limit_violations()

    # Filter violations for this specific graph
    graph_violations = [v for v in violations if v["graph_id"] == credits.graph_id]
    assert len(graph_violations) == 1
    violation = graph_violations[0]
    assert violation["graph_id"] == credits.graph_id
    assert violation["user_id"] == sample_user.id
    assert violation["current_storage_gb"] == 120.0
    assert violation["effective_limit_gb"] == 100.0
    assert violation["exceeds_limit"] is True
    assert violation["usage_percentage"] == 120.0

  def test_get_storage_limit_violations_approaching_only(self, db_session, sample_user):
    """Test violation detection for graphs approaching limits."""
    import uuid

    # First create organization and add user to it
    from robosystems.models.iam import Graph, Org, OrgUser, OrgType

    org = Org.create(
      name="Test Organization",
      org_type=OrgType.PERSONAL,
      session=db_session,
    )

    OrgUser.create(
      org_id=org.id,
      user_id=sample_user.id,
      role="OWNER",
      session=db_session,
    )

    # Then create the graph
    graph_id = f"approaching_test_graph_{uuid.uuid4().hex[:8]}"
    Graph.create(
      graph_id=graph_id,
      graph_name="Test Graph",
      graph_type="generic",
      org_id=org.id,
      session=db_session,
      graph_tier=GraphTier.KUZU_STANDARD,
    )

    # Create graph credits
    credits = GraphCredits.create_for_graph(
      graph_id=graph_id,
      user_id=sample_user.id,
      billing_admin_id=sample_user.id,
      monthly_allocation=Decimal("1000"),
      session=db_session,
    )

    # Create usage record that approaches limit (90GB = 90% of 100GB)
    now = datetime.now(timezone.utc)
    usage_record = GraphUsage(
      id="approaching_usage_1",
      graph_id=credits.graph_id,
      user_id=sample_user.id,
      event_type=UsageEventType.STORAGE_SNAPSHOT.value,
      graph_tier=credits.graph_tier.value
      if hasattr(credits.graph_tier, "value")
      else credits.graph_tier,
      storage_gb=Decimal("90"),
      recorded_at=now,
      billing_year=now.year,
      billing_month=now.month,
      billing_day=now.day,
      billing_hour=now.hour,
    )
    db_session.add(usage_record)
    db_session.commit()

    credit_service = CreditService(db_session)
    violations = credit_service.get_storage_limit_violations()

    # Filter violations for this specific graph
    graph_violations = [v for v in violations if v["graph_id"] == credits.graph_id]
    assert len(graph_violations) == 1
    violation = graph_violations[0]
    assert violation["exceeds_limit"] is False
    assert violation["approaching_limit"] is True
    assert violation["usage_percentage"] == 90.0


class TestStorageCreditsConsumption:
  """Test storage credit consumption functionality."""

  def test_consume_storage_credits_success(self, db_session, sample_graph_credits):
    """Test successful storage credit consumption with overage."""
    credit_service = CreditService(db_session)

    # Record initial balance
    initial_balance = sample_graph_credits.current_balance
    # Standard tier has 100 GB included, so 200 GB total = 100 GB overage
    storage_gb = Decimal("200")

    result = credit_service.consume_storage_credits(
      graph_id=sample_graph_credits.graph_id,
      storage_gb=storage_gb,
      metadata={"test": "data"},
    )

    assert result["success"] is True
    assert result["total_storage_gb"] == 200.0
    assert result["included_gb"] == 100.0
    assert result["overage_gb"] == 100.0
    assert result["credits_per_gb_day"] == 10
    assert result["credits_consumed"] == 1000.0  # 100 GB overage * 10 credits/GB/day

    # Check balance was updated
    db_session.refresh(sample_graph_credits)
    expected_balance = initial_balance - Decimal("1000.0")
    assert sample_graph_credits.current_balance == expected_balance

  def test_consume_storage_credits_allows_negative(
    self, db_session, sample_graph_credits
  ):
    """Test that storage credits can result in negative balance."""
    credit_service = CreditService(db_session)

    # Set balance to very low amount
    sample_graph_credits.current_balance = Decimal("1.0")
    db_session.commit()

    # Consume more storage credits than available
    # Standard tier has 100 GB included, so 200 GB = 100 GB overage = 1000 credits
    result = credit_service.consume_storage_credits(
      graph_id=sample_graph_credits.graph_id,
      storage_gb=Decimal("200"),  # 100 GB overage = 1000 credits
    )

    assert result["success"] is True
    assert result["went_negative"] is True
    assert result["old_balance"] == 1.0
    assert result["remaining_balance"] == -999.0  # 1.0 - 1000.0

  def test_consume_storage_credits_creates_transaction(
    self, db_session, sample_graph_credits
  ):
    """Test that storage overage consumption creates transaction record."""
    credit_service = CreditService(db_session)

    # Standard tier has 100 GB included, use 150 GB = 50 GB overage
    credit_service.consume_storage_credits(
      graph_id=sample_graph_credits.graph_id,
      storage_gb=Decimal("150"),
    )

    # Check transaction was created
    from robosystems.models.iam.graph_credits import (
      GraphCreditTransaction,
      CreditTransactionType,
    )

    transaction = (
      db_session.query(GraphCreditTransaction)
      .filter(GraphCreditTransaction.graph_credits_id == sample_graph_credits.id)
      .filter(
        GraphCreditTransaction.transaction_type
        == CreditTransactionType.CONSUMPTION.value
      )
      .filter(GraphCreditTransaction.description.contains("Daily storage overage"))
      .first()
    )

    assert transaction is not None
    assert transaction.amount == Decimal("-500.0")  # -50 GB overage * 10 credits/GB/day
    metadata = transaction.get_metadata()
    assert metadata["total_storage_gb"] == "150"
    assert metadata["included_gb"] == "100"
    assert metadata["overage_gb"] == "50"
    assert metadata["overage_cost"] == "500"
    assert metadata["credits_per_gb_day"] == "10"
    assert metadata["allows_negative"] is True

  def test_consume_storage_credits_no_credit_pool(self, db_session):
    """Test storage consumption with non-existent graph."""
    credit_service = CreditService(db_session)

    result = credit_service.consume_storage_credits(
      graph_id="nonexistent_graph",
      storage_gb=Decimal("100"),
    )

    assert result["success"] is False
    assert result["error"] == "No credit pool found for graph"
    assert result["credits_consumed"] == 0


@pytest.fixture
def sample_graph_credits(db_session, sample_user):
  """Create sample graph credits for testing."""
  import uuid

  # First create organization and add user to it
  from robosystems.models.iam import Graph, Org, OrgUser, OrgType

  org = Org.create(
    name="Test Organization",
    org_type=OrgType.PERSONAL,
    session=db_session,
  )

  OrgUser.create(
    org_id=org.id,
    user_id=sample_user.id,
    role="OWNER",
    session=db_session,
  )

  # Then create the graph
  graph_id = f"test_graph_{uuid.uuid4().hex[:8]}"
  Graph.create(
    graph_id=graph_id,
    graph_name="Test Graph",
    graph_type="generic",
    org_id=org.id,
    session=db_session,
    graph_tier=GraphTier.KUZU_STANDARD,
  )

  credits = GraphCredits.create_for_graph(
    graph_id=graph_id,
    user_id=sample_user.id,
    billing_admin_id=sample_user.id,
    monthly_allocation=Decimal("1000"),
    session=db_session,
  )
  return credits


@pytest.fixture
def sample_user(db_session):
  """Create sample user for testing."""
  import uuid
  from robosystems.models.iam import User

  user = User(
    id=f"test_user_{uuid.uuid4().hex[:8]}",
    email=f"test{uuid.uuid4().hex[:8]}@example.com",
    name="Test User",
    password_hash="hashed_password",
    is_active=True,
  )
  db_session.add(user)
  db_session.commit()
  return user
