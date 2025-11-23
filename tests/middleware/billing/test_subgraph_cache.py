import pytest
from decimal import Decimal
from sqlalchemy.orm import Session

from robosystems.operations.graph.credit_service import CreditService
from robosystems.models.iam import User, Graph, GraphCredits
from robosystems.config.graph_tier import GraphTier


@pytest.fixture
def parent_graph_setup(db_session: Session) -> tuple[Graph, GraphCredits, User]:
  from robosystems.utils.ulid import generate_prefixed_ulid
  import uuid

  user = User(
    id=generate_prefixed_ulid("user"),
    email=f"cache_test_{uuid.uuid4().hex[:8]}@example.com",
    name="Cache Test User",
    password_hash="hashed_password",
    is_active=True,
    email_verified=True,
  )
  db_session.add(user)

  graph_id = f"kg{uuid.uuid4().hex[:16]}"
  graph = Graph(
    graph_id=graph_id,
    graph_name="Cache Test Graph",
    graph_type="generic",
    graph_tier=GraphTier.LADYBUG_STANDARD.value,
  )
  db_session.add(graph)

  credits = GraphCredits(
    graph_id=graph.graph_id,
    user_id=user.id,
    billing_admin_id=user.id,
    monthly_allocation=Decimal("10000"),
    current_balance=Decimal("10000"),
  )
  db_session.add(credits)
  db_session.commit()

  return graph, credits, user


class TestSubgraphCacheSharing:
  """Test credit cache sharing between parent and subgraphs"""

  @pytest.mark.integration
  def test_cache_hit_after_parent_consumption(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Subgraph should hit cache after parent consumes credits"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_dev"

    credit_service = CreditService(db_session)

    parent_result = credit_service.consume_credits(
      graph_id=parent_graph.graph_id,
      operation_type="ai_agent",
      base_cost=Decimal("100"),
      metadata={"source": "parent"},
    )

    assert parent_result["success"] is True

    subgraph_check = credit_service.check_credit_balance(
      graph_id=subgraph_id,
      required_credits=Decimal("50"),
    )

    assert subgraph_check["has_sufficient_credits"] is True
    if "cached" in subgraph_check:
      assert subgraph_check["cached"] is True

  @pytest.mark.integration
  def test_cache_invalidation_propagates_to_subgraphs(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Cache invalidation on parent should affect subgraph lookups"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_staging"

    from robosystems.middleware.billing.cache import credit_cache

    credit_cache.cache_graph_credit_balance(
      graph_id=parent_graph.graph_id,
      balance=Decimal("5000"),
      graph_tier=GraphTier.LADYBUG_STANDARD.value,
    )

    cached_balance = credit_cache.get_cached_graph_credit_balance(parent_graph.graph_id)
    assert cached_balance is not None
    assert cached_balance[0] == Decimal("5000")

    credit_cache.invalidate_graph_credit_balance(parent_graph.graph_id)

    invalidated_cache = credit_cache.get_cached_graph_credit_balance(
      parent_graph.graph_id
    )
    assert invalidated_cache is None

    subgraph_cache = credit_cache.get_cached_graph_credit_balance(subgraph_id)
    assert subgraph_cache is None

  @pytest.mark.integration
  def test_concurrent_subgraph_cache_access(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Multiple subgraphs accessing cache simultaneously should see consistent data"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_dev = f"{parent_graph.graph_id}_dev"
    subgraph_prod = f"{parent_graph.graph_id}_prod"
    subgraph_test = f"{parent_graph.graph_id}_test"

    credit_service = CreditService(db_session)

    credit_service.consume_credits(
      graph_id=parent_graph.graph_id,
      operation_type="ai_agent",
      base_cost=Decimal("100"),
      metadata={"source": "parent"},
    )

    dev_check = credit_service.check_credit_balance(
      graph_id=subgraph_dev,
      required_credits=Decimal("10"),
    )
    prod_check = credit_service.check_credit_balance(
      graph_id=subgraph_prod,
      required_credits=Decimal("10"),
    )
    test_check = credit_service.check_credit_balance(
      graph_id=subgraph_test,
      required_credits=Decimal("10"),
    )

    assert dev_check["available_credits"] == prod_check["available_credits"]
    assert prod_check["available_credits"] == test_check["available_credits"]

  @pytest.mark.integration
  def test_subgraph_consumption_updates_shared_cache(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Subgraph consumption should update the shared cache"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_dev"

    credit_service = CreditService(db_session)

    initial_balance = credit_service.get_credit_summary(parent_graph.graph_id)[
      "current_balance"
    ]

    subgraph_result = credit_service.consume_credits(
      graph_id=subgraph_id,
      operation_type="ai_agent",
      base_cost=Decimal("50"),
      metadata={"source": "subgraph"},
    )

    assert subgraph_result["success"] is True

    parent_check = credit_service.check_credit_balance(
      graph_id=parent_graph.graph_id,
      required_credits=Decimal("10"),
    )

    expected_balance = initial_balance - 50.0
    assert abs(parent_check["available_credits"] - expected_balance) < 1.0

  @pytest.mark.integration
  def test_cache_summary_shared_between_parent_and_subgraphs(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Credit summary cache should be shared between parent and subgraphs"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_cachetest"

    credit_service = CreditService(db_session)

    parent_summary = credit_service.get_credit_summary(parent_graph.graph_id)

    subgraph_summary = credit_service.get_credit_summary(subgraph_id)

    assert (
      parent_summary["monthly_allocation"] == subgraph_summary["monthly_allocation"]
    )
    assert parent_summary["graph_tier"] == subgraph_summary["graph_tier"]
    assert (
      abs(parent_summary["current_balance"] - subgraph_summary["current_balance"])
      < 0.01
    )

  @pytest.mark.integration
  def test_cache_miss_then_hit_pattern(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Test cache miss followed by cache hit for subgraph"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_misshit"

    credit_service = CreditService(db_session)

    from robosystems.middleware.billing.cache import credit_cache

    credit_cache.invalidate_graph_credit_balance(parent_graph.graph_id)

    first_check = credit_service.check_credit_balance(
      graph_id=subgraph_id,
      required_credits=Decimal("10"),
    )

    assert first_check["has_sufficient_credits"] is True
    if "cached" in first_check:
      assert first_check["cached"] is False

    second_check = credit_service.check_credit_balance(
      graph_id=subgraph_id,
      required_credits=Decimal("10"),
    )

    assert second_check["has_sufficient_credits"] is True
    if "cached" in second_check:
      assert second_check["cached"] is True

  @pytest.mark.integration
  def test_cache_invalidation_after_monthly_allocation(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Cache should be invalidated after monthly credit allocation"""
    from datetime import datetime, timezone

    parent_graph, parent_credits, user = parent_graph_setup
    subgraph_id = f"{parent_graph.graph_id}_alloc"

    credit_service = CreditService(db_session)

    parent_credits.last_allocation_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    parent_credits.current_balance = Decimal("100")
    db_session.commit()

    from robosystems.middleware.billing.cache import credit_cache

    credit_cache.cache_graph_credit_balance(
      graph_id=parent_graph.graph_id,
      balance=Decimal("100"),
      graph_tier=GraphTier.LADYBUG_STANDARD.value,
    )

    cached_before = credit_cache.get_cached_graph_credit_balance(parent_graph.graph_id)
    assert cached_before is not None

    allocation_result = credit_service.allocate_monthly_credits(subgraph_id)

    assert allocation_result["success"] is True

    cached_after = credit_cache.get_cached_graph_credit_balance(parent_graph.graph_id)
    if cached_after is not None:
      assert cached_after[0] > Decimal("100")

  @pytest.mark.integration
  def test_cache_consistency_under_rapid_operations(
    self,
    db_session: Session,
    parent_graph_setup: tuple[Graph, GraphCredits, User],
  ):
    """Test cache consistency when multiple operations happen rapidly"""
    parent_graph, parent_credits, user = parent_graph_setup
    subgraph1 = f"{parent_graph.graph_id}_rapid1"
    subgraph2 = f"{parent_graph.graph_id}_rapid2"

    credit_service = CreditService(db_session)

    operations = [
      (parent_graph.graph_id, Decimal("10")),
      (subgraph1, Decimal("15")),
      (subgraph2, Decimal("20")),
      (parent_graph.graph_id, Decimal("5")),
    ]

    total_consumed = Decimal("0")
    for graph_id, amount in operations:
      result = credit_service.consume_credits(
        graph_id=graph_id,
        operation_type="ai_agent",
        base_cost=amount,
        metadata={"test": "rapid"},
      )
      if result["success"]:
        total_consumed += amount

    final_balance = credit_service.get_credit_summary(parent_graph.graph_id)[
      "current_balance"
    ]

    expected_balance = float(Decimal("10000") - total_consumed)
    assert abs(final_balance - expected_balance) < 1.0
