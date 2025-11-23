import pytest
from decimal import Decimal
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from robosystems.operations.graph.credit_service import CreditService
from robosystems.models.iam import User, Graph, GraphCredits
from robosystems.config.graph_tier import GraphTier
from robosystems.middleware.graph.types import parse_graph_id, is_subgraph_id


@pytest.fixture
def test_user(db_session: Session) -> User:
  from robosystems.utils.ulid import generate_prefixed_ulid

  user = User(
    id=generate_prefixed_ulid("user"),
    email=f"test_{generate_prefixed_ulid('em')}@example.com",
    name="Test User",
    password_hash="hashed_password",
    is_active=True,
    email_verified=True,
  )
  db_session.add(user)
  db_session.commit()
  return user


@pytest.fixture
def parent_graph(db_session: Session, test_user: User) -> Graph:
  import uuid

  graph_id = f"kg{uuid.uuid4().hex[:16]}"
  graph = Graph(
    graph_id=graph_id,
    graph_name="Test Parent Graph",
    graph_type="generic",
    graph_tier=GraphTier.LADYBUG_STANDARD.value,
  )
  db_session.add(graph)
  db_session.commit()
  return graph


@pytest.fixture
def parent_credits(
  db_session: Session, parent_graph: Graph, test_user: User
) -> GraphCredits:
  credits = GraphCredits(
    graph_id=parent_graph.graph_id,
    user_id=test_user.id,
    billing_admin_id=test_user.id,
    monthly_allocation=Decimal("10000"),
    current_balance=Decimal("10000"),
  )
  db_session.add(credits)
  db_session.commit()
  return credits


@pytest.fixture
def credit_service(db_session: Session) -> CreditService:
  return CreditService(db_session)


class TestSubgraphCreditSharing:
  def test_is_subgraph_id_detection(self):
    assert is_subgraph_id("kg1234567890abcdef_dev") is True
    assert is_subgraph_id("kg1234567890abcdef") is False
    assert is_subgraph_id("sec") is False

  def test_parse_graph_id_for_subgraph(self):
    parent_id, subgraph_name = parse_graph_id("kg1234567890abcdef_dev")
    assert parent_id == "kg1234567890abcdef"
    assert subgraph_name == "dev"

  def test_parse_graph_id_for_parent(self):
    parent_id, subgraph_name = parse_graph_id("kg1234567890abcdef")
    assert parent_id == "kg1234567890abcdef"
    assert subgraph_name is None

  def test_consume_credits_with_subgraph_id(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
    db_session: Session,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev"

    result = credit_service.consume_credits(
      graph_id=subgraph_id,
      operation_type="ai_agent",
      base_cost=Decimal("10"),
      metadata={"description": "Test AI operation from subgraph"},
    )

    assert result["success"] is True
    assert result["credits_consumed"] == 10.0

    db_session.refresh(parent_credits)
    assert parent_credits.current_balance < Decimal("10000")

  def test_check_credit_balance_with_subgraph_id(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev"

    result = credit_service.check_credit_balance(
      graph_id=subgraph_id,
      required_credits=Decimal("100"),
    )

    assert result["has_sufficient_credits"] is True
    assert result["available_credits"] > 0

  def test_get_credit_summary_with_subgraph_id(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev"

    summary = credit_service.get_credit_summary(graph_id=subgraph_id)

    assert summary["monthly_allocation"] == 10000.0
    assert summary["current_balance"] > 0
    assert summary["graph_tier"] == GraphTier.LADYBUG_STANDARD.value

  def test_multiple_subgraphs_share_same_pool(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
    db_session: Session,
  ):
    subgraph_dev = f"{parent_graph.graph_id}_dev2"
    subgraph_staging = f"{parent_graph.graph_id}_staging"

    initial_balance = parent_credits.current_balance

    credit_service.consume_credits(
      graph_id=subgraph_dev,
      operation_type="ai_agent",
      base_cost=Decimal("100"),
      metadata={"description": "Dev subgraph operation"},
    )

    credit_service.consume_credits(
      graph_id=subgraph_staging,
      operation_type="ai_agent",
      base_cost=Decimal("200"),
      metadata={"description": "Staging subgraph operation"},
    )

    db_session.refresh(parent_credits)
    assert parent_credits.current_balance == initial_balance - Decimal("300")

    dev_check = credit_service.check_credit_balance(
      graph_id=subgraph_dev,
      required_credits=Decimal("50"),
    )
    staging_check = credit_service.check_credit_balance(
      graph_id=subgraph_staging,
      required_credits=Decimal("50"),
    )

    assert dev_check["has_sufficient_credits"] is True
    assert staging_check["has_sufficient_credits"] is True
    assert dev_check["available_credits"] == staging_check["available_credits"]

  def test_allocate_monthly_credits_with_subgraph_id(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
    db_session: Session,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev3"

    parent_credits.last_allocation_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    initial_balance = parent_credits.current_balance
    db_session.commit()

    result = credit_service.allocate_monthly_credits(graph_id=subgraph_id)

    assert result["success"] is True
    assert result["allocated_credits"] == 10000.0
    assert result["new_balance"] == float(initial_balance + Decimal("10000"))

  def test_get_credit_transactions_with_subgraph_id(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev"

    credit_service.consume_credits(
      graph_id=subgraph_id,
      operation_type="ai_agent",
      base_cost=Decimal("50"),
      metadata={"description": "Subgraph transaction"},
    )

    transactions = credit_service.get_credit_transactions(
      graph_id=subgraph_id,
      limit=10,
    )

    assert len(transactions) > 0

  def test_insufficient_credits_error_with_subgraph(
    self,
    credit_service: CreditService,
    parent_credits: GraphCredits,
    parent_graph: Graph,
    db_session: Session,
  ):
    subgraph_id = f"{parent_graph.graph_id}_dev"

    parent_credits.current_balance = Decimal("10")
    db_session.commit()

    result = credit_service.consume_credits(
      graph_id=subgraph_id,
      operation_type="ai_agent",
      base_cost=Decimal("100"),
      metadata={"description": "Should fail"},
    )

    assert result["success"] is False
    assert "insufficient" in result["error"].lower()
