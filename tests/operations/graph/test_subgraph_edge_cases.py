from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from robosystems.middleware.graph.types import (
  construct_subgraph_id,
  is_subgraph_id,
  parse_graph_id,
)
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.models.iam import GraphCredits, User
from robosystems.operations.graph.credit_service import CreditService


class TestSubgraphEdgeCases:
  """Test edge cases in subgraph handling"""

  @pytest.mark.unit
  def test_shared_repositories_not_treated_as_subgraphs(self):
    """Shared repositories should never be treated as subgraph IDs"""
    shared_repos = ["sec", "industry", "economic"]

    for repo in shared_repos:
      assert is_subgraph_id(repo) is False
      assert MultiTenantUtils.is_shared_repository(repo) is True

      parent_id, subgraph_name = parse_graph_id(repo)
      assert parent_id == repo
      assert subgraph_name is None

  @pytest.mark.unit
  def test_shared_repo_with_underscore_not_parsed_as_subgraph(self):
    """Shared repositories in GraphTypeRegistry should not be parsed even with underscores"""
    assert is_subgraph_id("sec") is False
    assert is_subgraph_id("industry") is False

    parent_id, subgraph_name = parse_graph_id("sec")
    assert parent_id == "sec"
    assert subgraph_name is None

  @pytest.mark.unit
  def test_construct_subgraph_id_validation(self):
    """Test subgraph ID construction with validation"""
    assert construct_subgraph_id("kg123", "dev") == "kg123_dev"
    assert (
      construct_subgraph_id("kg1234567890abcdef", "staging")
      == "kg1234567890abcdef_staging"
    )

    with pytest.raises(ValueError, match="parent_graph_id cannot be empty"):
      construct_subgraph_id("", "dev")

    with pytest.raises(ValueError, match="subgraph_name cannot be empty"):
      construct_subgraph_id("kg123", "")

    with pytest.raises(ValueError, match="parent_graph_id cannot contain underscore"):
      construct_subgraph_id("kg123_test", "dev")

    with pytest.raises(ValueError, match="subgraph_name must be alphanumeric"):
      construct_subgraph_id("kg123", "dev-test")

    with pytest.raises(ValueError, match="subgraph_name must be alphanumeric"):
      construct_subgraph_id("kg123", "dev.test")

    with pytest.raises(ValueError, match="subgraph_name must be alphanumeric"):
      construct_subgraph_id("kg123", "a" * 21)

  @pytest.mark.unit
  def test_invalid_subgraph_name_formats(self):
    """Test that invalid subgraph names are detected"""
    invalid_subgraph_ids = [
      "kg123_dev-test",
      "kg123_dev.test",
      "kg123_dev test",
      "kg123_dev@prod",
      "kg123_" + ("a" * 21),
    ]

    for invalid_id in invalid_subgraph_ids:
      if "_" in invalid_id:
        parent_id, subgraph_name = parse_graph_id(invalid_id)
        import re

        from robosystems.middleware.graph.types import SUBGRAPH_NAME_PATTERN

        if subgraph_name:
          assert not re.match(SUBGRAPH_NAME_PATTERN, subgraph_name)

  @pytest.mark.unit
  def test_multiple_underscores_in_graph_id(self):
    """Test handling of graph IDs with multiple underscores (invalid)"""
    # Graph IDs with multiple underscores are NOT valid subgraph IDs
    # because subgraph names must be alphanumeric only (no underscores)
    graph_id_with_multiple_underscores = "kg1234567890abcdef_dev_test"

    # This should NOT be treated as a subgraph since "dev_test" is invalid
    assert is_subgraph_id(graph_id_with_multiple_underscores) is False

    # parse_graph_id returns it unchanged
    parent_id, subgraph_name = parse_graph_id(graph_id_with_multiple_underscores)
    assert parent_id == graph_id_with_multiple_underscores
    assert subgraph_name is None

  @pytest.mark.integration
  def test_nonexistent_parent_graph_credit_lookup(
    self,
    db_session: Session,
  ):
    """Subgraph ID with non-existent parent should return no credits"""
    credit_service = CreditService(db_session)

    nonexistent_parent_id = "kg_nonexistent123"
    subgraph_id = f"{nonexistent_parent_id}_dev"

    credits = GraphCredits.get_by_graph_id(nonexistent_parent_id, db_session)
    assert credits is None

    result = credit_service.check_credit_balance(
      graph_id=subgraph_id,
      required_credits=Decimal("10"),
    )

    assert result["has_sufficient_credits"] is False
    assert "error" in result

  @pytest.mark.unit
  def test_valid_graph_id_patterns(self):
    """Test various valid graph ID formats"""
    valid_parent_ids = [
      "kg1234567890abcdef",
      "kg123456",
      "kgabc",
      "sec",
      "industry",
      "economic",
    ]

    for graph_id in valid_parent_ids:
      parent_id, subgraph_name = parse_graph_id(graph_id)
      assert parent_id == graph_id
      assert subgraph_name is None

  @pytest.mark.unit
  def test_valid_subgraph_id_patterns(self):
    """Test various valid subgraph ID formats"""
    # Parent IDs must be kg[a-f0-9]{16,} (16+ hex chars)
    valid_subgraph_ids = [
      ("kg1234567890abcdef_dev", "kg1234567890abcdef", "dev"),
      ("kg1234567890abcdef_prod", "kg1234567890abcdef", "prod"),
      ("kgabcdef1234567890_staging", "kgabcdef1234567890", "staging"),
      ("kg0000000000000001_test", "kg0000000000000001", "test"),
      ("kg1234567890abcdef_a", "kg1234567890abcdef", "a"),
      ("kg1234567890abcdef_12345", "kg1234567890abcdef", "12345"),
      ("kg1234567890abcdef_A1B2C3", "kg1234567890abcdef", "A1B2C3"),
    ]

    for subgraph_id, expected_parent, expected_subgraph in valid_subgraph_ids:
      assert is_subgraph_id(subgraph_id) is True
      parent_id, subgraph_name = parse_graph_id(subgraph_id)
      assert parent_id == expected_parent
      assert subgraph_name == expected_subgraph

  @pytest.mark.integration
  def test_credit_operations_fail_gracefully_for_invalid_parent(
    self,
    db_session: Session,
  ):
    """Credit operations should fail gracefully for invalid parent IDs"""
    credit_service = CreditService(db_session)

    invalid_subgraph_id = "kg_invalid_dev"

    result = credit_service.consume_credits(
      graph_id=invalid_subgraph_id,
      operation_type="ai_agent",
      base_cost=Decimal("10"),
      metadata={"test": "invalid parent"},
    )

    assert result["success"] is False
    assert "error" in result

  @pytest.mark.unit
  def test_empty_string_not_valid_graph_id(self):
    """Empty string should not be treated as valid graph ID"""
    assert is_subgraph_id("") is False

    parent_id, subgraph_name = parse_graph_id("")
    assert parent_id == ""
    assert subgraph_name is None

  @pytest.mark.unit
  def test_underscore_only_not_valid_subgraph(self):
    """Edge cases with underscores"""
    # Both of these are invalid
    assert is_subgraph_id("_") is False
    assert is_subgraph_id("kg_") is False  # Parent too short, empty subgraph name

    # parse_graph_id returns them unchanged
    parent_id, subgraph_name = parse_graph_id("kg_")
    assert parent_id == "kg_"
    assert subgraph_name is None

  @pytest.mark.unit
  def test_case_sensitivity_in_subgraph_names(self):
    """Subgraph names should be case-sensitive"""
    subgraph_dev = "kg1234567890abcdef_dev"
    subgraph_DEV = "kg1234567890abcdef_DEV"

    parent1, name1 = parse_graph_id(subgraph_dev)
    parent2, name2 = parse_graph_id(subgraph_DEV)

    assert parent1 == parent2 == "kg1234567890abcdef"
    assert name1 == "dev"
    assert name2 == "DEV"
    assert name1 != name2

  @pytest.mark.unit
  def test_numeric_only_subgraph_names(self):
    """Numeric-only subgraph names should be valid"""
    subgraph_id = "kg1234567890abcdef_12345"

    assert is_subgraph_id(subgraph_id) is True

    parent_id, subgraph_name = parse_graph_id(subgraph_id)
    assert parent_id == "kg1234567890abcdef"
    assert subgraph_name == "12345"

  @pytest.mark.integration
  def test_shared_repo_credit_operations_use_different_path(
    self,
    db_session: Session,
  ):
    """Shared repository credit operations should use repository credit system"""
    credit_service = CreditService(db_session)

    import uuid

    from robosystems.utils.ulid import generate_prefixed_ulid

    user = User(
      id=generate_prefixed_ulid("user"),
      email=f"test_{uuid.uuid4().hex[:8]}@example.com",
      name="Test User",
      password_hash="hashed",
      is_active=True,
      email_verified=True,
    )
    db_session.add(user)
    db_session.commit()

    result = credit_service.check_credit_balance(
      graph_id="sec",
      required_credits=Decimal("10"),
      user_id=user.id,
    )

    assert "repository_type" in result or "error" in result

  @pytest.mark.unit
  def test_very_long_parent_id_with_subgraph(self):
    """Test handling of very long parent IDs with subgraph names"""
    long_parent_id = "kg" + ("a" * 50)
    subgraph_id = f"{long_parent_id}_dev"

    assert is_subgraph_id(subgraph_id) is True

    parent_id, subgraph_name = parse_graph_id(subgraph_id)
    assert parent_id == long_parent_id
    assert subgraph_name == "dev"

  @pytest.mark.unit
  def test_maximum_length_subgraph_name(self):
    """Test maximum allowed subgraph name length (20 characters)"""
    max_length_name = "a" * 20
    parent_id = "kg1234567890abcdef"
    subgraph_id = f"{parent_id}_{max_length_name}"

    constructed = construct_subgraph_id(parent_id, max_length_name)
    assert constructed == subgraph_id

    parsed_parent, subgraph_name = parse_graph_id(subgraph_id)
    assert parsed_parent == parent_id
    assert subgraph_name == max_length_name
    assert len(subgraph_name) == 20
