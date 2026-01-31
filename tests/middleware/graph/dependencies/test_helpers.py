"""
Tests for helper dependency functions.

These tests cover the common FastAPI dependencies for entity and graph requirements.
"""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.middleware.graph.dependencies.helpers import (
  optional_entity,
  optional_user_graph,
  require_entity,
  require_graph_category,
  require_user_graph,
)


class TestRequireEntity:
  """Test require_entity dependency function."""

  def test_require_entity_with_selected_entity(self):
    """Test require_entity returns entity when selected."""
    mock_user = Mock()
    mock_user.selected_entity = "entity123"

    result = require_entity(current_user=mock_user)

    assert result == "entity123"

  def test_require_entity_without_selected_entity(self):
    """Test require_entity raises when no entity selected."""
    mock_user = Mock()
    mock_user.selected_entity = None

    with pytest.raises(HTTPException) as exc_info:
      require_entity(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "No entity selected" in str(exc_info.value.detail)

  def test_require_entity_missing_attribute(self):
    """Test require_entity handles missing attribute gracefully."""
    mock_user = Mock(spec=[])  # No attributes

    with pytest.raises(HTTPException) as exc_info:
      require_entity(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST


class TestOptionalEntity:
  """Test optional_entity dependency function."""

  def test_optional_entity_with_selected_entity(self):
    """Test optional_entity returns entity when selected."""
    mock_user = Mock()
    mock_user.selected_entity = "entity123"

    result = optional_entity(current_user=mock_user)

    assert result == "entity123"

  def test_optional_entity_without_selected_entity(self):
    """Test optional_entity returns None when no entity selected."""
    mock_user = Mock()
    mock_user.selected_entity = None

    result = optional_entity(current_user=mock_user)

    assert result is None

  def test_optional_entity_missing_attribute(self):
    """Test optional_entity returns None when attribute missing."""
    mock_user = Mock(spec=[])  # No attributes

    result = optional_entity(current_user=mock_user)

    assert result is None


class TestRequireUserGraph:
  """Test require_user_graph dependency function."""

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_user_graph_success(self, mock_utils):
    """Test require_user_graph returns graph_id when valid."""
    mock_user = Mock()
    mock_user.selected_graph = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity

    result = require_user_graph(current_user=mock_user)

    assert result == "kg1234567890abcdef"
    mock_utils.get_graph_identity.assert_called_once_with("kg1234567890abcdef")

  def test_require_user_graph_no_selection(self):
    """Test require_user_graph raises when no graph selected."""
    mock_user = Mock()
    mock_user.selected_graph = None

    with pytest.raises(HTTPException) as exc_info:
      require_user_graph(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "No graph selected" in str(exc_info.value.detail)

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_user_graph_shared_repo_rejected(self, mock_utils):
    """Test require_user_graph rejects shared repositories."""
    mock_user = Mock()
    mock_user.selected_graph = "sec"

    mock_identity = Mock()
    mock_identity.is_user_graph = False

    mock_utils.get_graph_identity.return_value = mock_identity

    with pytest.raises(HTTPException) as exc_info:
      require_user_graph(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "is not a user graph" in str(exc_info.value.detail)

  def test_require_user_graph_missing_attribute(self):
    """Test require_user_graph handles missing attribute."""
    mock_user = Mock(spec=[])  # No attributes

    with pytest.raises(HTTPException) as exc_info:
      require_user_graph(current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST


class TestOptionalUserGraph:
  """Test optional_user_graph dependency function."""

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_optional_user_graph_with_user_graph(self, mock_utils):
    """Test optional_user_graph returns graph_id when valid user graph."""
    mock_user = Mock()
    mock_user.selected_graph = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.is_user_graph = True

    mock_utils.get_graph_identity.return_value = mock_identity

    result = optional_user_graph(current_user=mock_user)

    assert result == "kg1234567890abcdef"

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_optional_user_graph_with_shared_repo(self, mock_utils):
    """Test optional_user_graph returns None for shared repository."""
    mock_user = Mock()
    mock_user.selected_graph = "sec"

    mock_identity = Mock()
    mock_identity.is_user_graph = False

    mock_utils.get_graph_identity.return_value = mock_identity

    result = optional_user_graph(current_user=mock_user)

    assert result is None

  def test_optional_user_graph_no_selection(self):
    """Test optional_user_graph returns None when no graph selected."""
    mock_user = Mock()
    mock_user.selected_graph = None

    result = optional_user_graph(current_user=mock_user)

    assert result is None

  def test_optional_user_graph_missing_attribute(self):
    """Test optional_user_graph returns None when attribute missing."""
    mock_user = Mock(spec=[])  # No attributes

    result = optional_user_graph(current_user=mock_user)

    assert result is None


class TestRequireGraphCategory:
  """Test require_graph_category dependency function."""

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_graph_category_user_success(self, mock_utils):
    """Test require_graph_category succeeds for matching category."""
    mock_user = Mock()
    mock_user.selected_graph = "kg1234567890abcdef"

    mock_identity = Mock()
    mock_identity.category.value = "user"

    mock_utils.get_graph_identity.return_value = mock_identity

    result = require_graph_category("user", current_user=mock_user)

    assert result == "kg1234567890abcdef"

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_graph_category_shared_success(self, mock_utils):
    """Test require_graph_category succeeds for shared category."""
    mock_user = Mock()
    mock_user.selected_graph = "sec"

    mock_identity = Mock()
    mock_identity.category.value = "shared"

    mock_utils.get_graph_identity.return_value = mock_identity

    result = require_graph_category("shared", current_user=mock_user)

    assert result == "sec"

  def test_require_graph_category_no_selection(self):
    """Test require_graph_category raises when no graph selected."""
    mock_user = Mock()
    mock_user.selected_graph = None

    with pytest.raises(HTTPException) as exc_info:
      require_graph_category("user", current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "No graph selected" in str(exc_info.value.detail)

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_graph_category_mismatch(self, mock_utils):
    """Test require_graph_category raises when category doesn't match."""
    mock_user = Mock()
    mock_user.selected_graph = "sec"

    mock_identity = Mock()
    mock_identity.category.value = "shared"

    mock_utils.get_graph_identity.return_value = mock_identity

    with pytest.raises(HTTPException) as exc_info:
      require_graph_category("user", current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "must be of category 'user'" in str(exc_info.value.detail)

  @patch("robosystems.middleware.graph.dependencies.helpers.MultiTenantUtils")
  def test_require_graph_category_system(self, mock_utils):
    """Test require_graph_category works for system category."""
    mock_user = Mock()
    mock_user.selected_graph = "system_graph"

    mock_identity = Mock()
    mock_identity.category.value = "system"

    mock_utils.get_graph_identity.return_value = mock_identity

    result = require_graph_category("system", current_user=mock_user)

    assert result == "system_graph"

  def test_require_graph_category_missing_attribute(self):
    """Test require_graph_category handles missing attribute."""
    mock_user = Mock(spec=[])  # No attributes

    with pytest.raises(HTTPException) as exc_info:
      require_graph_category("user", current_user=mock_user)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
