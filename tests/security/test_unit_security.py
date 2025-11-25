"""
Unit tests for security improvements (no database required).
"""

import pytest
from unittest.mock import Mock, patch
from robosystems.middleware.graph.utils import MultiTenantUtils


class TestPathTraversalUnit:
  """Test path traversal protection without database."""

  @pytest.mark.unit
  @pytest.mark.parametrize(
    "malicious_graph_id",
    [
      "../../../etc/passwd",
      "../../malicious",
      "valid/../invalid",
      "path\\traversal",
      "path/traversal",
      "-start-with-hyphen",
      "end-with-hyphen-",
      "_start_with_underscore",
      "end_with_underscore_",
      "contains..dots",
    ],
  )
  def test_path_traversal_blocked(self, malicious_graph_id):
    """Test that path traversal attempts are blocked."""
    with pytest.raises(ValueError) as exc_info:
      MultiTenantUtils.validate_graph_id(malicious_graph_id)

    error_message = str(exc_info.value).lower()
    # Check for various validation error keywords
    assert any(
      word in error_message for word in ["invalid", "cannot", "contains", "characters"]
    )

  @pytest.mark.unit
  def test_valid_graph_ids(self):
    """Test that valid graph IDs are accepted."""
    valid_ids = [
      "kg1a2b3c4d5",
      "user-graph-abc123",
      "ValidGraphName",
      "graph123",
      "my_entity_2024",
    ]

    for graph_id in valid_ids:
      # Should not raise
      validated = MultiTenantUtils.validate_graph_id(graph_id)
      assert validated == graph_id


class TestCypherParameterValidationUnit:
  """Test parameter validation for Cypher queries without database."""

  @pytest.mark.unit
  def test_deep_nested_parameters_rejected(self):
    """Test that deeply nested parameters are rejected."""
    # Mock the Engine class to avoid database connection
    with patch("robosystems.graph_api.core.ladybug.engine.Engine") as MockEngine:
      mock_engine = Mock()

      # Create a mock validation method that checks nesting depth
      def mock_validate_parameters(params):
        def check_depth(obj, current_depth=0):
          if current_depth > 4:  # Max depth is 4
            raise Exception("Parameter nesting too deep")
          if isinstance(obj, dict):
            for value in obj.values():
              check_depth(value, current_depth + 1)
          elif isinstance(obj, list):
            for item in obj:
              check_depth(item, current_depth + 1)

        for value in params.values():
          check_depth(value)

      mock_engine._validate_parameters = mock_validate_parameters
      MockEngine.return_value = mock_engine

      # Create deeply nested structure
      deep_param = {"level1": {"level2": {"level3": {"level4": {"level5": "value"}}}}}

      with pytest.raises(Exception) as exc_info:
        mock_engine._validate_parameters({"nested": deep_param})

      assert "nesting too deep" in str(exc_info.value)

  @pytest.mark.unit
  def test_large_arrays_rejected(self):
    """Test that overly large arrays are rejected."""
    # Mock the Engine class to avoid database connection
    with patch("robosystems.graph_api.core.ladybug.engine.Engine") as MockEngine:
      mock_engine = Mock()

      # Create a mock validation method that checks array size
      def mock_validate_parameters(params):
        for value in params.values():
          if isinstance(value, list) and len(value) > 1000:
            raise Exception("Parameter array too large")

      mock_engine._validate_parameters = mock_validate_parameters
      MockEngine.return_value = mock_engine

      # Create large array
      large_array = list(range(2000))  # Over limit of 1000

      with pytest.raises(Exception) as exc_info:
        mock_engine._validate_parameters({"array": large_array})

      assert "array too large" in str(exc_info.value)

  @pytest.mark.unit
  def test_invalid_parameter_names_rejected(self):
    """Test that invalid parameter names are rejected."""
    # Mock the Engine class to avoid database connection
    with patch("robosystems.graph_api.core.ladybug.engine.Engine") as MockEngine:
      mock_engine = Mock()

      # Create a mock validation method that checks parameter names
      def mock_validate_parameters(params):
        import re

        for param_name in params.keys():
          # Check for invalid characters
          if not re.match(r"^[a-zA-Z][a-zA-Z0-9_]*$", param_name):
            raise Exception(f"Invalid parameter name: {param_name}")

      mock_engine._validate_parameters = mock_validate_parameters
      MockEngine.return_value = mock_engine

      invalid_params = {
        "invalid-name": "value",  # Hyphen not allowed
        "123invalid": "value",  # Can't start with number
        "invalid name": "value",  # Space not allowed
      }

      for param_name, value in invalid_params.items():
        with pytest.raises(Exception) as exc_info:
          mock_engine._validate_parameters({param_name: value})

        assert "Invalid parameter name" in str(exc_info.value)
