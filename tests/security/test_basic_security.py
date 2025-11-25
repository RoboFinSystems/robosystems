"""
Basic security tests to validate security improvements.
"""

import pytest
from robosystems.middleware.graph.utils import MultiTenantUtils


class TestPathTraversal:
  """Test path traversal protection."""

  @pytest.mark.parametrize(
    "malicious_graph_id",
    [
      "../../../etc/passwd",
      "../../malicious",
      "valid/../invalid",
      "path\\traversal",
      "path/traversal",
    ],
  )
  def test_path_traversal_blocked(self, malicious_graph_id):
    """Test that path traversal attempts are blocked."""
    with pytest.raises(ValueError) as exc_info:
      MultiTenantUtils.validate_graph_id(malicious_graph_id)

    assert "invalid" in str(exc_info.value).lower()

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
