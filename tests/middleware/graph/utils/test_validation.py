"""Unit tests for graph ID validation utilities."""

import pytest


@pytest.mark.unit
class TestIsSharedRepository:
  """Tests for is_shared_repository."""

  def test_returns_true_for_registered_repository(self):
    """Test that known shared repositories return True."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    assert is_shared_repository("sec") is True

  def test_returns_false_for_unknown_repository(self):
    """Test that unknown identifiers return False."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    assert is_shared_repository("unknown_repo") is False

  def test_returns_false_for_none(self):
    """Test that None returns False."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    assert is_shared_repository(None) is False

  def test_returns_false_for_empty_string(self):
    """Test that empty string returns False."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    assert is_shared_repository("") is False

  def test_returns_false_for_user_graph_id(self):
    """Test that user graph IDs return False."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    assert is_shared_repository("kg01234567890abcdef") is False

  def test_returns_false_for_subgraph_of_shared_repo(self):
    """Test that subgraph IDs of shared repos return False (exact match only)."""
    from robosystems.middleware.graph.utils.validation import is_shared_repository

    # is_shared_repository checks exact parent IDs only, not subgraphs
    assert is_shared_repository("sec_historical") is False


@pytest.mark.unit
class TestValidateGraphId:
  """Tests for validate_graph_id."""

  def test_valid_alphanumeric(self):
    """Test validation passes for simple alphanumeric IDs."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    assert validate_graph_id("kg1a2b3c") == "kg1a2b3c"

  def test_valid_with_underscore(self):
    """Test validation passes for IDs with underscores."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    assert validate_graph_id("test_entity") == "test_entity"

  def test_valid_with_hyphen(self):
    """Test validation passes for IDs with hyphens in the middle."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    assert validate_graph_id("test-entity") == "test-entity"

  def test_valid_mixed_case(self):
    """Test validation passes for mixed case IDs."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    assert validate_graph_id("TestEntity123") == "TestEntity123"

  def test_valid_single_character(self):
    """Test validation passes for single character ID."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    assert validate_graph_id("a") == "a"

  def test_valid_max_length(self):
    """Test validation passes for exactly 64 character ID."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    graph_id = "a" * 64
    assert validate_graph_id(graph_id) == graph_id

  def test_empty_raises_value_error(self):
    """Test empty string raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="cannot be empty"):
      validate_graph_id("")

  def test_too_long_raises_value_error(self):
    """Test that ID exceeding 64 characters raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="too long"):
      validate_graph_id("a" * 65)

  def test_path_traversal_double_dot_raises_value_error(self):
    """Test that path traversal with double dot raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="invalid path characters"):
      validate_graph_id("../etc/passwd")

  def test_forward_slash_raises_value_error(self):
    """Test that forward slash raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="invalid path characters"):
      validate_graph_id("entity/test")

  def test_backslash_raises_value_error(self):
    """Test that backslash raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="invalid path characters"):
      validate_graph_id("entity\\test")

  def test_dot_raises_value_error(self):
    """Test that dot raises ValueError for invalid characters."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="contains invalid characters"):
      validate_graph_id("entity.test")

  def test_space_raises_value_error(self):
    """Test that space raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="contains invalid characters"):
      validate_graph_id("entity test")

  def test_at_sign_raises_value_error(self):
    """Test that at sign raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="contains invalid characters"):
      validate_graph_id("entity@test")

  def test_hash_raises_value_error(self):
    """Test that hash raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="contains invalid characters"):
      validate_graph_id("entity#test")

  def test_starts_with_hyphen_raises_value_error(self):
    """Test that starting with hyphen raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="cannot start or end with hyphen"):
      validate_graph_id("-entity")

  def test_ends_with_hyphen_raises_value_error(self):
    """Test that ending with hyphen raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="cannot start or end with hyphen"):
      validate_graph_id("entity-")

  def test_starts_with_underscore_raises_value_error(self):
    """Test that starting with underscore raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="cannot start or end with underscore"):
      validate_graph_id("_entity")

  def test_ends_with_underscore_raises_value_error(self):
    """Test that ending with underscore raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="cannot start or end with underscore"):
      validate_graph_id("entity_")

  def test_reserved_name_system_raises_value_error(self):
    """Test that reserved name 'system' raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("system")

  def test_reserved_name_ladybug_raises_value_error(self):
    """Test that reserved name 'ladybug' raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("ladybug")

  def test_reserved_name_default_raises_value_error(self):
    """Test that reserved name 'default' raises ValueError."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("default")

  def test_reserved_name_case_insensitive(self):
    """Test that reserved name check is case insensitive."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("SYSTEM")

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("LADYBUG")

    with pytest.raises(ValueError, match="reserved name"):
      validate_graph_id("DEFAULT")

  def test_allows_shared_repository_names(self):
    """Test that shared repository names pass validation."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    # "sec" is a registered shared repository and should pass validation
    assert validate_graph_id("sec") == "sec"

  def test_path_traversal_with_dots_and_slash(self):
    """Test combined path traversal patterns are rejected."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="invalid path characters"):
      validate_graph_id("../../secret")

  def test_embedded_double_dot_rejected(self):
    """Test that embedded double dots are rejected even without slash."""
    from robosystems.middleware.graph.utils.validation import validate_graph_id

    with pytest.raises(ValueError, match="invalid path characters"):
      validate_graph_id("foo..bar")


@pytest.mark.unit
class TestValidateDatabaseCreation:
  """Tests for validate_database_creation."""

  def test_valid_graph_id_passes(self):
    """Test that a valid graph ID passes database creation validation."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    result = validate_database_creation("kg1a2b3c")
    assert result == "kg1a2b3c"

  def test_invalid_graph_id_raises_value_error(self):
    """Test that an invalid graph ID raises ValueError."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    with pytest.raises(ValueError, match="contains invalid characters"):
      validate_database_creation("invalid.graph@id")

  def test_empty_graph_id_raises_value_error(self):
    """Test that empty graph ID raises ValueError."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    with pytest.raises(ValueError, match="cannot be empty"):
      validate_database_creation("")

  def test_reserved_name_raises_value_error(self):
    """Test that reserved names raise ValueError."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    with pytest.raises(ValueError, match="reserved name"):
      validate_database_creation("system")

  def test_returns_validated_graph_id(self):
    """Test that the validated graph ID is returned."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    result = validate_database_creation("my_graph_123")
    assert result == "my_graph_123"

  def test_shared_repository_passes(self):
    """Test that shared repository names pass creation validation."""
    from robosystems.middleware.graph.utils.validation import (
      validate_database_creation,
    )

    result = validate_database_creation("sec")
    assert result == "sec"
