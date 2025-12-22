"""
Tests for LadybugDB Schema Validator.

Comprehensive test coverage for schema validation functionality.
"""

from unittest.mock import Mock, patch

import pytest

from robosystems.schemas.validator import LadybugSchemaValidator


@pytest.fixture
def mock_schema_loader():
  """Create a mock schema loader for testing."""
  mock_loader = Mock()

  # Mock node types
  mock_loader.list_node_types.return_value = ["Entity", "Element", "Fact"]

  # Mock relationship types
  mock_loader.list_relationship_types.return_value = [
    "HAS_ELEMENT",
    "HAS_FACT",
    "RELATES_TO",
  ]

  # Mock node validation
  mock_loader.validate_node_properties.return_value = True

  # Mock relationship validation
  mock_loader.validate_relationship.return_value = True

  # Mock node schema
  mock_node = Mock()
  mock_node.name = "Entity"
  mock_node.description = "A business entity"
  mock_loader.get_node_schema.return_value = mock_node

  # Mock node properties
  mock_loader.get_node_properties.return_value = {
    "id": {"type": "STRING", "nullable": False},
    "name": {"type": "STRING", "nullable": True},
    "created_at": {"type": "TIMESTAMP", "nullable": False},
  }

  # Mock primary key
  mock_loader.get_node_primary_key.return_value = "id"

  # Mock relationship schema
  mock_rel = Mock()
  mock_rel.name = "HAS_ELEMENT"
  mock_rel.description = "Entity has element relationship"
  mock_rel.from_node = "Entity"
  mock_rel.to_node = "Element"
  mock_rel.properties = []
  mock_loader.get_relationship_schema.return_value = mock_rel

  # Mock node relationships
  mock_outgoing_rel = Mock()
  mock_outgoing_rel.name = "HAS_ELEMENT"
  mock_outgoing_rel.to_node = "Element"

  mock_incoming_rel = Mock()
  mock_incoming_rel.name = "BELONGS_TO"
  mock_incoming_rel.from_node = "Parent"

  mock_loader.get_node_relationships.return_value = {
    "outgoing": [mock_outgoing_rel],
    "incoming": [mock_incoming_rel],
  }

  return mock_loader


@pytest.fixture
def validator(mock_schema_loader):
  """Create a validator instance with mocked schema loader."""
  with patch(
    "robosystems.schemas.validator.get_schema_loader", return_value=mock_schema_loader
  ):
    return LadybugSchemaValidator()


class TestLadybugSchemaValidatorInit:
  """Test validator initialization."""

  @patch("robosystems.schemas.validator.get_schema_loader")
  def test_init_creates_schema_loader(self, mock_get_loader):
    """Test that initialization creates schema loader."""
    mock_loader = Mock()
    mock_loader.list_node_types.return_value = ["Entity", "Element"]
    mock_loader.list_relationship_types.return_value = ["HAS_ELEMENT"]
    mock_get_loader.return_value = mock_loader

    validator = LadybugSchemaValidator()

    mock_get_loader.assert_called_once()
    assert validator.schema_loader == mock_loader

  @patch("robosystems.schemas.validator.get_schema_loader")
  @patch("robosystems.schemas.validator.logger")
  def test_init_logs_schema_counts(self, mock_logger, mock_get_loader):
    """Test that initialization logs schema counts."""
    mock_loader = Mock()
    mock_loader.list_node_types.return_value = ["Entity", "Element"]
    mock_loader.list_relationship_types.return_value = ["HAS_ELEMENT"]
    mock_get_loader.return_value = mock_loader

    LadybugSchemaValidator()

    # Verify debug logs were called
    assert mock_logger.debug.call_count >= 3
    mock_logger.debug.assert_any_call("Initialized LadybugSchemaValidator")
    mock_logger.debug.assert_any_call("Loaded schemas for 2 node types")
    mock_logger.debug.assert_any_call("Loaded schemas for 1 relationship types")


class TestValidateNode:
  """Test node validation."""

  def test_validate_node_calls_schema_loader(self, validator, mock_schema_loader):
    """Test that validate_node calls schema loader correctly."""
    node_type = "Entity"
    properties = {"id": "ent_123", "name": "Test Entity"}

    result = validator.validate_node(node_type, properties)

    mock_schema_loader.validate_node_properties.assert_called_once_with(
      node_type, properties
    )
    assert result is True

  def test_validate_node_returns_loader_result(self, validator, mock_schema_loader):
    """Test that validate_node returns schema loader result."""
    mock_schema_loader.validate_node_properties.return_value = False

    result = validator.validate_node("Entity", {})

    assert result is False

  def test_validate_node_propagates_exceptions(self, validator, mock_schema_loader):
    """Test that validate_node propagates validation exceptions."""
    mock_schema_loader.validate_node_properties.side_effect = ValueError(
      "Invalid property"
    )

    with pytest.raises(ValueError, match="Invalid property"):
      validator.validate_node("Entity", {"invalid": "value"})


class TestValidateRelationship:
  """Test relationship validation."""

  def test_validate_relationship_without_properties(
    self, validator, mock_schema_loader
  ):
    """Test relationship validation without properties."""
    source_type = "Entity"
    target_type = "Element"
    relationship_type = "HAS_ELEMENT"

    result = validator.validate_relationship(
      source_type, target_type, relationship_type
    )

    mock_schema_loader.validate_relationship.assert_called_once_with(
      source_type, target_type, relationship_type, None
    )
    assert result is True

  def test_validate_relationship_with_properties(self, validator, mock_schema_loader):
    """Test relationship validation with properties."""
    source_type = "Entity"
    target_type = "Element"
    relationship_type = "HAS_ELEMENT"
    properties = {"weight": 1.0, "created_at": "2023-01-01"}

    result = validator.validate_relationship(
      source_type, target_type, relationship_type, properties
    )

    mock_schema_loader.validate_relationship.assert_called_once_with(
      source_type, target_type, relationship_type, properties
    )
    assert result is True

  def test_validate_relationship_returns_loader_result(
    self, validator, mock_schema_loader
  ):
    """Test that validate_relationship returns schema loader result."""
    mock_schema_loader.validate_relationship.return_value = False

    result = validator.validate_relationship("Entity", "Element", "INVALID_REL")

    assert result is False

  def test_validate_relationship_propagates_exceptions(
    self, validator, mock_schema_loader
  ):
    """Test that validate_relationship propagates validation exceptions."""
    mock_schema_loader.validate_relationship.side_effect = ValueError(
      "Invalid relationship"
    )

    with pytest.raises(ValueError, match="Invalid relationship"):
      validator.validate_relationship("Entity", "Element", "INVALID_REL")


class TestGetNodeSchema:
  """Test getting node schema."""

  def test_get_node_schema_returns_formatted_schema(
    self, validator, mock_schema_loader
  ):
    """Test that get_node_schema returns properly formatted schema."""
    node_type = "Entity"

    result = validator.get_node_schema(node_type)

    expected = {
      "name": "Entity",
      "description": "A business entity",
      "properties": {
        "id": {"type": "STRING", "nullable": False},
        "name": {"type": "STRING", "nullable": True},
        "created_at": {"type": "TIMESTAMP", "nullable": False},
      },
      "primary_key": "id",
    }

    assert result == expected

    # Verify all loader methods were called
    mock_schema_loader.get_node_schema.assert_called_once_with(node_type)
    mock_schema_loader.get_node_properties.assert_called_once_with(node_type)
    mock_schema_loader.get_node_primary_key.assert_called_once_with(node_type)

  def test_get_node_schema_returns_none_for_missing_node(
    self, validator, mock_schema_loader
  ):
    """Test that get_node_schema returns None for missing node type."""
    mock_schema_loader.get_node_schema.return_value = None

    result = validator.get_node_schema("NonExistentNode")

    assert result is None
    mock_schema_loader.get_node_schema.assert_called_once_with("NonExistentNode")
    # Other methods should not be called if node doesn't exist
    mock_schema_loader.get_node_properties.assert_not_called()
    mock_schema_loader.get_node_primary_key.assert_not_called()


class TestGetRelationshipSchema:
  """Test getting relationship schema."""

  def test_get_relationship_schema_returns_formatted_schema(
    self, validator, mock_schema_loader
  ):
    """Test that get_relationship_schema returns properly formatted schema."""
    relationship_type = "HAS_ELEMENT"

    # Create mock property objects
    mock_prop1 = Mock()
    mock_prop1.name = "weight"
    mock_prop1.type = "DOUBLE"
    mock_prop1.nullable = True

    mock_prop2 = Mock()
    mock_prop2.name = "created_at"
    mock_prop2.type = "TIMESTAMP"
    mock_prop2.nullable = False

    # Update mock relationship to have properties
    mock_rel = mock_schema_loader.get_relationship_schema.return_value
    mock_rel.properties = [mock_prop1, mock_prop2]

    result = validator.get_relationship_schema(relationship_type)

    expected = {
      "name": "HAS_ELEMENT",
      "description": "Entity has element relationship",
      "from_node": "Entity",
      "to_node": "Element",
      "properties": {
        "weight": {"type": "DOUBLE", "nullable": True},
        "created_at": {"type": "TIMESTAMP", "nullable": False},
      },
    }

    assert result == expected
    mock_schema_loader.get_relationship_schema.assert_called_once_with(
      relationship_type
    )

  def test_get_relationship_schema_returns_none_for_missing_relationship(
    self, validator, mock_schema_loader
  ):
    """Test that get_relationship_schema returns None for missing relationship type."""
    mock_schema_loader.get_relationship_schema.return_value = None

    result = validator.get_relationship_schema("NonExistentRelationship")

    assert result is None
    mock_schema_loader.get_relationship_schema.assert_called_once_with(
      "NonExistentRelationship"
    )

  def test_get_relationship_schema_with_empty_properties(
    self, validator, mock_schema_loader
  ):
    """Test relationship schema with no properties."""
    relationship_type = "HAS_ELEMENT"

    result = validator.get_relationship_schema(relationship_type)

    expected = {
      "name": "HAS_ELEMENT",
      "description": "Entity has element relationship",
      "from_node": "Entity",
      "to_node": "Element",
      "properties": {},
    }

    assert result == expected


class TestListMethods:
  """Test list methods."""

  def test_list_node_types(self, validator, mock_schema_loader):
    """Test listing node types."""
    # Reset call count since mock is called during validator init
    mock_schema_loader.list_node_types.reset_mock()

    result = validator.list_node_types()

    assert result == ["Entity", "Element", "Fact"]
    mock_schema_loader.list_node_types.assert_called_once()

  def test_list_relationship_types(self, validator, mock_schema_loader):
    """Test listing relationship types."""
    # Reset call count since mock is called during validator init
    mock_schema_loader.list_relationship_types.reset_mock()

    result = validator.list_relationship_types()

    assert result == ["HAS_ELEMENT", "HAS_FACT", "RELATES_TO"]
    mock_schema_loader.list_relationship_types.assert_called_once()


class TestGetNodeRelationships:
  """Test getting node relationships."""

  def test_get_node_relationships_returns_formatted_relationships(
    self, validator, mock_schema_loader
  ):
    """Test that get_node_relationships returns properly formatted relationships."""
    node_type = "Entity"

    result = validator.get_node_relationships(node_type)

    expected = {
      "outgoing": ["HAS_ELEMENT -> Element"],
      "incoming": ["Parent -> BELONGS_TO"],
    }

    assert result == expected
    mock_schema_loader.get_node_relationships.assert_called_once_with(node_type)

  def test_get_node_relationships_with_empty_relationships(
    self, validator, mock_schema_loader
  ):
    """Test get_node_relationships with no relationships."""
    mock_schema_loader.get_node_relationships.return_value = {
      "outgoing": [],
      "incoming": [],
    }

    result = validator.get_node_relationships("IsolatedNode")

    expected = {"outgoing": [], "incoming": []}

    assert result == expected

  def test_get_node_relationships_with_multiple_relationships(
    self, validator, mock_schema_loader
  ):
    """Test get_node_relationships with multiple relationships."""
    # Create multiple mock relationships
    mock_outgoing1 = Mock()
    mock_outgoing1.name = "HAS_ELEMENT"
    mock_outgoing1.to_node = "Element"

    mock_outgoing2 = Mock()
    mock_outgoing2.name = "HAS_FACT"
    mock_outgoing2.to_node = "Fact"

    mock_incoming1 = Mock()
    mock_incoming1.name = "OWNS"
    mock_incoming1.from_node = "User"

    mock_incoming2 = Mock()
    mock_incoming2.name = "MANAGES"
    mock_incoming2.from_node = "Manager"

    mock_schema_loader.get_node_relationships.return_value = {
      "outgoing": [mock_outgoing1, mock_outgoing2],
      "incoming": [mock_incoming1, mock_incoming2],
    }

    result = validator.get_node_relationships("Entity")

    expected = {
      "outgoing": ["HAS_ELEMENT -> Element", "HAS_FACT -> Fact"],
      "incoming": ["User -> OWNS", "Manager -> MANAGES"],
    }

    assert result == expected


class TestErrorHandling:
  """Test error handling scenarios."""

  def test_validator_handles_schema_loader_exceptions(self, mock_schema_loader):
    """Test that validator handles schema loader initialization exceptions."""
    with patch("robosystems.schemas.validator.get_schema_loader") as mock_get_loader:
      mock_get_loader.side_effect = Exception("Schema loader failed")

      with pytest.raises(Exception, match="Schema loader failed"):
        LadybugSchemaValidator()

  def test_node_validation_with_loader_exception(self, validator, mock_schema_loader):
    """Test node validation when loader raises exception."""
    mock_schema_loader.validate_node_properties.side_effect = RuntimeError(
      "Database error"
    )

    with pytest.raises(RuntimeError, match="Database error"):
      validator.validate_node("Entity", {"id": "test"})

  def test_relationship_validation_with_loader_exception(
    self, validator, mock_schema_loader
  ):
    """Test relationship validation when loader raises exception."""
    mock_schema_loader.validate_relationship.side_effect = RuntimeError("Schema error")

    with pytest.raises(RuntimeError, match="Schema error"):
      validator.validate_relationship("Entity", "Element", "HAS_ELEMENT")

  def test_get_node_schema_with_properties_exception(
    self, validator, mock_schema_loader
  ):
    """Test get_node_schema when get_node_properties raises exception."""
    mock_schema_loader.get_node_properties.side_effect = ValueError("Properties error")

    with pytest.raises(ValueError, match="Properties error"):
      validator.get_node_schema("Entity")

  def test_get_node_schema_with_primary_key_exception(
    self, validator, mock_schema_loader
  ):
    """Test get_node_schema when get_node_primary_key raises exception."""
    mock_schema_loader.get_node_primary_key.side_effect = ValueError(
      "Primary key error"
    )

    with pytest.raises(ValueError, match="Primary key error"):
      validator.get_node_schema("Entity")


class TestIntegrationScenarios:
  """Test integration-like scenarios."""

  def test_full_validation_workflow(self, validator, mock_schema_loader):
    """Test a complete validation workflow."""
    # Check available node types
    node_types = validator.list_node_types()
    assert "Entity" in node_types

    # Get schema for a node type
    schema = validator.get_node_schema("Entity")
    assert schema is not None
    assert "properties" in schema

    # Validate a node
    node_properties = {"id": "ent_123", "name": "Test Entity"}
    is_valid = validator.validate_node("Entity", node_properties)
    assert is_valid

    # Check available relationship types
    rel_types = validator.list_relationship_types()
    assert "HAS_ELEMENT" in rel_types

    # Validate a relationship
    is_rel_valid = validator.validate_relationship("Entity", "Element", "HAS_ELEMENT")
    assert is_rel_valid

    # Get node relationships
    relationships = validator.get_node_relationships("Entity")
    assert "outgoing" in relationships
    assert "incoming" in relationships

  def test_validation_with_complex_properties(self, validator, mock_schema_loader):
    """Test validation with complex property structures."""
    complex_properties = {
      "id": "ent_123",
      "name": "Complex Entity",
      "metadata": {"created_by": "user_456", "tags": ["important", "test"]},
      "metrics": [1.0, 2.5, 3.7],
    }

    result = validator.validate_node("Entity", complex_properties)

    mock_schema_loader.validate_node_properties.assert_called_once_with(
      "Entity", complex_properties
    )
    assert result is True

  def test_relationship_validation_with_complex_properties(
    self, validator, mock_schema_loader
  ):
    """Test relationship validation with complex properties."""
    relationship_properties = {
      "weight": 0.85,
      "created_at": "2023-01-01T00:00:00Z",
      "metadata": {"source": "automated", "confidence": 0.95},
    }

    result = validator.validate_relationship(
      "Entity", "Element", "HAS_ELEMENT", relationship_properties
    )

    mock_schema_loader.validate_relationship.assert_called_once_with(
      "Entity", "Element", "HAS_ELEMENT", relationship_properties
    )
    assert result is True
