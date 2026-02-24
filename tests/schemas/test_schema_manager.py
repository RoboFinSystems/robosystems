"""Schema manager tests."""

import pytest

from robosystems.schemas.manager import (
  SchemaCompatibility,
  SchemaConfiguration,
  SchemaManager,
  SchemaType,
  create_accounting_schema,
  create_roboledger_schema,
  get_recommended_schema_groups,
)


@pytest.fixture
def manager():
  return SchemaManager()


class TestSchemaType:
  """Tests for SchemaType enum."""

  def test_base_type(self):
    assert SchemaType.BASE.value == "base"

  def test_roboledger_type(self):
    assert SchemaType.ROBOLEDGER.value == "roboledger"

  def test_memory_type(self):
    assert SchemaType.MEMORY.value == "memory"


class TestSchemaConfiguration:
  """Tests for SchemaConfiguration dataclass."""

  def test_create_configuration(self):
    config = SchemaConfiguration(
      name="test",
      description="test desc",
      version="1.0.0",
      base_schema="base",
      extensions=["roboledger"],
    )
    assert config.name == "test"
    assert config.extensions == ["roboledger"]


class TestCreateSchemaConfiguration:
  """Tests for SchemaManager.create_schema_configuration."""

  def test_default_config(self, manager):
    config = manager.create_schema_configuration(name="test", description="test desc")
    assert config.base_schema == "base"
    assert config.extensions == []
    assert config.version == "1.0.0"

  def test_with_extensions(self, manager):
    config = manager.create_schema_configuration(
      name="test",
      description="test desc",
      extensions=["roboledger", "memory"],
    )
    assert config.extensions == ["roboledger", "memory"]

  def test_without_base(self, manager):
    config = manager.create_schema_configuration(
      name="test",
      description="test desc",
      include_base=False,
    )
    assert config.base_schema == ""

  def test_custom_version(self, manager):
    config = manager.create_schema_configuration(
      name="test", description="test desc", version="2.0.0"
    )
    assert config.version == "2.0.0"


class TestLoadAndCompileSchema:
  """Tests for schema loading and compilation."""

  def test_base_schema_only(self, manager):
    config = manager.create_schema_configuration(
      name="Base Only", description="Base schema"
    )
    schema = manager.load_and_compile_schema(config)
    assert len(schema.nodes) > 0
    assert len(schema.relationships) > 0

  def test_base_with_roboledger(self, manager):
    config = manager.create_schema_configuration(
      name="Ledger",
      description="With roboledger",
      extensions=["roboledger"],
    )
    schema = manager.load_and_compile_schema(config)
    assert len(schema.nodes) > 0

  def test_base_with_memory(self, manager):
    config = manager.create_schema_configuration(
      name="Memory",
      description="With memory",
      extensions=["memory"],
    )
    schema = manager.load_and_compile_schema(config)
    assert len(schema.nodes) > 0

  def test_caching(self, manager):
    config = manager.create_schema_configuration(
      name="Cache Test", description="Test caching"
    )
    schema1 = manager.load_and_compile_schema(config)
    schema2 = manager.load_and_compile_schema(config)
    assert schema1 is schema2  # Same cached object

  def test_invalid_extension_raises(self, manager):
    config = manager.create_schema_configuration(
      name="Bad",
      description="bad",
      extensions=["nonexistent_extension_xyz"],
    )
    with pytest.raises(ValueError, match="not found"):
      manager.load_and_compile_schema(config)

  def test_extension_only_mode(self, manager):
    config = manager.create_schema_configuration(
      name="Extension Only",
      description="No base",
      include_base=False,
      extensions=["memory"],
    )
    schema = manager.load_and_compile_schema(config)
    assert schema is not None


class TestSchemaConsistencyValidation:
  """Tests for schema consistency validation."""

  def test_base_schema_is_consistent(self, manager):
    config = manager.create_schema_configuration(name="Consistent", description="test")
    # Should not raise
    schema = manager.load_and_compile_schema(config)
    assert schema is not None


class TestCheckSchemaCompatibility:
  """Tests for schema compatibility checking."""

  def test_single_extension_compatible(self, manager):
    result = manager.check_schema_compatibility(["roboledger"])
    assert isinstance(result, SchemaCompatibility)
    assert result.compatible is True

  def test_memory_compatible(self, manager):
    result = manager.check_schema_compatibility(["memory"])
    assert result.compatible is True

  def test_compatibility_cached(self, manager):
    result1 = manager.check_schema_compatibility(["roboledger"])
    result2 = manager.check_schema_compatibility(["roboledger"])
    assert result1 is result2

  def test_invalid_extension_has_conflict(self, manager):
    result = manager.check_schema_compatibility(["nonexistent_xyz"])
    assert result.compatible is False
    assert len(result.conflicts) > 0


class TestGetOptimalSchemaGroups:
  """Tests for get_optimal_schema_groups."""

  def test_returns_groups(self, manager):
    groups = manager.get_optimal_schema_groups()
    assert isinstance(groups, dict)
    assert "financial_core" in groups
    assert "standalone_investment" in groups

  def test_groups_contain_lists(self, manager):
    groups = manager.get_optimal_schema_groups()
    for name, extensions in groups.items():
      assert isinstance(extensions, list)
      assert len(extensions) > 0


class TestGenerateCypherDDL:
  """Tests for DDL generation."""

  def test_generates_ddl(self, manager):
    config = manager.create_schema_configuration(name="DDL Test", description="test")
    schema = manager.load_and_compile_schema(config)
    ddl = manager.generate_cypher_ddl(schema)
    assert isinstance(ddl, str)
    assert "CREATE" in ddl


class TestGetSchemaStatistics:
  """Tests for schema statistics."""

  def test_statistics(self, manager):
    config = manager.create_schema_configuration(name="Stats Test", description="test")
    schema = manager.load_and_compile_schema(config)
    stats = manager.get_schema_statistics(schema)
    assert stats["name"] == "Stats Test"
    assert stats["total_nodes"] > 0
    assert stats["total_relationships"] > 0
    assert isinstance(stats["node_names"], list)
    assert isinstance(stats["relationship_names"], list)
    assert stats["estimated_ddl_size"] > 0


class TestListAvailableExtensions:
  """Tests for list_available_extensions."""

  def test_lists_extensions(self, manager):
    extensions = manager.list_available_extensions()
    assert isinstance(extensions, list)
    assert len(extensions) > 0

  def test_extension_format(self, manager):
    extensions = manager.list_available_extensions()
    for ext in extensions:
      assert "name" in ext
      assert "type" in ext
      assert "description" in ext
      assert "available" in ext

  def test_base_not_in_extensions(self, manager):
    extensions = manager.list_available_extensions()
    names = [ext["name"] for ext in extensions]
    assert "base" not in names


class TestClearCache:
  """Tests for cache clearing."""

  def test_clear_cache(self, manager):
    # Populate cache
    config = manager.create_schema_configuration(name="Cache Clear", description="test")
    manager.load_and_compile_schema(config)
    assert len(manager._schema_cache) > 0

    manager.clear_cache()
    assert len(manager._schema_cache) == 0
    assert len(manager._module_cache) == 0
    assert len(manager._compatibility_cache) == 0


class TestConvenienceFunctions:
  """Tests for module-level convenience functions."""

  def test_create_roboledger_schema(self):
    schema = create_roboledger_schema()
    assert len(schema.nodes) > 0
    assert len(schema.relationships) > 0

  def test_create_accounting_schema(self):
    schema = create_accounting_schema()
    assert len(schema.nodes) > 0

  def test_get_recommended_schema_groups(self):
    groups = get_recommended_schema_groups()
    assert isinstance(groups, dict)
    assert len(groups) > 0
