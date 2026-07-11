"""Tests for the knowledge schema extension (formerly "memory")."""

import pytest

from robosystems.schemas.extensions.knowledge import (
  EXTENSION_NODES,
  EXTENSION_RELATIONSHIPS,
)
from robosystems.schemas.runtime.manager import SchemaManager


class TestKnowledgeSchemaExtension:
  """Tests for the knowledge schema extension definitions."""

  @pytest.mark.unit
  def test_extension_nodes_defined(self):
    assert len(EXTENSION_NODES) == 3
    node_names = {n.name for n in EXTENSION_NODES}
    assert node_names == {"Concept", "Observation", "Session"}

  @pytest.mark.unit
  def test_extension_relationships_defined(self):
    assert len(EXTENSION_RELATIONSHIPS) == 4
    rel_names = {r.name for r in EXTENSION_RELATIONSHIPS}
    assert rel_names == {
      "CONCEPT_RELATES_TO",
      "OBSERVATION_ABOUT",
      "SESSION_PRODUCED",
      "SESSION_REFERENCED",
    }

  @pytest.mark.unit
  def test_all_nodes_have_primary_key(self):
    for node in EXTENSION_NODES:
      pks = [p for p in node.properties if p.is_primary_key]
      assert len(pks) >= 1, f"Node {node.name} has no primary key"

  @pytest.mark.unit
  def test_concept_node_properties(self):
    concept = next(n for n in EXTENSION_NODES if n.name == "Concept")
    prop_names = {p.name for p in concept.properties}
    assert "identifier" in prop_names
    assert "name" in prop_names
    assert "category" in prop_names
    assert "confidence" in prop_names

  @pytest.mark.unit
  def test_nodes_generate_valid_ddl(self):
    for node in EXTENSION_NODES:
      ddl = node.to_cypher()
      assert f"CREATE NODE TABLE IF NOT EXISTS {node.name}" in ddl
      assert "PRIMARY KEY" in ddl

  @pytest.mark.unit
  def test_relationships_generate_valid_ddl(self):
    for rel in EXTENSION_RELATIONSHIPS:
      ddl = rel.to_cypher()
      assert f"CREATE REL TABLE IF NOT EXISTS {rel.name}" in ddl
      assert f"FROM {rel.from_node} TO {rel.to_node}" in ddl


class TestSchemaManagerKnowledgeSupport:
  """Tests for SchemaManager with the knowledge extension."""

  @pytest.mark.unit
  def test_knowledge_extension_loads(self):
    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="KnowledgeTest",
      description="Test knowledge schema",
      extensions=["knowledge"],
    )
    schema = manager.load_and_compile_schema(config)
    node_names = {n.name for n in schema.nodes}
    # Should have base nodes + knowledge nodes
    assert "Concept" in node_names
    assert "Observation" in node_names
    assert "Session" in node_names
    assert "Entity" in node_names  # base schema included

  @pytest.mark.unit
  def test_knowledge_extension_without_base(self):
    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="KnowledgeOnly",
      description="Knowledge schema without base",
      extensions=["knowledge"],
      include_base=False,
    )
    schema = manager.load_and_compile_schema(config)
    node_names = {n.name for n in schema.nodes}
    # Should have only knowledge nodes, no base
    assert "Concept" in node_names
    assert "Observation" in node_names
    assert "Session" in node_names
    assert "Entity" not in node_names
    assert "Period" not in node_names

  @pytest.mark.unit
  def test_knowledge_only_generates_valid_ddl(self):
    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="KnowledgeOnly",
      description="Knowledge schema without base",
      extensions=["knowledge"],
      include_base=False,
    )
    schema = manager.load_and_compile_schema(config)
    ddl = schema.to_cypher()
    assert "Concept" in ddl
    assert "CONCEPT_RELATES_TO" in ddl
    # Should NOT have base schema tables
    assert "Entity" not in ddl

  @pytest.mark.unit
  def test_knowledge_schema_statistics(self):
    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="KnowledgeOnly",
      description="Knowledge schema without base",
      extensions=["knowledge"],
      include_base=False,
    )
    schema = manager.load_and_compile_schema(config)
    stats = manager.get_schema_statistics(schema)
    assert stats["total_nodes"] == 3
    assert stats["total_relationships"] == 4

  @pytest.mark.unit
  def test_legacy_memory_alias_resolves_to_knowledge(self):
    """A subgraph persisted with the legacy "memory" extension must still load
    the knowledge schema (backward-compat alias in SchemaManager)."""
    manager = SchemaManager()
    config = manager.create_schema_configuration(
      name="LegacyMemory",
      description="Legacy memory alias",
      extensions=["memory"],
      include_base=False,
    )
    schema = manager.load_and_compile_schema(config)
    node_names = {n.name for n in schema.nodes}
    assert node_names == {"Concept", "Observation", "Session"}
