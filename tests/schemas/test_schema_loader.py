"""Schema loader tests."""

import pytest

from robosystems.schemas.loader import (
  ContextAwareSchemaLoader,
  LadybugSchemaLoader,
  get_contextual_schema_loader,
  get_schema_loader,
  get_sec_schema_loader,
)


class TestLadybugSchemaLoaderInit:
  """Tests for LadybugSchemaLoader initialization."""

  def test_load_all_extensions(self):
    loader = LadybugSchemaLoader()
    assert len(loader.nodes) > 0
    assert len(loader.relationships) > 0

  def test_load_specific_extension(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    assert "roboledger" in loader.loaded_extensions
    assert len(loader.nodes) > 0

  def test_load_base_only(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert len(loader.loaded_extensions) == 0
    assert len(loader.nodes) > 0  # Still has base nodes

  def test_invalid_extension_skipped(self):
    loader = LadybugSchemaLoader(extensions=["nonexistent_xyz"])
    assert "nonexistent_xyz" not in loader.loaded_extensions

  def test_memory_extension(self):
    loader = LadybugSchemaLoader(extensions=["memory"])
    assert "memory" in loader.loaded_extensions


class TestNodeOperations:
  """Tests for node-related operations."""

  def test_get_node_schema(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    node = loader.get_node_schema("Entity")
    assert node is not None
    assert node.name == "Entity"

  def test_get_unknown_node(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader.get_node_schema("NonExistentNode") is None

  def test_list_node_types(self):
    loader = LadybugSchemaLoader(extensions=[])
    node_types = loader.list_node_types()
    assert isinstance(node_types, list)
    assert len(node_types) > 0
    assert "Entity" in node_types

  def test_get_node_properties(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    props = loader.get_node_properties("Entity")
    assert isinstance(props, dict)
    assert len(props) > 0
    # Check property metadata structure
    for name, meta in props.items():
      assert "type" in meta
      assert "is_primary_key" in meta
      assert "nullable" in meta

  def test_get_node_properties_unknown(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader.get_node_properties("Unknown") == {}

  def test_get_node_primary_key(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    pk = loader.get_node_primary_key("Entity")
    assert pk is not None
    assert isinstance(pk, str)

  def test_get_node_primary_key_unknown(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader.get_node_primary_key("Unknown") is None


class TestRelationshipOperations:
  """Tests for relationship-related operations."""

  def test_get_relationship_schema(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    rel = loader.get_relationship_schema("ENTITY_HAS_REPORT")
    assert rel is not None

  def test_get_unknown_relationship(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader.get_relationship_schema("NONEXISTENT_REL") is None

  def test_list_relationship_types(self):
    loader = LadybugSchemaLoader(extensions=[])
    rel_types = loader.list_relationship_types()
    assert isinstance(rel_types, list)
    assert len(rel_types) > 0

  def test_get_node_relationships(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    rels = loader.get_node_relationships("Entity")
    assert "outgoing" in rels
    assert "incoming" in rels
    assert isinstance(rels["outgoing"], list)
    assert isinstance(rels["incoming"], list)


class TestNodePropertyValidation:
  """Tests for validate_node_properties."""

  def test_valid_properties(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    pk = loader.get_node_primary_key("Entity")
    result = loader.validate_node_properties("Entity", {pk: "test_value"})
    assert result is True

  def test_unknown_node_type(self):
    loader = LadybugSchemaLoader(extensions=[])
    with pytest.raises(ValueError, match="not defined"):
      loader.validate_node_properties("Unknown", {"id": "test"})

  def test_missing_required_property(self):
    loader = LadybugSchemaLoader(extensions=["roboledger"])
    # Find a node with non-nullable properties
    for node_name, node in loader.nodes.items():
      non_nullable = [p for p in node.properties if not p.nullable]
      if non_nullable:
        with pytest.raises(ValueError, match="Required property"):
          loader.validate_node_properties(node_name, {})
        return
    # If all properties are nullable, validate passes with empty dict
    pk = loader.get_node_primary_key("Entity")
    result = loader.validate_node_properties("Entity", {pk: "test"})
    assert result is True


class TestRelationshipValidation:
  """Tests for validate_relationship."""

  def test_unknown_relationship_type(self):
    loader = LadybugSchemaLoader(extensions=[])
    with pytest.raises(ValueError, match="not defined"):
      loader.validate_relationship("A", "B", "NONEXISTENT_REL")


class TestTypeCompatibility:
  """Tests for _check_type_compatibility."""

  def test_string_type(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader._check_type_compatibility("hello", "STRING") is True
    assert loader._check_type_compatibility(42, "STRING") is False

  def test_int64_type(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader._check_type_compatibility(42, "INT64") is True
    assert loader._check_type_compatibility("42", "INT64") is False

  def test_double_type_accepts_int(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader._check_type_compatibility(3.14, "DOUBLE") is True
    assert loader._check_type_compatibility(42, "DOUBLE") is True

  def test_boolean_type(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader._check_type_compatibility(True, "BOOLEAN") is True
    assert loader._check_type_compatibility("true", "BOOLEAN") is False

  def test_null_always_compatible(self):
    loader = LadybugSchemaLoader(extensions=[])
    assert loader._check_type_compatibility(None, "STRING") is True
    assert loader._check_type_compatibility(None, "INT64") is True


class TestGlobalLoaderFunctions:
  """Tests for module-level loader functions."""

  def test_get_schema_loader_default(self):
    loader = get_schema_loader()
    assert isinstance(loader, LadybugSchemaLoader)

  def test_get_schema_loader_with_extensions(self):
    loader = get_schema_loader(extensions=["roboledger"])
    assert "roboledger" in loader.loaded_extensions

  def test_get_schema_loader_caching(self):
    loader1 = get_schema_loader(extensions=["roboledger"])
    loader2 = get_schema_loader(extensions=["roboledger"])
    assert loader1 is loader2

  def test_get_sec_schema_loader(self):
    loader = get_sec_schema_loader()
    assert isinstance(loader, LadybugSchemaLoader)


class TestContextAwareSchemaLoader:
  """Tests for ContextAwareSchemaLoader."""

  def test_sec_repository_context(self):
    loader = ContextAwareSchemaLoader(extension="roboledger", context="sec_repository")
    assert len(loader.nodes) > 0
    assert len(loader.relationships) > 0

  def test_full_accounting_context(self):
    loader = ContextAwareSchemaLoader(extension="roboledger", context="full_accounting")
    assert len(loader.nodes) > 0

  def test_get_contextual_schema_loader_sec(self):
    loader = get_contextual_schema_loader("repository", "sec")
    assert isinstance(loader, ContextAwareSchemaLoader)

  def test_get_contextual_schema_loader_roboledger(self):
    loader = get_contextual_schema_loader("application", "roboledger")
    assert isinstance(loader, ContextAwareSchemaLoader)

  def test_get_contextual_schema_loader_other(self):
    loader = get_contextual_schema_loader("application", "roboinvestor")
    assert isinstance(loader, LadybugSchemaLoader)

  def test_get_contextual_schema_loader_admin(self):
    loader = get_contextual_schema_loader("application", "robosystems")
    assert isinstance(loader, LadybugSchemaLoader)

  def test_get_contextual_with_additional_extensions(self):
    loader = get_contextual_schema_loader(
      "application", "robosystems", additional_extensions=["memory"]
    )
    assert isinstance(loader, LadybugSchemaLoader)


class TestREAPrimitives:
  """Phase 1 ontology alignment: Agent + Event in base, EVENT_TRIGGERS_TRANSACTION
  bridge in roboledger TRANSACTION layer.

  Validates that the spec §4.5 placement decision actually holds in the loader:
  REA primitives reach every consumer (including SEC) as base nodes, while the
  McCarthy bridge edge is correctly scoped to roboledger TRANSACTION_RELATIONSHIPS
  and absent from SEC.
  """

  def test_agent_and_event_in_default_loader(self):
    loader = get_schema_loader()
    assert "Agent" in loader.nodes
    assert "Event" in loader.nodes

  def test_agent_and_event_in_roboledger_application(self):
    loader = get_contextual_schema_loader("application", "roboledger")
    assert "Agent" in loader.nodes
    assert "Event" in loader.nodes

  def test_agent_and_event_in_sec_repository(self):
    """Base nodes flow through to SEC. Tables stay empty (no rows
    loaded), but the schema includes them per spec §4.5 Option A."""
    loader = ContextAwareSchemaLoader(extension="roboledger", context="sec_repository")
    assert "Agent" in loader.nodes
    assert "Event" in loader.nodes

  def test_rea_edges_in_default_loader(self):
    loader = get_schema_loader()
    for edge in (
      "ENTITY_HAS_AGENT",
      "ENTITY_HAS_EVENT",
      "EVENT_INVOLVES_AGENT",
      "EVENT_AFFECTS_RESOURCE",
      "EVENT_OBLIGATED_BY_EVENT",
      "EVENT_DISCHARGES_EVENT",
      "EVENT_REPLACES_EVENT",
    ):
      assert edge in loader.relationships, f"missing base edge: {edge}"

  def test_event_action_property_on_event_node(self):
    """The canonical action verb refinement is materialized as a graph property."""
    loader = get_schema_loader()
    event = loader.nodes["Event"]
    prop_names = {p.name for p in event.properties}
    assert "event_action" in prop_names

  def test_event_triggers_transaction_in_roboledger(self):
    loader = get_contextual_schema_loader("application", "roboledger")
    assert "EVENT_TRIGGERS_TRANSACTION" in loader.relationships
    edge = loader.relationships["EVENT_TRIGGERS_TRANSACTION"]
    assert edge.from_node == "Event"
    assert edge.to_node == "Transaction"

  def test_event_triggers_transaction_absent_from_sec(self):
    """Bridge edge is roboledger TRANSACTION-scoped; SEC uses REPORTING_ONLY
    context so the bridge is correctly excluded."""
    loader = ContextAwareSchemaLoader(extension="roboledger", context="sec_repository")
    assert "EVENT_TRIGGERS_TRANSACTION" not in loader.relationships

  def test_fiscal_nodes_not_in_graph_layer(self):
    """FiscalCalendar / FiscalPeriod stay OLTP-only per the §2.1 test
    (operational state, not curated business truth). Guards against
    accidental re-introduction during future graph work."""
    loader = get_contextual_schema_loader("application", "roboledger")
    assert "FiscalCalendar" not in loader.nodes
    assert "FiscalPeriod" not in loader.nodes
