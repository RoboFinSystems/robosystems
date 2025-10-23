"""
Custom Schema Support for Kuzu

This module provides support for custom schema definitions via JSON/YAML,
allowing users to define their own graph schemas without modifying code.
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
import json
import yaml
from enum import Enum

from .models import Schema, Node, Relationship, Property
from robosystems.logger import logger


class SchemaFormat(Enum):
  """Supported schema definition formats."""

  JSON = "json"
  YAML = "yaml"
  DICT = "dict"


@dataclass
class CustomSchemaDefinition:
  """
  Custom schema definition structure.

  This represents the JSON/YAML structure that users provide to define custom schemas.
  """

  name: str
  version: str = "1.0.0"
  description: Optional[str] = None
  extends: Optional[str] = None  # Can extend 'base' or be completely custom
  nodes: List[Dict[str, Any]] = field(default_factory=list)
  relationships: List[Dict[str, Any]] = field(default_factory=list)
  metadata: Dict[str, Any] = field(default_factory=dict)


class CustomSchemaParser:
  """
  Parser for custom schemas.

  Converts JSON/YAML schema definitions into Schema objects.
  """

  # Supported Kuzu data types
  VALID_TYPES = {
    "STRING",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
    "INT128",
    "UINT8",
    "UINT16",
    "UINT32",
    "UINT64",
    "FLOAT",
    "DOUBLE",
    "BOOLEAN",
    "BLOB",
    "DATE",
    "TIMESTAMP",
    "INTERVAL",
    "UUID",
    "LIST",
    "MAP",
    "STRUCT",
    "UNION",
    "NODE",
    "REL",
  }

  # Reserved node names that cannot be used in custom schemas
  # NOTE: Platform metadata (GraphMetadata, User, Connection) are stored in PostgreSQL,
  # not in the Kuzu graph database. These reserved names are for future system nodes.
  RESERVED_NODE_NAMES = {
    "SystemConfig",  # System configuration node
    "SchemaVersion",  # Schema versioning node
    "AuditLog",  # Audit logging node
    "Permission",  # Permission management node
    "Role",  # Role management node
    "Session",  # Session management node
    "Lock",  # Database lock node
    "Migration",  # Schema migration tracking
    "SystemUser",  # System-level user (different from User)
  }

  # Reserved relationship names that cannot be used in custom schemas
  RESERVED_RELATIONSHIP_NAMES = {
    "SYSTEM_OWNS",
    "SYSTEM_MANAGES",
    "SYSTEM_LOGS",
    "SYSTEM_GRANTS",
    "SYSTEM_DENIES",
    "SYSTEM_MIGRATES",
    "SYSTEM_LOCKS",
  }

  def parse(
    self,
    schema_input: Union[str, Dict[str, Any]],
    format: SchemaFormat = SchemaFormat.JSON,
  ) -> Schema:
    """
    Parse custom schema from various formats.

    Args:
        schema_input: Schema definition as string or dict
        format: Input format (JSON, YAML, or DICT)

    Returns:
        Compiled Schema object

    Raises:
        ValueError: If schema is invalid
    """
    # Parse input based on format
    if format == SchemaFormat.DICT:
      # For DICT format, ensure we have a dictionary
      if isinstance(schema_input, dict):
        schema_dict: Dict[str, Any] = schema_input
      else:
        raise ValueError("DICT format requires dictionary input")
    elif format == SchemaFormat.JSON:
      if isinstance(schema_input, str):
        schema_dict = json.loads(schema_input)
      elif isinstance(schema_input, dict):
        schema_dict = schema_input
      else:
        raise ValueError("JSON format requires string or dict input")
    else:  # YAML
      if isinstance(schema_input, str):
        schema_dict = yaml.safe_load(schema_input)
        # Validate immediately after YAML parsing
        if not isinstance(schema_dict, dict):
          raise ValueError(
            f"Failed to parse YAML into dictionary, got {type(schema_dict)}"
          )
      elif isinstance(schema_input, dict):
        schema_dict = schema_input
      else:
        raise ValueError("YAML format requires string or dict input")

    # Validate required fields
    self._validate_schema_structure(schema_dict)

    # Create schema definition
    definition = CustomSchemaDefinition(
      name=schema_dict["name"],
      version=schema_dict.get("version", "1.0.0"),
      description=schema_dict.get("description"),
      extends=schema_dict.get("extends"),
      nodes=schema_dict.get("nodes", []),
      relationships=schema_dict.get("relationships", []),
      metadata=schema_dict.get("metadata", {}),
    )

    # Convert to Schema object
    return self._build_schema(definition)

  def _validate_schema_structure(self, schema_dict: Dict[str, Any]):
    """Validate the basic structure of the schema definition."""
    if not isinstance(schema_dict, dict):
      raise ValueError("Schema must be a dictionary")

    if "name" not in schema_dict:
      raise ValueError("Schema must have a 'name' field")

    if not isinstance(schema_dict.get("nodes", []), list):
      raise ValueError("'nodes' must be a list")

    if not isinstance(schema_dict.get("relationships", []), list):
      raise ValueError("'relationships' must be a list")

  def _build_schema(self, definition: CustomSchemaDefinition) -> Schema:
    """Convert schema definition to Schema object."""
    schema = Schema(
      name=definition.name,
      version=definition.version,
      description=definition.description,
    )

    # Build nodes
    for node_def in definition.nodes:
      node = self._build_node(node_def)
      schema.nodes.append(node)

    # Build relationships
    for rel_def in definition.relationships:
      relationship = self._build_relationship(rel_def, schema)
      schema.relationships.append(relationship)

    # Validate the complete schema
    self._validate_complete_schema(schema)

    return schema

  def _build_node(self, node_def: Dict[str, Any]) -> Node:
    """Build a Node from definition."""
    # Validate required fields
    if "name" not in node_def:
      raise ValueError("Node must have a 'name' field")

    if "properties" not in node_def:
      raise ValueError(f"Node '{node_def['name']}' must have 'properties' field")

    # Check for reserved node names
    node_name = node_def["name"]
    if node_name in self.RESERVED_NODE_NAMES:
      raise ValueError(
        f"Node name '{node_name}' is reserved for system use. "
        f"Reserved names: {', '.join(sorted(self.RESERVED_NODE_NAMES))}"
      )

    # Build properties
    properties = []
    has_primary_key = False

    for prop_def in node_def["properties"]:
      prop = self._build_property(prop_def)
      if prop.is_primary_key:
        has_primary_key = True
      properties.append(prop)

    # Ensure at least one primary key
    if not has_primary_key:
      raise ValueError(f"Node '{node_def['name']}' must have at least one primary key")

    return Node(
      name=node_def["name"],
      properties=properties,
      description=node_def.get("description"),
    )

  def _build_property(self, prop_def: Dict[str, Any]) -> Property:
    """Build a Property from definition."""
    # Validate required fields
    if "name" not in prop_def:
      raise ValueError("Property must have a 'name' field")

    if "type" not in prop_def:
      raise ValueError(f"Property '{prop_def['name']}' must have a 'type' field")

    # Validate type
    prop_type = prop_def["type"].upper()
    if prop_type not in self.VALID_TYPES:
      raise ValueError(
        f"Invalid type '{prop_type}' for property '{prop_def['name']}'. Valid types: {self.VALID_TYPES}"
      )

    return Property(
      name=prop_def["name"],
      type=prop_type,
      is_primary_key=prop_def.get("is_primary_key", False),
      nullable=prop_def.get("nullable", True),
    )

  def _build_relationship(
    self, rel_def: Dict[str, Any], schema: Schema
  ) -> Relationship:
    """Build a Relationship from definition."""
    # Validate required fields
    if "name" not in rel_def:
      raise ValueError("Relationship must have a 'name' field")

    if "from_node" not in rel_def:
      raise ValueError(f"Relationship '{rel_def['name']}' must have 'from_node' field")

    if "to_node" not in rel_def:
      raise ValueError(f"Relationship '{rel_def['name']}' must have 'to_node' field")

    # Check for reserved relationship names
    rel_name = rel_def["name"]
    if rel_name in self.RESERVED_RELATIONSHIP_NAMES:
      raise ValueError(
        f"Relationship name '{rel_name}' is reserved for system use. "
        f"Reserved names: {', '.join(sorted(self.RESERVED_RELATIONSHIP_NAMES))}"
      )

    # Build properties if provided
    properties = []
    if "properties" in rel_def:
      for prop_def in rel_def["properties"]:
        prop = self._build_property(prop_def)
        properties.append(prop)

    return Relationship(
      name=rel_def["name"],
      from_node=rel_def["from_node"],
      to_node=rel_def["to_node"],
      properties=properties,
      description=rel_def.get("description"),
    )

  def _validate_complete_schema(self, schema: Schema):
    """Validate the complete schema for consistency."""
    # Check for duplicate node names
    node_names = [node.name for node in schema.nodes]
    duplicates = set([name for name in node_names if node_names.count(name) > 1])
    if duplicates:
      raise ValueError(f"Duplicate node names found: {duplicates}")

    # Check for duplicate relationship names
    rel_names = [rel.name for rel in schema.relationships]
    duplicates = set([name for name in rel_names if rel_names.count(name) > 1])
    if duplicates:
      raise ValueError(f"Duplicate relationship names found: {duplicates}")

    # Validate relationship node references
    node_name_set = set(node_names)
    for rel in schema.relationships:
      if rel.from_node != "*" and rel.from_node not in node_name_set:
        raise ValueError(
          f"Relationship '{rel.name}' references unknown from_node: '{rel.from_node}'"
        )
      if rel.to_node != "*" and rel.to_node not in node_name_set:
        raise ValueError(
          f"Relationship '{rel.name}' references unknown to_node: '{rel.to_node}'"
        )


class CustomSchemaManager:
  """
  Manager for custom schemas.

  Handles parsing, validation, storage, and integration with the main schema system.
  """

  def __init__(self):
    self.parser = CustomSchemaParser()

  def create_from_json(self, json_str: str) -> Schema:
    """Create schema from JSON string."""
    return self.parser.parse(json_str, SchemaFormat.JSON)

  def create_from_yaml(self, yaml_str: str) -> Schema:
    """Create schema from YAML string."""
    return self.parser.parse(yaml_str, SchemaFormat.YAML)

  def create_from_dict(self, schema_dict: Dict[str, Any]) -> Schema:
    """Create schema from dictionary."""
    return self.parser.parse(schema_dict, SchemaFormat.DICT)

  def merge_with_base(self, user_schema: Schema) -> Schema:
    """
    Merge custom schema with base schema.

    This allows users to extend the base schema with custom nodes and relationships.
    """
    from .base import BASE_NODES, BASE_RELATIONSHIPS
    from .models import Schema

    # Create merged schema
    merged = Schema(
      name=f"{user_schema.name} (Extended)",
      version=user_schema.version,
      description=f"Custom schema extending base: {user_schema.description}",
    )

    # Add base nodes first
    merged.nodes.extend(BASE_NODES)

    # Add user nodes (checking for conflicts)
    base_node_names = {node.name for node in BASE_NODES}
    for user_node in user_schema.nodes:
      if user_node.name in base_node_names:
        logger.warning(
          f"User node '{user_node.name}' conflicts with base node, skipping"
        )
      elif user_node.name in self.parser.RESERVED_NODE_NAMES:
        logger.warning(
          f"User node '{user_node.name}' uses reserved system name, skipping"
        )
      else:
        merged.nodes.append(user_node)

    # Add base relationships
    merged.relationships.extend(BASE_RELATIONSHIPS)

    # Add user relationships (checking for conflicts)
    base_rel_names = {rel.name for rel in BASE_RELATIONSHIPS}
    for user_rel in user_schema.relationships:
      if user_rel.name in base_rel_names:
        logger.warning(
          f"User relationship '{user_rel.name}' conflicts with base relationship, skipping"
        )
      elif user_rel.name in self.parser.RESERVED_RELATIONSHIP_NAMES:
        logger.warning(
          f"User relationship '{user_rel.name}' uses reserved system name, skipping"
        )
      else:
        merged.relationships.append(user_rel)

    return merged

  def validate_json_schema(self, json_str: str) -> Dict[str, Any]:
    """
    Validate a JSON schema definition without creating Schema object.

    Returns validation result with any errors found.
    """
    try:
      schema = self.create_from_json(json_str)
      return {
        "valid": True,
        "message": "Schema is valid",
        "stats": {
          "nodes": len(schema.nodes),
          "relationships": len(schema.relationships),
          "total_properties": sum(len(node.properties) for node in schema.nodes),
        },
      }
    except Exception as e:
      return {"valid": False, "message": str(e), "error_type": type(e).__name__}
