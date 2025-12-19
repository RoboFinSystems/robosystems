"""
LadybugDB Schema Validator

Validates node and relationship operations against predefined LadybugDB schema
using the robosystems.schemas definitions as the source of truth.
"""

import logging
from typing import Any

from .loader import get_schema_loader

logger = logging.getLogger(__name__)


class LadybugSchemaValidator:
  """
  Validates LadybugDB operations against the robosystems schema.

  Uses robosystems.schemas as the source of truth for all validation.
  """

  def __init__(self):
    self.schema_loader = get_schema_loader()

    logger.debug("Initialized LadybugSchemaValidator")
    logger.debug(
      f"Loaded schemas for {len(self.schema_loader.list_node_types())} node types"
    )
    logger.debug(
      f"Loaded schemas for {len(self.schema_loader.list_relationship_types())} relationship types"
    )

  def validate_node(self, node_type: str, properties: dict[str, Any]) -> bool:
    """
    Validate node properties against schema.

    Args:
        node_type: The node type (e.g., "Entity", "Element")
        properties: Dict of property names and values

    Returns:
        True if valid, raises ValueError if invalid
    """
    return self.schema_loader.validate_node_properties(node_type, properties)

  def validate_relationship(
    self,
    source_type: str,
    target_type: str,
    relationship_type: str,
    properties: dict[str, Any] | None = None,
  ) -> bool:
    """
    Validate relationship against schema.

    Args:
        source_type: Source node type
        target_type: Target node type
        relationship_type: Relationship type
        properties: Optional relationship properties

    Returns:
        True if valid, raises ValueError if invalid
    """
    return self.schema_loader.validate_relationship(
      source_type, target_type, relationship_type, properties
    )

  def get_node_schema(self, node_type: str) -> dict[str, Any] | None:
    """Get schema definition for a node type."""
    node = self.schema_loader.get_node_schema(node_type)
    if not node:
      return None

    return {
      "name": node.name,
      "description": node.description,
      "properties": self.schema_loader.get_node_properties(node_type),
      "primary_key": self.schema_loader.get_node_primary_key(node_type),
    }

  def get_relationship_schema(self, relationship_type: str) -> dict[str, Any] | None:
    """Get schema definition for a relationship type."""
    rel = self.schema_loader.get_relationship_schema(relationship_type)
    if not rel:
      return None

    return {
      "name": rel.name,
      "description": rel.description,
      "from_node": rel.from_node,
      "to_node": rel.to_node,
      "properties": {
        prop.name: {"type": prop.type, "nullable": prop.nullable}
        for prop in rel.properties
      },
    }

  def list_node_types(self) -> list[str]:
    """Get list of all defined node types."""
    return self.schema_loader.list_node_types()

  def list_relationship_types(self) -> list[str]:
    """Get list of all defined relationship types."""
    return self.schema_loader.list_relationship_types()

  def get_node_relationships(self, node_type: str) -> dict[str, list[str]]:
    """Get all possible relationships for a node type."""
    relationships = self.schema_loader.get_node_relationships(node_type)

    return {
      "outgoing": [f"{rel.name} -> {rel.to_node}" for rel in relationships["outgoing"]],
      "incoming": [
        f"{rel.from_node} -> {rel.name}" for rel in relationships["incoming"]
      ],
    }
