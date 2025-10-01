"""
Enhanced Schema Manager for Kuzu

Manages configurable schema definitions with inheritance and compilation.
Supports base schema + extensions architecture.
"""

import importlib
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

from .models import Schema
from robosystems.logger import logger


class SchemaType(Enum):
  """Available schema types."""

  BASE = "base"
  ROBOLEDGER = "roboledger"
  ROBOINVESTOR = "roboinvestor"
  ROBOSCM = "roboscm"
  ROBOFO = "robofo"
  ROBOREPORT = "roboreport"
  ROBOHRM = "robohrm"
  ROBOEPM = "roboepm"


@dataclass
class SchemaConfiguration:
  """Schema configuration with base + extensions."""

  name: str
  description: str
  version: str
  base_schema: str  # "base"
  extensions: List[str]  # ["roboledger", "roboinvestor"]


@dataclass
class SchemaCompatibility:
  """Schema compatibility information."""

  compatible: bool
  conflicts: List[str]
  shared_nodes: List[str]
  shared_relationships: List[str]


class SchemaManager:
  """
  Enhanced schema manager with base + extension support.

  Provides schema loading, inheritance, compilation, and compatibility checking.
  """

  def __init__(self):
    self._schema_cache: Dict[str, Schema] = {}
    self._module_cache: Dict[str, Any] = {}
    self._compatibility_cache: Dict[Tuple[str, ...], SchemaCompatibility] = {}

  def create_schema_configuration(
    self,
    name: str,
    description: str,
    version: str = "1.0.0",
    extensions: Optional[List[str]] = None,
  ) -> SchemaConfiguration:
    """Create a schema configuration."""
    return SchemaConfiguration(
      name=name,
      description=description,
      version=version,
      base_schema="base",
      extensions=extensions or [],
    )

  def load_and_compile_schema(self, config: SchemaConfiguration) -> Schema:
    """
    Load and compile a complete schema from configuration.

    Args:
        config: Schema configuration with base + extensions

    Returns:
        Complete compiled schema
    """
    cache_key = f"{config.base_schema}+{'+'.join(sorted(config.extensions))}"

    if cache_key in self._schema_cache:
      logger.debug(f"Using cached schema: {cache_key}")
      return self._schema_cache[cache_key]

    logger.info(f"Compiling schema: {config.name}")
    logger.debug(f"Base: {config.base_schema}, Extensions: {config.extensions}")

    # Create base schema
    schema = Schema(
      name=config.name, description=config.description, version=config.version
    )

    # Load base schema
    base_module = self._load_schema_module(config.base_schema)
    if hasattr(base_module, "BASE_NODES"):
      schema.nodes.extend(base_module.BASE_NODES)
      logger.debug(f"Loaded {len(base_module.BASE_NODES)} base nodes")

    if hasattr(base_module, "BASE_RELATIONSHIPS"):
      schema.relationships.extend(base_module.BASE_RELATIONSHIPS)
      logger.debug(f"Loaded {len(base_module.BASE_RELATIONSHIPS)} base relationships")

    # Load extensions
    for extension_name in config.extensions:
      extension_module = self._load_extension_module(extension_name)

      if hasattr(extension_module, "EXTENSION_NODES"):
        schema.nodes.extend(extension_module.EXTENSION_NODES)
        logger.debug(
          f"Loaded {len(extension_module.EXTENSION_NODES)} nodes from {extension_name}"
        )

      if hasattr(extension_module, "EXTENSION_RELATIONSHIPS"):
        schema.relationships.extend(extension_module.EXTENSION_RELATIONSHIPS)
        logger.debug(
          f"Loaded {len(extension_module.EXTENSION_RELATIONSHIPS)} relationships from {extension_name}"
        )

    # Validate schema consistency
    self._validate_schema_consistency(schema)

    # Cache compiled schema
    self._schema_cache[cache_key] = schema

    logger.info(
      f"Compiled schema '{config.name}' with {len(schema.nodes)} nodes and {len(schema.relationships)} relationships"
    )
    return schema

  def _load_schema_module(self, schema_name: str):
    """Load base schema module."""
    module_path = f"robosystems.schemas.{schema_name}"
    return self._load_module(module_path, schema_name)

  def _load_extension_module(self, extension_name: str):
    """Load extension schema module."""
    module_path = f"robosystems.schemas.extensions.{extension_name}"
    return self._load_module(module_path, extension_name)

  def _load_module(self, module_path: str, cache_key: str):
    """Load and cache Python module."""
    if cache_key in self._module_cache:
      return self._module_cache[cache_key]

    try:
      module = importlib.import_module(module_path)
      self._module_cache[cache_key] = module
      logger.debug(f"Loaded module: {module_path}")
      return module
    except ImportError as e:
      logger.error(f"Failed to load schema module '{module_path}': {e}")
      raise ValueError(f"Schema module '{cache_key}' not found")

  def _validate_schema_consistency(self, schema: Schema):
    """Validate schema for consistency and conflicts."""
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
        logger.warning(
          f"Relationship {rel.name} references unknown from_node: {rel.from_node}"
        )
      if rel.to_node != "*" and rel.to_node not in node_name_set:
        logger.warning(
          f"Relationship {rel.name} references unknown to_node: {rel.to_node}"
        )

    logger.debug("Schema consistency validation passed")

  def check_schema_compatibility(self, extensions: List[str]) -> SchemaCompatibility:
    """
    Check compatibility between multiple schema extensions.

    Args:
        extensions: List of extension names to check

    Returns:
        Compatibility information
    """
    cache_key = tuple(sorted(extensions))

    if cache_key in self._compatibility_cache:
      return self._compatibility_cache[cache_key]

    logger.debug(f"Checking compatibility for extensions: {extensions}")

    all_nodes: Dict[str, str] = {}  # name -> extension
    all_relationships: Dict[str, str] = {}  # name -> extension
    conflicts = []

    # Collect all nodes and relationships from extensions
    for extension_name in extensions:
      try:
        extension_module = self._load_extension_module(extension_name)

        # Check nodes
        if hasattr(extension_module, "EXTENSION_NODES"):
          for node in extension_module.EXTENSION_NODES:
            if node.name in all_nodes:
              conflicts.append(
                f"Node '{node.name}' defined in both {all_nodes[node.name]} and {extension_name}"
              )
            else:
              all_nodes[node.name] = extension_name

        # Check relationships
        if hasattr(extension_module, "EXTENSION_RELATIONSHIPS"):
          for rel in extension_module.EXTENSION_RELATIONSHIPS:
            if rel.name in all_relationships:
              conflicts.append(
                f"Relationship '{rel.name}' defined in both {all_relationships[rel.name]} and {extension_name}"
              )
            else:
              all_relationships[rel.name] = extension_name

      except Exception as e:
        logger.error(f"Error checking compatibility for {extension_name}: {e}")
        conflicts.append(f"Failed to load extension {extension_name}: {e}")

    # Find shared nodes (nodes used by multiple extensions)
    shared_nodes = []
    shared_relationships = []

    # This would require more sophisticated analysis of relationship dependencies
    # For now, we'll mark as compatible if no naming conflicts

    compatibility = SchemaCompatibility(
      compatible=len(conflicts) == 0,
      conflicts=conflicts,
      shared_nodes=shared_nodes,
      shared_relationships=shared_relationships,
    )

    self._compatibility_cache[cache_key] = compatibility
    return compatibility

  def get_optimal_schema_groups(self) -> Dict[str, List[str]]:
    """
    Get predefined optimal schema groupings for instance placement.

    Returns:
        Dictionary of group name to list of compatible extensions
    """
    return {
      "financial_core": ["roboledger", "roboepm", "roboreport"],
      "operations_hub": ["roboscm", "robohrm", "roboepm"],
      "sales_engine": ["robofo", "robohrm", "roboinvestor"],
      "compliance_center": ["roboreport", "roboledger", "robohrm"],
      "standalone_investment": ["roboinvestor"],
      "standalone_scm": ["roboscm"],
      "standalone_fo": ["robofo"],
    }

  def generate_cypher_ddl(self, schema: Schema) -> str:
    """
    Generate complete Cypher DDL for schema.

    Args:
        schema: Compiled schema object

    Returns:
        Complete DDL script
    """
    return schema.to_cypher()

  def get_schema_statistics(self, schema: Schema) -> Dict[str, Any]:
    """
    Get statistics about a compiled schema.

    Args:
        schema: Compiled schema object

    Returns:
        Schema statistics
    """
    return {
      "name": schema.name,
      "version": schema.version,
      "total_nodes": len(schema.nodes),
      "total_relationships": len(schema.relationships),
      "node_names": [node.name for node in schema.nodes],
      "relationship_names": [rel.name for rel in schema.relationships],
      "estimated_ddl_size": len(schema.to_cypher()),
    }

  def list_available_extensions(self) -> List[Dict[str, str]]:
    """
    List all available schema extensions.

    Returns:
        List of extension information
    """
    extensions = []

    for schema_type in SchemaType:
      if schema_type == SchemaType.BASE:
        continue

      try:
        module = self._load_extension_module(schema_type.value)
        doc = getattr(module, "__doc__", "No description available").strip()

        # Extract description from docstring
        description = (
          doc.split("\n")[0] if doc else f"{schema_type.value} schema extension"
        )

        extensions.append(
          {
            "name": schema_type.value,
            "type": schema_type.name,
            "description": description,
            "available": True,
          }
        )
      except Exception as e:
        extensions.append(
          {
            "name": schema_type.value,
            "type": schema_type.name,
            "description": f"Failed to load schema: {str(e)}",
            "available": False,
          }
        )

    return extensions

  def clear_cache(self):
    """Clear all cached schemas and modules."""
    self._schema_cache.clear()
    self._module_cache.clear()
    self._compatibility_cache.clear()
    logger.info("Schema manager cache cleared")


# Convenience functions for common operations
def create_roboledger_schema() -> Schema:
  """Create RoboLedger schema (base + roboledger extension)."""
  manager = SchemaManager()
  config = manager.create_schema_configuration(
    name="RoboLedger Financial Reporting Schema",
    description="Complete financial reporting and XBRL processing schema",
    extensions=["roboledger"],
  )
  return manager.load_and_compile_schema(config)


def create_accounting_schema() -> Schema:
  """Create complete accounting schema (base + roboledger extension)."""
  manager = SchemaManager()
  config = manager.create_schema_configuration(
    name="Complete Accounting Schema",
    description="Financial reporting (XBRL) and general ledger schema",
    extensions=["roboledger"],
  )
  return manager.load_and_compile_schema(config)


def create_multi_app_schema(extensions: List[str]) -> Schema:
  """Create multi-application schema with specified extensions."""
  manager = SchemaManager()

  # Check compatibility first
  compatibility = manager.check_schema_compatibility(extensions)
  if not compatibility.compatible:
    raise ValueError(f"Schema extensions are not compatible: {compatibility.conflicts}")

  config = manager.create_schema_configuration(
    name=f"Multi-Application Schema ({', '.join(extensions)})",
    description=f"Combined schema with {', '.join(extensions)} applications",
    extensions=extensions,
  )
  return manager.load_and_compile_schema(config)


def get_recommended_schema_groups() -> Dict[str, List[str]]:
  """Get recommended schema groupings for optimal performance."""
  manager = SchemaManager()
  return manager.get_optimal_schema_groups()
