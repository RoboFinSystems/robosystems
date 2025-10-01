"""
Kuzu Schema Loader

Loads schema definitions from robosystems.schemas and provides
utilities for creating Kuzu models from schema definitions.

Supports context-aware loading for unified schemas like RoboLedger.
"""

import logging
import importlib
import pkgutil
from typing import Dict, Any, List, Optional

from robosystems.schemas.base import BASE_NODES, BASE_RELATIONSHIPS
from robosystems.schemas.models import Node, Relationship
import robosystems.schemas.extensions as extensions_pkg

logger = logging.getLogger(__name__)


class KuzuSchemaLoader:
  """Loads and manages Kuzu schema definitions with selective extension loading."""

  def __init__(self, extensions: Optional[List[str]] = None):
    """
    Initialize schema loader with optional extension filtering.

    Args:
        extensions: List of extension names to load. If None, loads all available extensions.
                   Example: ['roboledger'] for SEC repository, ['roboledger', 'roboinvestor'] for multi-product

    Note: Schema loading is already optimized to only load requested extensions.
    For XBRL processing, schemas are loaded on-demand based on the repository context.
    Further lazy loading could be implemented at the field level if needed.
    """
    # Start with base schemas
    all_nodes = list(BASE_NODES)
    all_relationships = list(BASE_RELATIONSHIPS)

    # Determine which extensions to load
    if extensions is None:
      # Load all available extensions (backward compatibility)
      target_extensions = self._discover_all_extensions()
      logger.info("Loading all available extensions (no filter specified)")
    else:
      # Load only specified extensions
      target_extensions = extensions
      logger.info(f"Loading selective extensions: {target_extensions}")

    # Load the specified extensions
    loaded_extensions = []

    for extension_name in target_extensions:
      try:
        # Import the extension module
        extension_module = importlib.import_module(
          f"robosystems.schemas.extensions.{extension_name}"
        )

        # Check if module has EXTENSION_NODES and EXTENSION_RELATIONSHIPS
        if hasattr(extension_module, "EXTENSION_NODES") and hasattr(
          extension_module, "EXTENSION_RELATIONSHIPS"
        ):
          extension_nodes = getattr(extension_module, "EXTENSION_NODES")
          extension_relationships = getattr(extension_module, "EXTENSION_RELATIONSHIPS")

          # Add to our collections
          all_nodes.extend(extension_nodes)
          all_relationships.extend(extension_relationships)
          loaded_extensions.append(extension_name)

          logger.debug(
            f"Loaded extension '{extension_name}': {len(extension_nodes)} nodes, {len(extension_relationships)} relationships"
          )
        else:
          logger.warning(
            f"Extension '{extension_name}' missing EXTENSION_NODES or EXTENSION_RELATIONSHIPS"
          )

      except ImportError:
        logger.error(f"Extension '{extension_name}' not found")
        continue
      except Exception as e:
        logger.warning(f"Failed to load extension '{extension_name}': {e}")
        continue

    # Fallback if no extensions loaded successfully
    if not loaded_extensions and extensions is not None:
      # DISABLED: Fallback causing schema corruption in Docker
      # Always honor the user's extension selection, even if empty
      logger.info(
        f"No extensions loaded (intentional). Using base schema only: {len(all_nodes)} nodes, {len(all_relationships)} relationships"
      )

    # Create lookup dictionaries
    self.nodes = {node.name: node for node in all_nodes}
    self.relationships = {rel.name: rel for rel in all_relationships}
    self.loaded_extensions = loaded_extensions

    logger.info(
      f"Schema loader initialized: {len(self.nodes)} node types, {len(self.relationships)} relationship types"
    )
    logger.info(
      f"Loaded extensions: {', '.join(loaded_extensions) if loaded_extensions else 'none'}"
    )
    logger.debug(f"Available node types: {sorted(self.nodes.keys())}")
    logger.debug(f"Available relationship types: {sorted(self.relationships.keys())}")

  def _discover_all_extensions(self) -> List[str]:
    """Discover all available extensions for backward compatibility."""
    available_extensions = []

    try:
      # Get the extensions package path
      extensions_path = extensions_pkg.__path__

      # Iterate through all modules in the extensions package
      for importer, modname, ispkg in pkgutil.iter_modules(extensions_path):
        if modname.startswith("__"):
          continue  # Skip __init__ and other special modules

        try:
          # Check if module has the required attributes without importing
          extension_module = importlib.import_module(
            f"robosystems.schemas.extensions.{modname}"
          )
          if hasattr(extension_module, "EXTENSION_NODES") and hasattr(
            extension_module, "EXTENSION_RELATIONSHIPS"
          ):
            available_extensions.append(modname)
        except Exception:
          continue

    except Exception as e:
      logger.error(f"Failed to discover extensions: {e}")
      # Return minimal fallback
      return ["roboledger"]

    return available_extensions

  def get_node_schema(self, node_name: str) -> Optional[Node]:
    """Get schema definition for a node type."""
    return self.nodes.get(node_name)

  def get_relationship_schema(self, relationship_name: str) -> Optional[Relationship]:
    """Get schema definition for a relationship type."""
    return self.relationships.get(relationship_name)

  def list_node_types(self) -> List[str]:
    """Get list of all defined node types."""
    return list(self.nodes.keys())

  def list_relationship_types(self) -> List[str]:
    """Get list of all defined relationship types."""
    return list(self.relationships.keys())

  def get_node_properties(self, node_name: str) -> Dict[str, Dict[str, Any]]:
    """
    Get properties for a node type in a format suitable for Kuzu models.

    Returns:
        Dict mapping property names to property metadata
    """
    node = self.get_node_schema(node_name)
    if not node:
      return {}

    properties = {}
    for prop in node.properties:
      properties[prop.name] = {
        "type": prop.type,
        "is_primary_key": prop.is_primary_key,
        "nullable": prop.nullable,
        "required": not prop.nullable,
        "index": prop.is_primary_key,  # Primary keys are indexed by default
      }

    return properties

  def get_node_primary_key(self, node_name: str) -> Optional[str]:
    """Get the primary key field name for a node type."""
    node = self.get_node_schema(node_name)
    if not node:
      return None

    for prop in node.properties:
      if prop.is_primary_key:
        return prop.name

    return None

  def validate_node_properties(
    self, node_name: str, properties: Dict[str, Any]
  ) -> bool:
    """
    Validate node properties against schema.

    Args:
        node_name: The node type name
        properties: Dict of property names and values

    Returns:
        True if valid, raises ValueError if invalid
    """
    node_schema = self.get_node_schema(node_name)
    if not node_schema:
      raise ValueError(f"Node type '{node_name}' not defined in schema")

    schema_props = {prop.name: prop for prop in node_schema.properties}

    # Check required properties (non-nullable)
    for prop in node_schema.properties:
      if not prop.nullable and prop.name not in properties:
        raise ValueError(f"Required property '{prop.name}' missing for {node_name}")

    # Check property types
    for prop_name, prop_value in properties.items():
      if prop_name in schema_props:
        expected_type = schema_props[prop_name].type
        if not self._check_type_compatibility(prop_value, expected_type):
          raise ValueError(
            f"Property '{prop_name}' has invalid type. "
            f"Expected {expected_type}, got {type(prop_value).__name__}"
          )

    return True

  def validate_relationship(
    self,
    from_node: str,
    to_node: str,
    relationship_name: str,
    properties: Optional[Dict[str, Any]] = None,
  ) -> bool:
    """
    Validate relationship against schema.

    Args:
        from_node: Source node type
        to_node: Target node type
        relationship_name: Relationship type name
        properties: Optional relationship properties

    Returns:
        True if valid, raises ValueError if invalid
    """
    rel_schema = self.get_relationship_schema(relationship_name)
    if not rel_schema:
      raise ValueError(f"Relationship type '{relationship_name}' not defined in schema")

    # Check if from/to nodes match schema
    if rel_schema.from_node != from_node:
      raise ValueError(
        f"Relationship {relationship_name} expects from_node='{rel_schema.from_node}', "
        f"got '{from_node}'"
      )

    if rel_schema.to_node != to_node:
      raise ValueError(
        f"Relationship {relationship_name} expects to_node='{rel_schema.to_node}', "
        f"got '{to_node}'"
      )

    # Validate relationship properties if any
    if properties and rel_schema.properties:
      schema_props = {prop.name: prop for prop in rel_schema.properties}
      for prop_name, prop_value in properties.items():
        if prop_name in schema_props:
          expected_type = schema_props[prop_name].type
          if not self._check_type_compatibility(prop_value, expected_type):
            raise ValueError(
              f"Relationship property '{prop_name}' has invalid type. "
              f"Expected {expected_type}, got {type(prop_value).__name__}"
            )

    return True

  def _check_type_compatibility(self, value: Any, expected_type: str) -> bool:
    """Check if value is compatible with expected Kuzu type."""
    if value is None:
      return True  # NULL values are generally allowed

    type_mapping = {
      "STRING": (str,),
      "INT64": (int,),
      "DOUBLE": (float, int),  # int can be cast to double
      "BOOLEAN": (bool,),
      "TIMESTAMP": (str,),  # Assuming timestamp as ISO string
      "DATE": (str,),
      "BLOB": (bytes, str),
    }

    allowed_types = type_mapping.get(expected_type, (str,))
    return isinstance(value, allowed_types)

  def get_node_relationships(self, node_name: str) -> Dict[str, List[Relationship]]:
    """
    Get all relationships for a node type.

    Returns:
        Dict with 'outgoing' and 'incoming' lists of relationships
    """
    outgoing = []
    incoming = []

    for rel in self.relationships.values():
      if rel.from_node == node_name:
        outgoing.append(rel)
      elif rel.to_node == node_name:
        incoming.append(rel)

    return {"outgoing": outgoing, "incoming": incoming}


# Global schema loader instances (cache by extension configuration)
_schema_loader_cache: Dict[str, KuzuSchemaLoader] = {}
_default_schema_loader = None


def get_schema_loader(extensions: Optional[List[str]] = None) -> KuzuSchemaLoader:
  """
  Get a schema loader instance with specified extensions.

  Args:
      extensions: List of extension names to load. If None, loads all available extensions.
                 Example: ['roboledger'] for SEC repository

  Returns:
      Schema loader instance configured with the specified extensions
  """
  global _default_schema_loader, _schema_loader_cache

  # If no extensions specified, use the default global instance
  if extensions is None:
    if _default_schema_loader is None:
      _default_schema_loader = KuzuSchemaLoader()
    return _default_schema_loader

  # Create cache key from sorted extensions
  cache_key = "+".join(sorted(extensions))

  # Return cached instance if available
  if cache_key in _schema_loader_cache:
    return _schema_loader_cache[cache_key]

  # Create new instance with specified extensions
  loader = KuzuSchemaLoader(extensions=extensions)
  _schema_loader_cache[cache_key] = loader

  return loader


def get_sec_schema_loader() -> KuzuSchemaLoader:
  """
  Get schema loader configured for SEC repository.

  Uses context-aware loading to only include XBRL/reporting tables,
  not transaction-level GL tables which would confuse MCP agents.
  """
  return get_contextual_schema_loader("repository", "sec")


def get_entity_schema_loader(entity_type: Optional[str] = None) -> KuzuSchemaLoader:
  """
  Get schema loader configured for entity databases based on entity type.

  Args:
      entity_type: Type of entity to determine which extensions to load.
                   If None, loads all extensions for maximum compatibility.

  Returns:
      Schema loader with appropriate extensions for the entity type
  """
  if entity_type == "financial_services":
    return get_schema_loader(extensions=["roboledger", "roboinvestor"])
  elif entity_type == "manufacturing":
    return get_schema_loader(extensions=["roboledger", "roboscm", "roboepm"])
  elif entity_type == "tech_startup":
    return get_schema_loader(extensions=["roboledger", "robohrm"])
  else:
    # Default: load all extensions
    return get_schema_loader()


def get_contextual_schema_loader(
  context_type: str,
  context_name: str,
  additional_extensions: Optional[List[str]] = None,
) -> KuzuSchemaLoader:
  """
  Get schema loader based on context (repository, application, custom).

  Supports context-aware loading for unified schemas like RoboLedger that
  need different table subsets based on use case.

  Args:
      context_type: Type of context ('repository', 'application', 'custom')
      context_name: Name of the specific context ('sec', 'roboledger', etc.)
      additional_extensions: Extra extensions to include beyond defaults

  Returns:
      KuzuSchemaLoader configured for the context
  """
  # Special handling for RoboLedger unified schema contexts
  if context_type == "repository" and context_name == "sec":
    # SEC repository needs reporting-only view of RoboLedger
    loader = ContextAwareSchemaLoader(extension="roboledger", context="sec_repository")
    return loader
  elif context_type == "application" and context_name == "roboledger":
    # RoboLedger app needs full accounting capabilities
    loader = ContextAwareSchemaLoader(extension="roboledger", context="full_accounting")
    return loader
  else:
    # For other contexts, use standard extension loading
    extensions = []

    # Map context to extensions
    if context_type == "application":
      if context_name == "roboinvestor":
        extensions = ["roboinvestor"]
      elif context_name == "robosystems":
        extensions = []  # Base schema only for admin

    # Add any additional requested extensions
    if additional_extensions:
      extensions.extend(additional_extensions)

    return get_schema_loader(extensions=extensions if extensions else None)


class ContextAwareSchemaLoader(KuzuSchemaLoader):
  """
  Schema loader that supports context-aware loading for unified schemas.

  This is used for schemas like RoboLedger that contain multiple logical
  sections (transactions + reporting) but need to expose different subsets
  based on the use case.
  """

  def __init__(self, extension: str, context: str):
    """
    Initialize context-aware loader.

    Args:
        extension: Name of the unified extension (e.g., 'roboledger_unified')
        context: Context for loading (e.g., 'sec_repository', 'full_accounting')
    """
    # For SEC repository, filter out base nodes and relationships that aren't populated
    if context == "sec_repository":
      # These base nodes are not populated by SEC data
      excluded_base_nodes = {
        "User",  # No user data in SEC
        "GraphMetadata",  # Metadata is managed separately
        "Connection",  # No connection data in SEC
      }
      all_nodes = [node for node in BASE_NODES if node.name not in excluded_base_nodes]

      # These base relationships are not populated by SEC data
      excluded_base_rels = {
        "USER_HAS_ACCESS",
        "ENTITY_HAS_CONNECTION",
        "ENTITY_EVOLVED_FROM",
        "ENTITY_OWNS_ENTITY",
        "ELEMENT_IN_TAXONOMY",
        "STRUCTURE_HAS_CHILD",
        "STRUCTURE_HAS_PARENT",
      }
      all_relationships = [
        rel for rel in BASE_RELATIONSHIPS if rel.name not in excluded_base_rels
      ]
    else:
      # For other contexts, use all base schemas
      all_nodes = list(BASE_NODES)
      all_relationships = list(BASE_RELATIONSHIPS)

    try:
      # Import the unified extension module
      extension_module = importlib.import_module(
        f"robosystems.schemas.extensions.{extension}"
      )

      # Check if it has context-aware support
      if hasattr(extension_module, "RoboLedgerContext"):
        context_class = getattr(extension_module, "RoboLedgerContext")

        # Get nodes and relationships for the specific context
        context_nodes = context_class.get_nodes_for_context(context)
        context_relationships = context_class.get_relationships_for_context(context)

        all_nodes.extend(context_nodes)
        all_relationships.extend(context_relationships)

        logger.debug(
          f"Loaded context-aware extension '{extension}' with context '{context}': "
          f"{len(context_nodes)} nodes, {len(context_relationships)} relationships"
        )
      else:
        # Fall back to standard loading if not context-aware
        logger.warning(
          f"Extension '{extension}' does not support context-aware loading"
        )
        super().__init__(extensions=[extension])
        return

    except ImportError as e:
      logger.error(f"Failed to import extension '{extension}': {e}")
      # Fall back to base schema only
    except Exception as e:
      logger.error(f"Error loading context-aware extension '{extension}': {e}")
      # Fall back to base schema only

    # Create lookup dictionaries
    self.nodes = {node.name: node for node in all_nodes}
    self.relationships = {rel.name: rel for rel in all_relationships}
    self.loaded_extensions = [f"{extension}[{context}]"]
    self.context = context
    self.extension = extension

    logger.debug(
      f"Context-aware schema loader initialized: {len(self.nodes)} node types, "
      f"{len(self.relationships)} relationship types for {extension}[{context}]"
    )
