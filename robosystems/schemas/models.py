"""
LadybugDB Schema Models

Defines the data structures used to represent LadybugDB schema elements.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class Property:
  """Represents a property on a node or relationship."""

  name: str
  type: str
  is_primary_key: bool = False
  nullable: bool = True

  def to_cypher(self) -> str:
    """Convert property to Cypher DDL syntax."""
    # Primary keys are defined separately in LadybugDB
    return f"{self.name} {self.type}"


@dataclass
class Node:
  """Represents a node table in LadybugDB."""

  name: str
  properties: List[Property]
  description: Optional[str] = None

  def to_cypher(self, safe_mode: bool = True) -> str:
    """
    Convert node to Cypher CREATE NODE TABLE statement.

    Args:
        safe_mode: If True, uses IF NOT EXISTS to prevent data loss.
                  If False, creates without existence check (dangerous).

    Returns:
        Cypher DDL statement for creating the node table

    NOTE: We use CREATE TABLE IF NOT EXISTS to prevent data loss.
    This means schema changes require manual migration, but protects existing data.
    For schema evolution, use dedicated migration scripts rather than DROP/CREATE.
    """
    properties_str = ",\n        ".join(prop.to_cypher() for prop in self.properties)

    # Find primary key for the table
    primary_keys = [prop.name for prop in self.properties if prop.is_primary_key]
    if not primary_keys:
      raise ValueError(f"Node {self.name} must have at least one primary key property")

    # Always include PRIMARY KEY clause when there are primary keys
    primary_key_clause = f",\n        PRIMARY KEY({', '.join(primary_keys)})"

    # Use IF NOT EXISTS by default to protect existing data
    existence_clause = "IF NOT EXISTS " if safe_mode else ""

    return f"""CREATE NODE TABLE {existence_clause}{self.name}(
        {properties_str}{primary_key_clause}
    )"""


@dataclass
class Relationship:
  """Represents a relationship table in LadybugDB."""

  name: str
  from_node: str
  to_node: str
  properties: List[Property] = field(default_factory=list)
  description: Optional[str] = None

  def to_cypher(self, safe_mode: bool = True) -> str:
    """
    Convert relationship to Cypher CREATE REL TABLE statement.

    Args:
        safe_mode: If True, uses IF NOT EXISTS to prevent data loss.
                  If False, creates without existence check (dangerous).

    Returns:
        Cypher DDL statement for creating the relationship table

    NOTE: We use CREATE REL TABLE IF NOT EXISTS to prevent data loss.
    This means schema changes require manual migration, but protects existing data.
    """
    if self.properties:
      properties_str = ",\n        " + ",\n        ".join(
        prop.to_cypher() for prop in self.properties
      )
    else:
      properties_str = ""

    # Use IF NOT EXISTS by default to protect existing data
    existence_clause = "IF NOT EXISTS " if safe_mode else ""

    return f"""CREATE REL TABLE {existence_clause}{self.name}(FROM {self.from_node} TO {self.to_node}{properties_str})"""


@dataclass
class Schema:
  """Represents a complete schema with nodes and relationships."""

  name: str
  nodes: List[Node] = field(default_factory=list)
  relationships: List[Relationship] = field(default_factory=list)
  description: Optional[str] = None
  version: str = "1.0.0"

  def to_cypher(self) -> str:
    """Convert entire schema to Cypher DDL script."""
    cypher_parts = []

    # Create node tables
    if self.nodes:
      for node in self.nodes:
        cypher_parts.append(node.to_cypher() + ";")

    # Create relationship tables
    if self.relationships:
      for relationship in self.relationships:
        cypher_parts.append(relationship.to_cypher() + ";")

    return "\n".join(cypher_parts)
