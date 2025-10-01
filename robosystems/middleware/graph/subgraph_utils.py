"""Utility functions for subgraph parsing and validation.

This module provides utilities for working with subgraphs, including:
- Parsing subgraph IDs (using underscore notation)
- Validating subgraph names
- Converting between API IDs and database names
"""

import re
from typing import Optional, NamedTuple, Tuple
from enum import Enum


class SubgraphType(Enum):
  """Types of subgraphs (for future expansion)."""

  STATIC = "static"  # Phase 1: Traditional environment-based subgraphs
  TEMPORAL = "temporal"  # Phase 2: Short-lived memory contexts
  VERSIONED = "versioned"  # Phase 3: Git-like version control
  MEMORY = "memory"  # Phase 3: Memory layer subgraphs


class SubgraphInfo(NamedTuple):
  """Information about a parsed subgraph."""

  graph_id: str  # Full graph ID including subgraph identifier (e.g., kg123_dev)
  parent_graph_id: str  # Parent graph ID (e.g., kg123)
  subgraph_name: str  # Subgraph name (e.g., dev, staging, prod1)
  database_name: str  # Actual database name on disk (e.g., kg123_dev)
  subgraph_index: Optional[int] = None  # Numeric index if applicable


# Regex patterns for validation
# Only user graphs (kg prefix) can be parents - shared repositories CANNOT have subgraphs
PARENT_GRAPH_PATTERN = re.compile(r"^kg[a-f0-9]{16,}$")
SUBGRAPH_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9]{1,20}$")
FULL_SUBGRAPH_PATTERN = re.compile(r"^(kg[a-f0-9]{16,})_([a-zA-Z0-9]{1,20})$")


def parse_subgraph_id(graph_id: str) -> Optional[SubgraphInfo]:
  """Parse a graph ID to determine if it's a subgraph.

  Uses underscore notation for subgraphs:
  - Parent graph: kg5f2e5e0da65d45d69645
  - Subgraph: kg5f2e5e0da65d45d69645_dev

  Args:
      graph_id: The graph identifier to parse

  Returns:
      SubgraphInfo if this is a subgraph, None otherwise
  """
  match = FULL_SUBGRAPH_PATTERN.match(graph_id)
  if match:
    parent_id = match.group(1)
    subgraph_name = match.group(2)

    return SubgraphInfo(
      graph_id=graph_id,
      parent_graph_id=parent_id,
      subgraph_name=subgraph_name,
      database_name=graph_id,  # Database name is same as graph_id with underscore
    )

  return None


def validate_subgraph_name(name: str) -> bool:
  """Validate that a subgraph name is valid.

  Rules:
  - Must be alphanumeric only (letters and numbers)
  - Must be 1-20 characters long
  - No special characters allowed

  Args:
      name: The subgraph name to validate

  Returns:
      True if valid, False otherwise
  """
  return bool(SUBGRAPH_NAME_PATTERN.match(name))


def validate_parent_graph_id(graph_id: str) -> bool:
  """Validate that a graph ID can be a parent (not a subgraph).

  Args:
      graph_id: The graph ID to validate

  Returns:
      True if this can be a parent graph, False otherwise
  """
  # Check it's not already a subgraph
  if parse_subgraph_id(graph_id):
    return False

  # Check it matches valid parent patterns
  return bool(PARENT_GRAPH_PATTERN.match(graph_id))


def construct_subgraph_id(parent_graph_id: str, subgraph_name: str) -> str:
  """Construct a full subgraph ID from parent and name.

  Args:
      parent_graph_id: The parent graph ID
      subgraph_name: The subgraph name

  Returns:
      The full subgraph ID using underscore notation

  Raises:
      ValueError: If parent_graph_id or subgraph_name are invalid
  """
  if not validate_parent_graph_id(parent_graph_id):
    raise ValueError(f"Invalid parent graph ID: {parent_graph_id}")

  if not validate_subgraph_name(subgraph_name):
    raise ValueError(
      f"Invalid subgraph name: {subgraph_name}. Must be alphanumeric and 1-20 characters."
    )

  return f"{parent_graph_id}_{subgraph_name}"


def get_database_name(graph_id: str) -> str:
  """Get the actual database name for a graph ID.

  For regular graphs, this is the same as the graph ID.
  For subgraphs, this uses underscore notation.

  Args:
      graph_id: The graph identifier

  Returns:
      The database name to use on disk
  """
  # Both regular graphs and subgraphs use their ID directly as database name
  # since we're using underscore notation throughout
  return graph_id


def split_graph_hierarchy(graph_id: str) -> Tuple[str, Optional[str]]:
  """Split a graph ID into parent and subgraph components.

  Args:
      graph_id: The graph identifier

  Returns:
      Tuple of (parent_graph_id, subgraph_name or None)
  """
  subgraph_info = parse_subgraph_id(graph_id)
  if subgraph_info:
    return subgraph_info.parent_graph_id, subgraph_info.subgraph_name

  return graph_id, None


def is_subgraph(graph_id: str) -> bool:
  """Check if a graph ID represents a subgraph.

  Args:
      graph_id: The graph identifier

  Returns:
      True if this is a subgraph, False otherwise
  """
  return parse_subgraph_id(graph_id) is not None


def is_parent_graph(graph_id: str) -> bool:
  """Check if a graph ID represents a parent graph (not a subgraph).

  Args:
      graph_id: The graph identifier

  Returns:
      True if this is a parent graph, False otherwise
  """
  return validate_parent_graph_id(graph_id)


def generate_unique_subgraph_name(
  parent_graph_id: str, base_name: str, existing_names: list[str]
) -> str:
  """Generate a unique subgraph name by appending numbers if needed.

  Args:
      parent_graph_id: The parent graph ID
      base_name: The desired base name
      existing_names: List of existing subgraph names for this parent

  Returns:
      A unique subgraph name

  Raises:
      ValueError: If unable to generate a unique name
  """
  # Clean the base name to be alphanumeric
  clean_name = re.sub(r"[^a-zA-Z0-9]", "", base_name)[:17]  # Leave room for numbers

  if not clean_name:
    clean_name = "subgraph"

  # If the name is unique, use it
  if clean_name not in existing_names and validate_subgraph_name(clean_name):
    return clean_name

  # Try appending numbers
  for i in range(1, 100):
    candidate = f"{clean_name}{i}"
    if len(candidate) <= 20 and candidate not in existing_names:
      return candidate

  raise ValueError(f"Unable to generate unique subgraph name for base: {base_name}")
