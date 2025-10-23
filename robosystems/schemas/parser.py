import re
from typing import List, Dict, Any
from dataclasses import dataclass


@dataclass
class NodeType:
  name: str
  properties: List[Dict[str, str]]

  def to_dict(self) -> Dict[str, Any]:
    return {
      "name": self.name,
      "properties": self.properties,
    }


def parse_cypher_schema(ddl: str) -> List[NodeType]:
  """
  Parse Cypher DDL to extract node types and their properties.

  Handles CREATE NODE TABLE statements from Kuzu schema.

  Example DDL:
      CREATE NODE TABLE Customer(name STRING, sector STRING, PRIMARY KEY(name));
      CREATE NODE TABLE Order(id INT64, amount DOUBLE, PRIMARY KEY(id));

  Returns:
      List of NodeType objects with name and properties

  Raises:
      ValueError: If DDL is malformed or cannot be parsed
  """
  if not ddl or not ddl.strip():
    raise ValueError("Schema DDL cannot be empty")

  node_types = []

  # Pattern to match CREATE NODE TABLE statements
  # Matches: CREATE NODE TABLE NodeName(prop1 TYPE1, prop2 TYPE2, ...)
  pattern = r"CREATE\s+NODE\s+TABLE\s+(\w+)\s*\((.*?)\);"

  try:
    matches = re.finditer(pattern, ddl, re.IGNORECASE | re.DOTALL)
  except re.error as e:
    raise ValueError(f"Invalid regex pattern in DDL parsing: {str(e)}") from e

  for match in matches:
    try:
      node_name = match.group(1)
      properties_str = match.group(2)

      if not node_name:
        raise ValueError("Node type name cannot be empty")

      properties = []

      # Split by comma, but handle nested parentheses (for PRIMARY KEY)
      prop_parts = []
      current_part = ""
      paren_depth = 0

      for char in properties_str:
        if char == "(":
          paren_depth += 1
        elif char == ")":
          paren_depth -= 1
          if paren_depth < 0:
            raise ValueError(f"Unbalanced parentheses in node {node_name}")
        elif char == "," and paren_depth == 0:
          prop_parts.append(current_part.strip())
          current_part = ""
          continue
        current_part += char

      if paren_depth != 0:
        raise ValueError(f"Unclosed parentheses in node {node_name}")

      if current_part.strip():
        prop_parts.append(current_part.strip())

      # Parse each property
      for prop_part in prop_parts:
        prop_part = prop_part.strip()

        # Skip PRIMARY KEY and other constraints
        if prop_part.upper().startswith("PRIMARY KEY"):
          continue
        if prop_part.upper().startswith("UNIQUE"):
          continue
        if prop_part.upper().startswith("FOREIGN KEY"):
          continue

        # Parse property definition: name TYPE [constraints]
        prop_match = re.match(r"(\w+)\s+(\w+(?:\[\])?)", prop_part, re.IGNORECASE)
        if prop_match:
          prop_name = prop_match.group(1)
          prop_type = prop_match.group(2)

          properties.append(
            {
              "name": prop_name,
              "type": prop_type,
            }
          )

      if properties:
        node_types.append(NodeType(name=node_name, properties=properties))
    except (IndexError, AttributeError, ValueError) as e:
      raise ValueError(f"Failed to parse node type definition: {str(e)}") from e

  return node_types


def parse_relationship_types(ddl: str) -> List[str]:
  """
  Parse Cypher DDL to extract relationship types.

  Example DDL:
      CREATE REL TABLE HAS_ORDER(FROM Customer TO Order);

  Returns:
      List of relationship type names
  """
  rel_types = []

  # Pattern to match CREATE REL TABLE statements
  pattern = r"CREATE\s+REL\s+TABLE\s+(\w+)\s*\("

  matches = re.finditer(pattern, ddl, re.IGNORECASE)

  for match in matches:
    rel_name = match.group(1)
    rel_types.append(rel_name)

  return rel_types
