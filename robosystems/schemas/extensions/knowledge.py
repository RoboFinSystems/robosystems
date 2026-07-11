"""
Knowledge Schema Extension for LadybugDB

Provides a starter schema for agent-built knowledge subgraphs. It gives AI
agents a structured knowledge graph for storing concepts, observations, and
session context.

AI agents can extend this schema dynamically using the add-node-table and
add-relationship-table MCP tools.

Formerly the "memory" extension; renamed when semantic memory moved to LanceDB.
The legacy "memory" name still resolves here via an alias in the SchemaManager.
"""

from ..models import Node, Property, Relationship

EXTENSION_NODES = [
  Node(
    name="Concept",
    description="A concept, topic, or subject the AI has learned about",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="description", type="STRING"),
      Property(name="category", type="STRING"),
      Property(name="confidence", type="DOUBLE"),
      Property(name="source", type="STRING"),
      Property(name="created_at", type="STRING"),
      Property(name="updated_at", type="STRING"),
      Property(name="access_count", type="INT32"),
      Property(name="last_accessed", type="STRING"),
    ],
  ),
  Node(
    name="Observation",
    description="A specific fact, data point, or observation recorded by the AI",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="content", type="STRING"),
      Property(name="observation_type", type="STRING"),
      Property(name="context", type="STRING"),
      Property(name="confidence", type="DOUBLE"),
      Property(name="created_at", type="STRING"),
      Property(name="expires_at", type="STRING"),
    ],
  ),
  Node(
    name="Session",
    description="A conversation or task session for grouping related memories",
    properties=[
      Property(name="identifier", type="STRING", is_primary_key=True),
      Property(name="name", type="STRING"),
      Property(name="purpose", type="STRING"),
      Property(name="started_at", type="STRING"),
      Property(name="ended_at", type="STRING"),
      Property(name="status", type="STRING"),
    ],
  ),
]

EXTENSION_RELATIONSHIPS = [
  Relationship(
    name="CONCEPT_RELATES_TO",
    from_node="Concept",
    to_node="Concept",
    description="Semantic relationship between concepts",
    properties=[
      Property(name="relationship_type", type="STRING"),
      Property(name="strength", type="DOUBLE"),
      Property(name="created_at", type="STRING"),
    ],
  ),
  Relationship(
    name="OBSERVATION_ABOUT",
    from_node="Observation",
    to_node="Concept",
    description="Observation is about a concept",
    properties=[
      Property(name="relevance", type="DOUBLE"),
      Property(name="created_at", type="STRING"),
    ],
  ),
  Relationship(
    name="SESSION_PRODUCED",
    from_node="Session",
    to_node="Observation",
    description="Session produced an observation",
    properties=[
      Property(name="created_at", type="STRING"),
    ],
  ),
  Relationship(
    name="SESSION_REFERENCED",
    from_node="Session",
    to_node="Concept",
    description="Session referenced or used a concept",
    properties=[
      Property(name="usage_type", type="STRING"),
      Property(name="created_at", type="STRING"),
    ],
  ),
]
