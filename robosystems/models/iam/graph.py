"""Graph model for storing user-owned graph metadata.

This model tracks metadata for user-created graphs (entity and generic graphs).
It does NOT include shared repositories (SEC, industry, economic) which have
a different access and billing model managed through UserRepository.
"""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Sequence

from sqlalchemy import (
  Column,
  String,
  DateTime,
  Index,
  CheckConstraint,
  Boolean,
  Integer,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship, Session

from ...database import Model
from .graph_credits import GraphTier


class Graph(Model):
  """Graph model for managing graph database metadata."""

  __tablename__ = "graphs"
  __table_args__ = (
    Index("idx_graphs_type", "graph_type"),
    Index("idx_graphs_instance", "graph_instance_id"),
    Index("idx_graphs_schema_extensions", "schema_extensions", postgresql_using="gin"),
    Index("idx_graphs_tier", "graph_tier"),
    Index("idx_graphs_parent", "parent_graph_id"),
    Index("idx_graphs_is_subgraph", "is_subgraph"),
    CheckConstraint("graph_type IN ('generic', 'entity')", name="check_graph_type"),
    UniqueConstraint("parent_graph_id", "subgraph_index", name="unique_subgraph_index"),
    CheckConstraint(
      "(is_subgraph = false AND parent_graph_id IS NULL AND subgraph_index IS NULL AND subgraph_name IS NULL) OR "
      "(is_subgraph = true AND parent_graph_id IS NOT NULL AND subgraph_index IS NOT NULL AND subgraph_name IS NOT NULL)",
      name="check_subgraph_consistency",
    ),
  )

  # Primary identifier - matches the Kuzu database name
  graph_id = Column(
    String, primary_key=True
  )  # e.g., "kg1a2b3c4d5", "sec", "generic_123"

  # Basic metadata
  graph_name = Column(String, nullable=False)  # Human-readable name
  graph_type = Column(String, nullable=False)  # "generic" or "entity"

  # Schema information
  base_schema = Column(
    String, nullable=True
  )  # "base" for entity graphs, null for generic
  schema_extensions = Column(
    JSONB, nullable=False, default=list
  )  # ["roboledger", "roboinvestor"] for entity graphs

  # Infrastructure metadata
  graph_instance_id = Column(
    String, nullable=False, default="default", index=True
  )  # Cluster/instance identifier
  graph_cluster_region = Column(String, nullable=True)  # Geographic region for cluster

  # Credit system integration
  graph_tier = Column(
    String, nullable=False, default=GraphTier.KUZU_STANDARD.value
  )  # kuzu-standard, kuzu-large, kuzu-xlarge, etc. (infrastructure tier)

  # Subgraph support (Enterprise/Premium only)
  parent_graph_id = Column(
    String, nullable=True, index=True
  )  # Parent graph ID if this is a subgraph
  subgraph_index = Column(
    Integer, nullable=True
  )  # Numeric index (1, 2, 3, ...) for subgraphs
  subgraph_name = Column(
    String, nullable=True
  )  # Custom alphanumeric name (max 20 chars, alphanumeric only)
  is_subgraph = Column(
    Boolean, default=False, nullable=False
  )  # True if this is a subgraph
  subgraph_metadata = Column(
    JSONB, nullable=True
  )  # Additional subgraph-specific metadata (TTL, type, etc.)

  # Timestamps
  created_at = Column(
    DateTime, default=lambda: datetime.now(timezone.utc), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
    nullable=False,
  )

  # Additional metadata that might be useful
  graph_metadata = Column(JSONB, nullable=True)  # Flexible field for future use

  # Relationships
  user_graphs = relationship(
    "UserGraph", back_populates="graph", cascade="all, delete-orphan"
  )

  def __repr__(self) -> str:
    """String representation of the graph."""
    if bool(self.is_subgraph):
      return f"<Graph {str(self.graph_id)} (subgraph of {str(self.parent_graph_id)}) type={str(self.graph_type)}>"
    return f"<Graph {str(self.graph_id)} type={str(self.graph_type)} extensions={str(self.schema_extensions)}>"

  @property
  def has_extension(self) -> bool:
    """Check if this graph has any schema extensions."""
    extensions = self.schema_extensions
    if extensions is None:
      return False
    # At runtime, extensions is a list; type checker doesn't know this
    return len(extensions) > 0

  @property
  def database_name(self) -> str:
    """Get the actual database name on disk (using underscore notation)."""
    if (
      bool(self.is_subgraph)
      and self.parent_graph_id is not None
      and self.subgraph_name is not None
    ):
      # Subgraph uses parent_id_subgraphname format
      return f"{str(self.parent_graph_id)}_{str(self.subgraph_name)}"
    # Regular graph uses its ID directly
    return str(self.graph_id)

  @property
  def can_have_subgraphs(self) -> bool:
    """Check if this graph tier supports subgraphs."""
    return str(self.graph_tier) in [
      GraphTier.KUZU_LARGE.value,
      GraphTier.KUZU_XLARGE.value,
      GraphTier.NEO4J_ENTERPRISE_XLARGE.value,
    ]

  def has_specific_extension(self, extension: str) -> bool:
    """Check if this graph has a specific schema extension."""
    extensions = self.schema_extensions or []
    return extension in extensions

  def get_credit_multiplier(self) -> float:
    """
    Get the credit multiplier for this graph's tier.

    In the simplified credit system, all tiers use 1.0x multiplier.
    Credits are consumed based on actual token usage post-operation.
    """
    return 1.0

  @classmethod
  def create(
    cls,
    graph_id: str,
    graph_name: str,
    graph_type: str,
    session: Session,
    base_schema: Optional[str] = None,
    schema_extensions: Optional[List[str]] = None,
    graph_instance_id: str = "default",
    graph_cluster_region: Optional[str] = None,
    graph_tier: GraphTier = GraphTier.KUZU_STANDARD,
    graph_metadata: Optional[Dict[str, Any]] = None,
    parent_graph_id: Optional[str] = None,
    subgraph_index: Optional[int] = None,
    subgraph_name: Optional[str] = None,
    is_subgraph: bool = False,
    subgraph_metadata: Optional[Dict[str, Any]] = None,
  ) -> "Graph":
    """Create a new graph metadata entry."""
    # Validate graph_type
    if graph_type not in ["generic", "entity"]:
      raise ValueError("graph_type must be either 'generic' or 'entity'")

    # Entity graphs should have base_schema
    if graph_type == "entity" and not base_schema:
      base_schema = "base"

    # Validate subgraph parameters
    if is_subgraph:
      if not parent_graph_id or subgraph_index is None or not subgraph_name:
        raise ValueError(
          "Subgraphs require parent_graph_id, subgraph_index, and subgraph_name"
        )

      # Validate subgraph_name (alphanumeric only, max 20 chars)
      import re

      if not re.match(r"^[a-zA-Z0-9]{1,20}$", subgraph_name):
        raise ValueError("Subgraph name must be alphanumeric and max 20 characters")

    graph = cls(
      graph_id=graph_id,
      graph_name=graph_name,
      graph_type=graph_type,
      base_schema=base_schema,
      schema_extensions=schema_extensions or [],
      graph_instance_id=graph_instance_id,
      graph_cluster_region=graph_cluster_region,
      graph_tier=graph_tier.value if isinstance(graph_tier, GraphTier) else graph_tier,
      graph_metadata=graph_metadata,
      parent_graph_id=parent_graph_id,
      subgraph_index=subgraph_index,
      subgraph_name=subgraph_name,
      is_subgraph=is_subgraph,
      subgraph_metadata=subgraph_metadata,
    )

    session.add(graph)
    try:
      session.commit()
      session.refresh(graph)
    except SQLAlchemyError:
      session.rollback()
      raise
    return graph

  @classmethod
  def get_by_id(cls, graph_id: str, session: Session) -> Optional["Graph"]:
    """Get a graph by its ID."""
    return session.query(cls).filter(cls.graph_id == graph_id).first()

  @classmethod
  def get_by_extension(cls, extension: str, session: Session) -> Sequence["Graph"]:
    """Get all graphs that have a specific schema extension."""
    from sqlalchemy import cast
    from sqlalchemy.dialects.postgresql import JSONB

    return (
      session.query(cls)
      .filter(cast(cls.schema_extensions, JSONB).contains([extension]))
      .all()
    )

  @classmethod
  def get_by_type(cls, graph_type: str, session: Session) -> Sequence["Graph"]:
    """Get all graphs of a specific type."""
    return session.query(cls).filter(cls.graph_type == graph_type).all()

  def update_extensions(self, extensions: List[str], session: Session) -> None:
    """Update the schema extensions for this graph."""
    self.schema_extensions = extensions
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Delete the graph metadata."""
    session.delete(self)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  @classmethod
  def get_subgraphs(cls, parent_graph_id: str, session: Session) -> Sequence["Graph"]:
    """Get all subgraphs for a parent graph."""
    return (
      session.query(cls)
      .filter(cls.parent_graph_id == parent_graph_id, cls.is_subgraph.is_(True))
      .order_by(cls.subgraph_index)
      .all()
    )

  @classmethod
  def get_next_subgraph_index(cls, parent_graph_id: str, session: Session) -> int:
    """Get the next available subgraph index for a parent graph."""
    max_index = (
      session.query(cls.subgraph_index)
      .filter(cls.parent_graph_id == parent_graph_id)
      .order_by(cls.subgraph_index.desc())
      .first()
    )

    if max_index and max_index[0] is not None:
      return int(max_index[0]) + 1
    return 1

  @classmethod
  def validate_subgraph_name(cls, name: str) -> bool:
    """Validate that a subgraph name is alphanumeric and within length limits."""
    import re

    return bool(re.match(r"^[a-zA-Z0-9]{1,20}$", name))
