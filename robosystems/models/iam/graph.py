"""Graph model for storing all graph database metadata.

This model tracks metadata for both user-created graphs and shared repositories.
User graphs use GraphUser for access control (role-based).
Repository graphs use UserRepository for access control (subscription-based).

Graph Ownership:
- Each graph is owned by an organization (org_id)
- The organization is responsible for billing
- Only users within the organization can be granted access
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
  ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import relationship, Session

from ...database import Model
from ...config.graph_tier import GraphTier


class Graph(Model):
  """Graph model for managing graph database metadata."""

  __tablename__ = "graphs"
  __table_args__ = (
    Index("idx_graphs_org", "org_id"),
    Index("idx_graphs_type", "graph_type"),
    Index("idx_graphs_instance", "graph_instance_id"),
    Index("idx_graphs_schema_extensions", "schema_extensions", postgresql_using="gin"),
    Index("idx_graphs_tier", "graph_tier"),
    Index("idx_graphs_parent", "parent_graph_id"),
    Index("idx_graphs_is_subgraph", "is_subgraph"),
    Index("idx_graphs_is_repository", "is_repository"),
    Index("idx_graphs_repository_type", "repository_type"),
    Index("idx_graphs_stale", "graph_stale"),
    CheckConstraint(
      "graph_type IN ('generic', 'entity', 'repository')", name="check_graph_type"
    ),
    UniqueConstraint("parent_graph_id", "subgraph_index", name="unique_subgraph_index"),
    CheckConstraint(
      "(is_subgraph = false AND parent_graph_id IS NULL AND subgraph_index IS NULL AND subgraph_name IS NULL) OR "
      "(is_subgraph = true AND parent_graph_id IS NOT NULL AND subgraph_index IS NOT NULL AND subgraph_name IS NOT NULL)",
      name="check_subgraph_consistency",
    ),
  )

  # Primary identifier - matches the LadybugDB database name
  graph_id = Column(
    String, primary_key=True
  )  # e.g., "kg1a2b3c4d5", "sec", "generic_123"

  # Ownership - graph is owned by an organization
  # Nullable for shared repositories which are system-wide
  org_id = Column(
    String, ForeignKey("orgs.id"), nullable=True
  )  # Organization that owns and pays for this graph (None for shared repositories)

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
    String, nullable=False, default=GraphTier.LADYBUG_STANDARD.value
  )  # ladybug-standard, ladybug-large, ladybug-xlarge, etc. (infrastructure tier)

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

  # Repository support (for shared data repositories like SEC, industry, economic)
  is_repository = Column(
    Boolean, default=False, nullable=False
  )  # True if this is a shared repository
  repository_type = Column(
    String, nullable=True
  )  # Type of repository: "sec", "industry", "economic", etc.
  data_source_type = Column(
    String, nullable=True
  )  # Source type: "sec_edgar", "bls_api", "fred_api", etc.
  data_source_url = Column(String, nullable=True)  # URL or endpoint for data source
  last_sync_at = Column(
    DateTime, nullable=True
  )  # Last successful data synchronization timestamp
  sync_status = Column(
    String, nullable=True
  )  # Sync status: "active", "syncing", "error", "stale"
  sync_frequency = Column(
    String, nullable=True
  )  # Expected sync frequency: "daily", "weekly", "monthly", "quarterly"
  sync_error_message = Column(
    String, nullable=True
  )  # Last error message if sync_status is "error"

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

  # v2 Incremental Ingestion: Staleness tracking for graph database
  graph_stale = Column(
    Boolean, default=False, nullable=False
  )  # True if DuckDB has changes not yet in graph database
  graph_stale_reason = Column(
    String, nullable=True
  )  # Reason for staleness (e.g., "file_deleted", "file_added")
  graph_stale_at = Column(DateTime, nullable=True)  # When graph became stale

  # Additional metadata that might be useful
  graph_metadata = Column(JSONB, nullable=True)  # Flexible field for future use

  # Relationships
  org = relationship("Org", back_populates="graphs")
  graph_users = relationship(
    "GraphUser", back_populates="graph", cascade="all, delete-orphan"
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
      GraphTier.LADYBUG_LARGE.value,
      GraphTier.LADYBUG_XLARGE.value,
      GraphTier.NEO4J_ENTERPRISE_XLARGE.value,
    ]

  def has_specific_extension(self, extension: str) -> bool:
    """Check if this graph has a specific schema extension."""
    extensions = self.schema_extensions or []
    return extension in extensions

  @classmethod
  def create(
    cls,
    graph_id: str,
    org_id: Optional[str],
    graph_name: str,
    graph_type: str,
    session: Session,
    base_schema: Optional[str] = None,
    schema_extensions: Optional[List[str]] = None,
    graph_instance_id: str = "default",
    graph_cluster_region: Optional[str] = None,
    graph_tier: GraphTier = GraphTier.LADYBUG_STANDARD,
    graph_metadata: Optional[Dict[str, Any]] = None,
    parent_graph_id: Optional[str] = None,
    subgraph_index: Optional[int] = None,
    subgraph_name: Optional[str] = None,
    is_subgraph: bool = False,
    subgraph_metadata: Optional[Dict[str, Any]] = None,
    commit: bool = True,
  ) -> "Graph":
    """Create a new graph metadata entry."""
    # Validate graph_type
    if graph_type not in ["generic", "entity", "repository"]:
      raise ValueError("graph_type must be 'generic', 'entity', or 'repository'")

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
      org_id=org_id,
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
    if commit:
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
    """
    Update the schema extensions for this graph.

    This modifies the schema extensions list and commits the change to the database.
    Note: This only updates the metadata - it does not modify the actual database schema.
    Use SchemaManager.apply_extensions() to update the physical schema.

    Args:
        extensions: List of extension names (e.g., ["roboledger", "roboinvestor"])
        session: Database session to use for the update

    Raises:
        SQLAlchemyError: If the database update fails
    """
    self.schema_extensions = extensions
    self.updated_at = datetime.now(timezone.utc)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """
    Delete the graph metadata from the database.

    This removes the Graph record and all associated relationships (UserGraph entries)
    via cascade delete. Note: This does NOT delete the actual graph database -
    use GraphClientFactory to delete the physical database.

    Args:
        session: Database session to use for the deletion

    Raises:
        SQLAlchemyError: If the database deletion fails
    """
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

  @classmethod
  def get_all_repositories(cls, session: Session) -> Sequence["Graph"]:
    """Get all shared repository graphs."""
    return (
      session.query(cls)
      .filter(cls.is_repository.is_(True))
      .order_by(cls.repository_type)
      .all()
    )

  @classmethod
  def get_repository_by_type(
    cls, repository_type: str, session: Session
  ) -> Optional["Graph"]:
    """Get a repository by its type."""
    return (
      session.query(cls)
      .filter(cls.is_repository.is_(True), cls.repository_type == repository_type)
      .first()
    )

  @classmethod
  def find_or_create_repository(
    cls,
    graph_id: str,
    graph_name: str,
    repository_type: str,
    session: Session,
    base_schema: Optional[str] = None,
    data_source_type: Optional[str] = None,
    data_source_url: Optional[str] = None,
    sync_frequency: Optional[str] = None,
    graph_tier: GraphTier = GraphTier.LADYBUG_SHARED,
    graph_instance_id: str = "ladybug-shared-prod",
  ) -> "Graph":
    """
    Find or create a repository graph entry.

    This is used by data pipelines (SEC, etc.) to ensure repository metadata
    exists on first access.

    Args:
        graph_id: Unique identifier (e.g., "sec", "industry")
        graph_name: Human-readable name
        repository_type: Type of repository (matches graph_id typically)
        session: Database session
        base_schema: Schema to use (e.g., "sec")
        data_source_type: Source type (e.g., "sec_edgar")
        data_source_url: URL for data source
        sync_frequency: Sync frequency ("daily", "weekly", etc.)
        graph_tier: Infrastructure tier
        graph_instance_id: Instance identifier

    Returns:
        Graph: Existing or newly created repository graph
    """
    existing = cls.get_by_id(graph_id, session)
    if existing:
      return existing

    repository = cls.create(
      graph_id=graph_id,
      org_id=None,  # Shared repositories are not owned by any org
      graph_name=graph_name,
      graph_type="repository",
      session=session,
      base_schema=base_schema or repository_type,
      graph_tier=graph_tier,
      graph_instance_id=graph_instance_id,
      commit=False,
    )

    repository.is_repository = True
    repository.repository_type = repository_type
    repository.data_source_type = data_source_type
    repository.data_source_url = data_source_url
    repository.sync_frequency = sync_frequency
    repository.sync_status = "active"

    try:
      session.commit()
      session.refresh(repository)
    except SQLAlchemyError:
      session.rollback()
      raise

    return repository

  def update_sync_status(
    self,
    status: str,
    error_message: Optional[str] = None,
    session: Session = None,
  ) -> None:
    """
    Update repository sync status and timestamp.

    This method is used by sync pipelines to track the synchronization state
    of shared repositories. When status is "active", it records the sync timestamp
    and clears any error messages. When status is "error", it stores the error message.

    Args:
        status: Sync status ("active", "syncing", "error", "stale")
        error_message: Error message if status is "error", otherwise ignored
        session: Optional database session to commit changes immediately

    Raises:
        ValueError: If called on a non-repository graph or invalid status
        SQLAlchemyError: If the database update fails
    """
    VALID_STATUSES = {"active", "syncing", "error", "stale"}

    if not self.is_repository:
      raise ValueError("Can only update sync status for repository graphs")

    if status not in VALID_STATUSES:
      raise ValueError(
        f"Invalid sync status '{status}'. Must be one of: {', '.join(sorted(VALID_STATUSES))}"
      )

    self.sync_status = status
    if status == "active":
      self.last_sync_at = datetime.now(timezone.utc)
      self.sync_error_message = None
    elif status == "error":
      self.sync_error_message = error_message

    self.updated_at = datetime.now(timezone.utc)

    if session:
      try:
        session.commit()
        session.refresh(self)
      except SQLAlchemyError:
        session.rollback()
        raise

  @property
  def is_user_graph(self) -> bool:
    """Check if this is a user-created graph (not a repository)."""
    return not bool(self.is_repository)

  @property
  def needs_sync(self) -> bool:
    """Check if repository needs synchronization."""
    if not self.is_repository:
      return False

    if self.sync_status in ["error", "stale"]:
      return True

    if not self.last_sync_at:
      return True

    if not self.sync_frequency:
      return False

    from datetime import timedelta

    frequency_map = {
      "daily": timedelta(days=1),
      "weekly": timedelta(weeks=1),
      "monthly": timedelta(days=30),
      "quarterly": timedelta(days=90),
    }

    sync_interval = frequency_map.get(str(self.sync_frequency))
    if not sync_interval:
      return False

    last_sync = self.last_sync_at
    if last_sync.tzinfo is None:
      last_sync = last_sync.replace(tzinfo=timezone.utc)

    time_since_sync = datetime.now(timezone.utc) - last_sync
    return time_since_sync > sync_interval

  def mark_stale(self, session: Session, reason: str) -> None:
    """Mark the graph as stale due to DuckDB changes not yet in graph database.

    Args:
      session: Database session for committing changes
      reason: Reason for staleness (e.g., "file_deleted", "file_added")
    """
    self.graph_stale = True
    self.graph_stale_reason = reason
    self.graph_stale_at = datetime.now(timezone.utc)
    session.commit()

  def mark_fresh(self, session: Session) -> None:
    """Mark the graph as fresh after sync with DuckDB.

    Also records the materialization timestamp in graph_metadata.

    Args:
      session: Database session for committing changes
    """
    self.graph_stale = False
    self.graph_stale_reason = None
    self.graph_stale_at = None

    metadata = {**self.graph_metadata} if self.graph_metadata else {}
    metadata["last_materialized_at"] = datetime.now(timezone.utc).isoformat()

    if "materialization_count" in metadata:
      metadata["materialization_count"] += 1
    else:
      metadata["materialization_count"] = 1

    self.graph_metadata = metadata
    session.commit()
