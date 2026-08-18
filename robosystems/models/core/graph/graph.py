"""Metadata for every graph database, user-created or shared repository.

Access control differs by kind: user graphs go through ``GraphUser``
(role-based), shared repositories through ``UserRepository``
(subscription-based).

A graph is owned by one organization (``org_id``), which is the billing party
and the only source of users who can be granted access. Shared repositories
have no owning org.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
  Boolean,
  CheckConstraint,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  UniqueConstraint,
  or_,
  update,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.config.graph_tier import GraphTier
from robosystems.database import Model


class GraphStatus(str, Enum):
  """Graph lifecycle state.

  ``suspended`` blocks access while leaving infrastructure in place;
  ``deprovisioned`` is terminal and means the infrastructure is torn down.
  """

  ACTIVE = "active"
  SUSPENDED = "suspended"
  DEPROVISIONED = "deprovisioned"


class Graph(Model):
  """A graph database and its platform-side metadata."""

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
    Index("idx_graphs_status", "status"),
    Index("idx_graphs_status_tier_created", "status", "graph_tier", "created_at"),
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

  # Subgraph support (all dedicated tiers; max count varies by tier)
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
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  # Staleness tracking: set when the DuckDB staging layer holds changes the
  # graph database has not materialized yet.
  graph_stale = Column(
    Boolean, default=False, nullable=False
  )  # True if DuckDB has changes not yet in graph database
  graph_stale_reason = Column(
    String, nullable=True
  )  # Reason for staleness (e.g., "file_deleted", "file_added")
  graph_stale_at = Column(DateTime, nullable=True)  # When graph became stale

  graph_metadata = Column(JSONB, nullable=True)  # Free-form extras

  # Per-graph taxonomy library pinning: {standard: version, ...}.
  # When NULL, the provisioner falls back to DEFAULT_TAXONOMY_PIN. Each
  # listed (standard, version) pair is copied from public.* into the tenant
  # schema at provision time. See robosystems/taxonomy/pins.py.
  taxonomy_pin = Column(JSONB, nullable=True)

  # Reporting Style lives on the entity (extensions DB), not the graph —
  # co-located with the ``structures`` / ``reporting_style_networks`` it
  # points at, and at the grain where heterogeneous subsidiaries can each
  # carry their own Style. See ``models/extensions/entity.py`` and
  # ``operations/roboledger/reports/network_picker.py``.

  # Per-graph autopilot for the period-boundary obligation promoter.
  # When False (default — co-pilot), the sensor flips matured `pending`
  # schedule_entry_due events to `classified` but stops there; an
  # operator/agent drives draft creation. When True (autopilot), the
  # sensor also dispatches the registered handler so the closing-entry
  # draft lands in the GL on the same tick. Overrides the process-wide
  # EXTENSIONS_PROMOTION_AUTO_DISPATCH env var; the env var supplies the
  # default when this column is NULL.
  auto_dispatch_obligations = Column(Boolean, nullable=True)

  # Lifecycle status
  status = Column(
    String, nullable=False, default=GraphStatus.ACTIVE.value
  )  # queued, provisioning, active, suspended, deprovisioned

  # Soft-delete marker
  deleted_at = Column(DateTime, nullable=True)

  # Relationships
  org = relationship("Org", back_populates="graphs")
  graph_users = relationship(
    "GraphUser", back_populates="graph", cascade="all, delete-orphan"
  )

  def __repr__(self) -> str:
    if bool(self.is_subgraph):
      return f"<Graph {self.graph_id!s} (subgraph of {self.parent_graph_id!s}) type={self.graph_type!s}>"
    return f"<Graph {self.graph_id!s} type={self.graph_type!s} extensions={self.schema_extensions!s}>"

  @property
  def has_extension(self) -> bool:
    """Check if this graph has any schema extensions."""
    extensions = self.schema_extensions
    if extensions is None:
      return False
    # At runtime, extensions is a list; type checker doesn't know this
    return len(extensions) > 0

  @property
  def description(self) -> str:
    """Free-form description, or ``""`` when unset.

    Stored inside the ``graph_metadata`` JSONB blob rather than in a column
    of its own, so nothing at the database level constrains what a row can
    hold. Type-check rather than trust: a malformed value reaching a
    response model fails validation for the *whole* response, so a single
    bad row would take out the entire graph list.
    """
    metadata = self.graph_metadata
    if not isinstance(metadata, dict):
      return ""
    description = metadata.get("description")
    return description if isinstance(description, str) else ""

  @property
  def tags(self) -> list[str]:
    """Organizational tags, or ``[]`` when unset.

    Same free-form JSONB caveat as ``description`` — non-string entries are
    dropped rather than passed through to a response model.
    """
    metadata = self.graph_metadata
    if not isinstance(metadata, dict):
      return []
    tags = metadata.get("tags")
    if not isinstance(tags, list):
      return []
    return [tag for tag in tags if isinstance(tag, str)]

  @property
  def database_name(self) -> str:
    """Database name on disk: ``{parent_graph_id}_{subgraph_name}`` for a
    subgraph, the graph ID itself otherwise."""
    if (
      bool(self.is_subgraph)
      and self.parent_graph_id is not None
      and self.subgraph_name is not None
    ):
      return f"{self.parent_graph_id!s}_{self.subgraph_name!s}"
    return str(self.graph_id)

  @property
  def can_have_subgraphs(self) -> bool:
    """Check if this graph tier supports subgraphs."""
    return str(self.graph_tier) in [
      GraphTier.LADYBUG_STANDARD.value,
      GraphTier.LADYBUG_LARGE.value,
      GraphTier.LADYBUG_XLARGE.value,
    ]

  def has_specific_extension(self, extension: str) -> bool:
    """Check if this graph has a specific schema extension."""
    extensions = self.schema_extensions or []
    return extension in extensions

  @classmethod
  def create(
    cls,
    graph_id: str,
    org_id: str | None,
    graph_name: str,
    graph_type: str,
    session: Session,
    base_schema: str | None = None,
    schema_extensions: list[str] | None = None,
    graph_instance_id: str = "default",
    graph_cluster_region: str | None = None,
    graph_tier: GraphTier = GraphTier.LADYBUG_STANDARD,
    graph_metadata: dict[str, Any] | None = None,
    parent_graph_id: str | None = None,
    subgraph_index: int | None = None,
    subgraph_name: str | None = None,
    is_subgraph: bool = False,
    subgraph_metadata: dict[str, Any] | None = None,
    status: GraphStatus = GraphStatus.ACTIVE,
    commit: bool = True,
  ) -> "Graph":
    """Create a new graph metadata entry."""
    if graph_type not in ["generic", "entity", "repository"]:
      raise ValueError("graph_type must be 'generic', 'entity', or 'repository'")

    if graph_type == "entity" and not base_schema:
      base_schema = "base"

    if is_subgraph:
      if not parent_graph_id or subgraph_index is None or not subgraph_name:
        raise ValueError(
          "Subgraphs require parent_graph_id, subgraph_index, and subgraph_name"
        )

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
      status=status.value if isinstance(status, GraphStatus) else status,
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

  @property
  def is_active(self) -> bool:
    """Check if graph is in active status."""
    return self.status == GraphStatus.ACTIVE.value

  @property
  def is_operational(self) -> bool:
    """Check if graph is in an operational state."""
    return self.status == GraphStatus.ACTIVE.value

  @classmethod
  def get_by_id(
    cls, graph_id: str, session: Session, include_deprovisioned: bool = False
  ) -> Optional["Graph"]:
    """Get a graph by its ID, skipping deprovisioned graphs by default."""
    query = session.query(cls).filter(cls.graph_id == graph_id)
    if not include_deprovisioned:
      query = query.filter(cls.status != GraphStatus.DEPROVISIONED.value)
    return query.first()

  @classmethod
  def get_active_by_id(cls, graph_id: str, session: Session) -> Optional["Graph"]:
    """Get a graph only if it is in active status."""
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.status == GraphStatus.ACTIVE.value)
      .first()
    )

  _VALID_TRANSITIONS: dict[str, list[str]] = {
    "active": ["suspended", "deprovisioned"],
    "suspended": ["active", "deprovisioned"],
    "deprovisioned": [],
  }

  def transition_status(self, new_status: GraphStatus, session: Session) -> None:
    """Transition graph to a new lifecycle status."""
    current = self.status or GraphStatus.ACTIVE.value
    allowed = self._VALID_TRANSITIONS.get(current, [])
    if new_status.value not in allowed:
      raise ValueError(
        f"Invalid status transition: {current} -> {new_status.value} "
        f"(allowed: {allowed})"
      )
    self.status = new_status.value
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

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

  def update_extensions(self, extensions: list[str], session: Session) -> None:
    """Record the schema extensions (``["roboledger", "roboinvestor"]``).

    Metadata only — the physical graph schema is unchanged. Use
    ``SchemaManager.apply_extensions()`` for that.
    """
    self.schema_extensions = extensions
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Delete the Graph row and its ``GraphUser`` grants (cascade).

    The physical graph database survives — tear that down through
    ``GraphClientFactory``.
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
    base_schema: str | None = None,
    schema_extensions: list[str] | None = None,
    data_source_type: str | None = None,
    data_source_url: str | None = None,
    sync_frequency: str | None = None,
    graph_tier: GraphTier = GraphTier.LADYBUG_SHARED,
    graph_instance_id: str = "ladybug-shared-prod",
  ) -> "Graph":
    """Find or create a repository graph entry.

    Data pipelines (SEC and friends) call this so repository metadata exists on
    first access.
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
      base_schema=base_schema or "base",
      schema_extensions=schema_extensions,
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
    error_message: str | None = None,
    session: Session = None,
  ) -> None:
    """Update a shared repository's sync status.

    ``active`` stamps ``last_sync_at`` and clears any prior error; ``error``
    stores ``error_message``. Rejected on non-repository graphs.
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
      self.last_sync_at = datetime.now(UTC)
      self.sync_error_message = None
    elif status == "error":
      self.sync_error_message = error_message

    self.updated_at = datetime.now(UTC)

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
      last_sync = last_sync.replace(tzinfo=UTC)

    time_since_sync = datetime.now(UTC) - last_sync
    return time_since_sync > sync_interval

  def mark_stale(self, session: Session, reason: str) -> None:
    """Mark the graph stale — DuckDB holds changes the graph lacks."""
    self.graph_stale = True
    self.graph_stale_reason = reason
    self.graph_stale_at = datetime.now(UTC)
    session.commit()

  def mark_fresh(self, session: Session, *, started_at: datetime | None = None) -> bool:
    """Record a completed materialization; clear staleness only if nothing
    was written after the materialization began.

    ``started_at`` is the moment the materialization snapshotted its source
    (before staging). A write that lands after that stamps a later
    ``graph_stale_at`` and is not in the graph, so clearing the flag would
    lose it until an unrelated later write — the compare-and-clear is done
    in SQL so a stale identity map cannot mask a newer stamp. Without
    ``started_at`` the clear is unconditional (legacy callers).

    Always stamps ``last_materialized_at`` and bumps
    ``materialization_count`` in ``graph_metadata``. Returns True when the
    staleness flag was cleared.
    """
    metadata = {**self.graph_metadata} if self.graph_metadata else {}
    metadata["last_materialized_at"] = datetime.now(UTC).isoformat()

    if "materialization_count" in metadata:
      metadata["materialization_count"] += 1
    else:
      metadata["materialization_count"] = 1

    self.graph_metadata = metadata
    session.flush()

    clear = (
      update(Graph)
      .where(Graph.graph_id == self.graph_id)
      .values(graph_stale=False, graph_stale_reason=None, graph_stale_at=None)
      .execution_options(synchronize_session=False)
    )
    if started_at is not None:
      clear = clear.where(
        or_(Graph.graph_stale_at.is_(None), Graph.graph_stale_at <= started_at)
      )
    cleared = (session.execute(clear).rowcount or 0) > 0
    session.commit()
    # The commit expires the instance, so callers reading the stale fields
    # afterwards see the row as the UPDATE left it, not the pre-clear values.
    session.refresh(self)
    return cleared
