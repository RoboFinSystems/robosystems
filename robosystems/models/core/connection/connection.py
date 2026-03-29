"""Connection model for managing data source connections.

Connections represent links between a user's graph and external data sources
(QuickBooks, SEC). All connection metadata is stored in PostgreSQL.
Encrypted credentials are stored separately in ConnectionCredentials.
"""

import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import Model


class ConnectionStatus(str, Enum):
  """Connection lifecycle status."""

  PENDING_OAUTH = "pending_oauth"
  CONNECTED = "connected"
  ERROR = "error"
  DISCONNECTED = "disconnected"


class Connection(Model):
  """Data source connection metadata."""

  __tablename__ = "connections"
  __table_args__ = (
    Index("idx_connections_graph", "graph_id"),
    Index("idx_connections_user", "user_id"),
    Index("idx_connections_provider", "provider"),
    Index("idx_connections_graph_provider", "graph_id", "provider"),
  )

  id = Column(
    String, primary_key=True, default=lambda: f"conn_{secrets.token_urlsafe(16)}"
  )
  graph_id = Column(String, ForeignKey("graphs.graph_id"), nullable=False)
  user_id = Column(String, ForeignKey("users.id"), nullable=False)
  provider = Column(String, nullable=False)  # quickbooks, sec
  status = Column(String, default=ConnectionStatus.PENDING_OAUTH, nullable=False)

  # Provider-specific metadata
  realm_id = Column(String, nullable=True)  # QuickBooks realm ID
  item_id = Column(String, nullable=True)
  cik = Column(String, nullable=True)  # SEC Central Index Key
  entity_name = Column(String, nullable=True)
  institution_name = Column(String, nullable=True)

  # Sync tracking
  auto_sync_enabled = Column(Boolean, default=True, nullable=False)
  last_sync = Column(DateTime, nullable=True)

  # Timestamps
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  def __repr__(self) -> str:
    return f"<Connection {self.id} {self.provider} graph={self.graph_id}>"

  @classmethod
  def create(
    cls,
    graph_id: str,
    user_id: str,
    provider: str,
    session: Session,
    status: str = ConnectionStatus.PENDING_OAUTH,
    realm_id: str | None = None,
    item_id: str | None = None,
    cik: str | None = None,
    entity_name: str | None = None,
    institution_name: str | None = None,
    auto_sync_enabled: bool = True,
  ) -> "Connection":
    """Create a new connection."""
    conn = cls(
      graph_id=graph_id,
      user_id=user_id,
      provider=provider,
      status=status,
      realm_id=realm_id,
      item_id=item_id,
      cik=cik,
      entity_name=entity_name,
      institution_name=institution_name,
      auto_sync_enabled=auto_sync_enabled,
    )
    session.add(conn)
    try:
      session.commit()
      session.refresh(conn)
    except SQLAlchemyError:
      session.rollback()
      raise
    return conn

  @classmethod
  def get_by_id(cls, connection_id: str, session: Session) -> Optional["Connection"]:
    """Get connection by ID."""
    return session.query(cls).filter(cls.id == connection_id).first()

  @classmethod
  def get_by_graph_and_provider(
    cls, graph_id: str, provider: str, session: Session
  ) -> Sequence["Connection"]:
    """Get all connections for a graph and provider."""
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.provider == provider)
      .order_by(cls.created_at.desc())
      .all()
    )

  @classmethod
  def get_all_for_graph(cls, graph_id: str, session: Session) -> Sequence["Connection"]:
    """Get all connections for a graph."""
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id)
      .order_by(cls.created_at.desc())
      .all()
    )

  @classmethod
  def list_filtered(
    cls,
    session: Session,
    graph_id: str | None = None,
    user_id: str | None = None,
    provider: str | None = None,
  ) -> Sequence["Connection"]:
    """List connections with optional filters."""
    query = session.query(cls)
    if graph_id:
      query = query.filter(cls.graph_id == graph_id)
    if user_id:
      query = query.filter(cls.user_id == user_id)
    if provider:
      query = query.filter(cls.provider.ilike(provider))
    return query.order_by(cls.created_at.desc()).all()

  def update_status(self, status: str, session: Session) -> None:
    """Update connection status."""
    self.status = status
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def update_last_sync(self, session: Session) -> None:
    """Update last sync timestamp."""
    self.last_sync = datetime.now(UTC)
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def update_metadata(
    self,
    session: Session,
    status: str | None = None,
    realm_id: str | None = None,
    item_id: str | None = None,
    cik: str | None = None,
    entity_name: str | None = None,
    institution_name: str | None = None,
    auto_sync_enabled: bool | None = None,
  ) -> None:
    """Update connection metadata fields."""
    if status is not None:
      self.status = status
    if realm_id is not None:
      self.realm_id = realm_id
    if item_id is not None:
      self.item_id = item_id
    if cik is not None:
      self.cik = cik
    if entity_name is not None:
      self.entity_name = entity_name
    if institution_name is not None:
      self.institution_name = institution_name
    if auto_sync_enabled is not None:
      self.auto_sync_enabled = auto_sync_enabled
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Delete the connection."""
    try:
      session.delete(self)
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def to_dict(self) -> dict:
    """Convert to dictionary matching the legacy return format."""
    return {
      "connection_id": self.id,
      "provider": self.provider,
      "status": self.status,
      "entity_id": self.graph_id,  # backward compat
      "graph_id": self.graph_id,
      "user_id": self.user_id,
      "metadata": {
        "realm_id": self.realm_id,
        "item_id": self.item_id,
        "cik": self.cik,
        "entity_name": self.entity_name,
        "institution_name": self.institution_name,
        "auto_sync_enabled": self.auto_sync_enabled,
        "last_sync": self.last_sync.isoformat() if self.last_sync else None,
      },
      "created_at": self.created_at,
      "updated_at": self.updated_at,
    }
