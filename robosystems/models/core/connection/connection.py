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
  """Connection lifecycle status.

  `NEEDS_REAUTH` is distinct from `ERROR`: the credential bundle is no
  longer valid (Intuit revoked / rotated past grace / scope insufficient)
  and the operator must re-OAuth. UI surfaces this as a "Reconnect" CTA
  rather than a generic "sync failed" message.
  """

  PENDING_OAUTH = "pending_oauth"
  CONNECTED = "connected"
  ERROR = "error"
  NEEDS_REAUTH = "needs_reauth"
  DISCONNECTED = "disconnected"


class WritePolicy(str, Enum):
  """Connection source-of-truth policy (Phase 4 §4.2).

  Governs whether RoboSystems-originated entries (manual JE, schedule
  drafts) flow into the source-of-truth system on the way to posted GL.

  - ``NATIVE``: RoboSystems IS the source of truth. RL-originated events
    write GL rows locally on dispatch; no outbound publish. The
    connection's inbound sync (if any) captures-to-inbox per
    event-driven-ledger.md.
  - ``QB_AUTHORITATIVE``: QuickBooks IS the source of truth. RL-originated
    events publish to QB via ``execute-event-block``; local GL holds as
    DRAFT until QB accepts. Inbound QB sync auto-commits (preserves
    pre-Phase-4 behavior). The cross-source matcher recognises round-
    tripped entries by ``metadata.qb_external_id`` and skips re-creation.
  - ``HYBRID``: QB authoritative with exception-flagging heuristics
    (low-confidence mapping, manual JE source class, amount-over-
    threshold). v1 ships only NATIVE + QB_AUTHORITATIVE; HYBRID lands
    when a real customer needs it.
  """

  NATIVE = "native"
  QB_AUTHORITATIVE = "qb_authoritative"
  HYBRID = "hybrid"


class Connection(Model):
  """Data source connection metadata."""

  __tablename__ = "connections"
  __table_args__ = (
    Index("idx_connections_graph", "graph_id"),
    Index("idx_connections_user", "user_id"),
    Index("idx_connections_provider", "provider"),
    Index("idx_connections_graph_provider", "graph_id", "provider"),
    # Phase 3 B6: re-OAuth reuse path queries this index to find a
    # soft-deleted connection for a freshly-OAuthed realm. Partial because
    # only soft-deleted rows are interesting to that query.
    Index(
      "idx_connections_soft_deleted_realm",
      "graph_id",
      "provider",
      "realm_id",
      postgresql_where="deleted_at IS NOT NULL",
    ),
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

  # Phase 4 §4.2 — source-of-truth policy. Default `'native'` preserves
  # pre-Phase-4 behavior for all existing connections (no outbound writes
  # without explicit operator opt-in via UI / API). The loader's
  # auto-commit branch reads this column instead of the legacy
  # `_SOURCE_AUTO_COMMITS` hardcode.
  write_policy = Column(
    String, default=WritePolicy.NATIVE.value, server_default="native", nullable=False
  )

  # Soft-delete marker (B6). When non-null the row is invisible to the
  # default lookup helpers below. Re-OAuth to the same realm revives the
  # row in place (preserves connection_id; downstream events/agents/
  # elements scoped to it stay live) rather than minting a new row and
  # orphaning the prior tenant data.
  deleted_at = Column(DateTime, nullable=True)

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
  def get_by_id(
    cls,
    connection_id: str,
    session: Session,
    *,
    include_deleted: bool = False,
  ) -> Optional["Connection"]:
    """Get connection by ID.

    Soft-deleted rows (`deleted_at IS NOT NULL`) are filtered out by
    default — the only callers that need to see them are the OAuth
    re-OAuth reuse path (B6) and admin tooling.
    """
    query = session.query(cls).filter(cls.id == connection_id)
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.first()

  @classmethod
  def get_by_graph_and_provider(
    cls,
    graph_id: str,
    provider: str,
    session: Session,
    *,
    include_deleted: bool = False,
  ) -> Sequence["Connection"]:
    """Get all connections for a graph and provider (live by default)."""
    query = session.query(cls).filter(
      cls.graph_id == graph_id, cls.provider == provider
    )
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.order_by(cls.created_at.desc()).all()

  @classmethod
  def get_all_for_graph(
    cls,
    graph_id: str,
    session: Session,
    *,
    include_deleted: bool = False,
  ) -> Sequence["Connection"]:
    """Get all connections for a graph (live by default)."""
    query = session.query(cls).filter(cls.graph_id == graph_id)
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.order_by(cls.created_at.desc()).all()

  @classmethod
  def list_filtered(
    cls,
    session: Session,
    graph_id: str | None = None,
    user_id: str | None = None,
    provider: str | None = None,
    *,
    include_deleted: bool = False,
  ) -> Sequence["Connection"]:
    """List connections with optional filters (live by default)."""
    query = session.query(cls)
    if graph_id:
      query = query.filter(cls.graph_id == graph_id)
    if user_id:
      query = query.filter(cls.user_id == user_id)
    if provider:
      query = query.filter(cls.provider.ilike(provider))
    if not include_deleted:
      query = query.filter(cls.deleted_at.is_(None))
    return query.order_by(cls.created_at.desc()).all()

  @classmethod
  def find_soft_deleted_for_realm(
    cls,
    graph_id: str,
    provider: str,
    realm_id: str,
    session: Session,
  ) -> Optional["Connection"]:
    """Find a soft-deleted connection for re-OAuth reuse (B6).

    Returns the most-recently-deleted soft-deleted connection matching
    the (graph_id, provider, realm_id) triple. Used by the OAuth
    callback to revive a prior connection rather than mint a new one
    when the user reconnects to the same QB realm.
    """
    return (
      session.query(cls)
      .filter(
        cls.graph_id == graph_id,
        cls.provider == provider,
        cls.realm_id == realm_id,
        cls.deleted_at.is_not(None),
      )
      .order_by(cls.deleted_at.desc())
      .first()
    )

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
    """Hard-delete the connection row.

    Almost always the wrong choice — prefer ``soft_delete`` so the
    tenant-side events/agents/elements scoped to this connection_id
    don't orphan. Kept for admin tooling that intentionally needs to
    purge.
    """
    try:
      session.delete(self)
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def soft_delete(self, session: Session) -> None:
    """Soft-delete (B6) — mark the row deleted without removing it.

    Default lookup helpers skip soft-deleted rows. Re-OAuth to the
    same realm can revive the row in place via ``restore``, preserving
    connection_id so tenant-side events/agents/elements stay attached.
    """
    self.deleted_at = datetime.now(UTC)
    self.updated_at = self.deleted_at
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def restore(self, session: Session) -> None:
    """Revive a soft-deleted connection (B6 re-OAuth reuse path)."""
    self.deleted_at = None
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
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
      "write_policy": self.write_policy,
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
      "deleted_at": self.deleted_at,
    }
