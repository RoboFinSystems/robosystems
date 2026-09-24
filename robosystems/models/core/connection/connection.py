"""Links between a graph and external data sources. Encrypted credentials live
separately in ConnectionCredentials."""

import secrets
from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Optional

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import Model


class ConnectionStatus(str, Enum):
  """Connection lifecycle status.

  `NEEDS_REAUTH`: the credentials are no longer valid and the user must
  re-OAuth (distinct from a failed sync, `ERROR`).
  `SEVERED`: the tenant kept the provider-created chart and went native. The
  row is soft-deleted and never revived by a later re-OAuth.
  """

  PENDING_OAUTH = "pending_oauth"
  CONNECTED = "connected"
  ERROR = "error"
  NEEDS_REAUTH = "needs_reauth"
  DISCONNECTED = "disconnected"
  SEVERED = "severed"


class WritePolicy(str, Enum):
  """Whether RoboSystems-originated entries write back to an external GL.

  - ``NATIVE``: RoboSystems is the source of truth; GL rows are written
    locally, nothing is published.
  - ``QB_AUTHORITATIVE``: entries publish to QuickBooks via
    ``execute-event-block`` and stay DRAFT locally until QB accepts.
    Round-tripped entries are matched by ``metadata.qb_external_id``.
  - ``HYBRID``: reserved, not implemented.
  """

  NATIVE = "native"
  QB_AUTHORITATIVE = "qb_authoritative"
  HYBRID = "hybrid"


# Per-provider default outbound write policy. A connected QuickBooks is the
# general ledger, so it is authoritative; providers with no external GL stay
# native. Inbound sync is independent of this.
_PROVIDER_WRITE_POLICY_DEFAULTS: dict[str, str] = {
  "quickbooks": WritePolicy.QB_AUTHORITATIVE.value,
}


def default_write_policy_for_provider(provider: str) -> str:
  return _PROVIDER_WRITE_POLICY_DEFAULTS.get(provider, WritePolicy.NATIVE.value)


class Connection(Model):
  """Data source connection metadata."""

  __tablename__ = "connections"
  __table_args__ = (
    Index("idx_connections_graph", "graph_id"),
    Index("idx_connections_user", "user_id"),
    Index("idx_connections_provider", "provider"),
    Index("idx_connections_graph_provider", "graph_id", "provider"),
    # Serves the re-OAuth reuse lookup (find_soft_deleted_for_realm).
    Index(
      "idx_connections_soft_deleted_realm",
      "graph_id",
      "provider",
      "realm_id",
      postgresql_where="deleted_at IS NOT NULL",
    ),
    # One live external source name per graph — the event-source registry's
    # uniqueness guarantee (advisory pre-checks race; this doesn't).
    Index(
      "uq_connections_graph_source_name",
      "graph_id",
      "source_name",
      unique=True,
      postgresql_where="source_name IS NOT NULL AND deleted_at IS NULL",
    ),
  )

  id = Column(
    String, primary_key=True, default=lambda: f"conn_{secrets.token_urlsafe(16)}"
  )
  graph_id = Column(String, ForeignKey("graphs.graph_id"), nullable=False)
  user_id = Column(String, ForeignKey("users.id"), nullable=False)
  provider = Column(String, nullable=False)  # quickbooks, external
  status = Column(String, default=ConnectionStatus.PENDING_OAUTH, nullable=False)

  # Provider-specific metadata
  realm_id = Column(String, nullable=True)  # QuickBooks realm ID
  item_id = Column(String, nullable=True)
  cik = Column(String, nullable=True)  # SEC Central Index Key
  entity_name = Column(String, nullable=True)
  institution_name = Column(String, nullable=True)
  # External provider's registered Event.source slug; the registration is the
  # event-source allow-list entry. No credentials are held for these.
  source_name = Column(String, nullable=True)

  # Sync tracking
  auto_sync_enabled = Column(Boolean, default=True, nullable=False)
  last_sync = Column(DateTime, nullable=True)
  # Outcome of the most recent sync attempt, success or failure (status,
  # window, per-category counts, truncated errors). `last_sync` advances only
  # on success because the close gate's sync-current check reads it.
  last_sync_result = Column(JSONB, nullable=True)

  # Outbound write-back policy (see WritePolicy). `create` applies the
  # per-provider default; inbound auto-commit does not read this column.
  write_policy = Column(
    String, default=WritePolicy.NATIVE.value, server_default="native", nullable=False
  )

  # Advanced only after a successful batch commit, so a failure replays from
  # the prior value (SyncToken-gated UPSERT makes replay a no-op). NULL means
  # no CDC sync has succeeded yet; extraction falls back to a full window.
  last_cdc_watermark = Column(DateTime, nullable=True)

  # Soft-delete marker, hidden from the default lookups. Re-OAuth to the same
  # realm revives the row so tenant data keyed on connection_id stays attached.
  deleted_at = Column(DateTime, nullable=True)
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
    source_name: str | None = None,
    auto_sync_enabled: bool = True,
    write_policy: str | None = None,
  ) -> "Connection":
    """Create a new connection.

    ``write_policy`` defaults per provider (QuickBooks → ``qb_authoritative``,
    others → ``native``); pass an explicit value to override.
    """
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
      source_name=source_name,
      auto_sync_enabled=auto_sync_enabled,
      write_policy=(
        write_policy
        if write_policy is not None
        else default_write_policy_for_provider(provider)
      ),
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
    """Get a connection by ID, excluding soft-deleted rows by default."""
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
    """Most recently soft-deleted connection for this realm, for re-OAuth
    revival. A ``severed`` row is never a candidate."""
    return (
      session.query(cls)
      .filter(
        cls.graph_id == graph_id,
        cls.provider == provider,
        cls.realm_id == realm_id,
        cls.deleted_at.is_not(None),
        cls.status != ConnectionStatus.SEVERED.value,
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

  def update_last_sync(self, session: Session, result: dict | None = None) -> None:
    """Update last sync timestamp (success path) + optional outcome summary."""
    self.last_sync = datetime.now(UTC)
    if result is not None:
      self.last_sync_result = result
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def record_sync_result(self, session: Session, result: dict) -> None:
    """Persist a failed sync's outcome without advancing `last_sync`."""
    self.last_sync_result = result
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def advance_cdc_watermark(self, watermark: datetime, session: Session) -> None:
    """Advance the CDC watermark. Call only after the batch load committed.

    Stored as naive UTC; a tz-aware value is converted.
    """
    if watermark.tzinfo is not None:
      watermark = watermark.astimezone(UTC).replace(tzinfo=None)
    self.last_cdc_watermark = watermark
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

  def set_write_policy(self, session: Session, write_policy: str) -> None:
    """Set the outbound write policy. ``HYBRID`` is rejected (not implemented)."""
    allowed = {WritePolicy.NATIVE.value, WritePolicy.QB_AUTHORITATIVE.value}
    if write_policy not in allowed:
      raise ValueError(
        f"Unsupported write_policy '{write_policy}'. Allowed: {sorted(allowed)}."
      )
    self.write_policy = write_policy
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Hard-delete (admin purge only). Prefer ``soft_delete``, which keeps
    tenant data keyed on connection_id attached."""
    try:
      session.delete(self)
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def soft_delete(self, session: Session) -> None:
    """Mark the row deleted; ``restore`` revives it in place."""
    self.deleted_at = datetime.now(UTC)
    self.updated_at = self.deleted_at
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def restore(self, session: Session) -> None:
    """Revive a soft-deleted connection, re-applying the provider's default
    write policy (disconnect resets it to ``native``)."""
    self.deleted_at = None
    self.updated_at = datetime.now(UTC)
    self.write_policy = default_write_policy_for_provider(self.provider)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def to_dict(self) -> dict:
    """Serialize for the connections API surface.

    ``entity_id`` duplicates ``graph_id``: consumers still read the older
    key, so both ship on every response.
    """
    return {
      "connection_id": self.id,
      "provider": self.provider,
      "status": self.status,
      "entity_id": self.graph_id,
      "graph_id": self.graph_id,
      "user_id": self.user_id,
      "write_policy": self.write_policy,
      "source_name": self.source_name,
      "metadata": {
        "realm_id": self.realm_id,
        "item_id": self.item_id,
        "cik": self.cik,
        "entity_name": self.entity_name,
        "institution_name": self.institution_name,
        "source_name": self.source_name,
        "auto_sync_enabled": self.auto_sync_enabled,
        "last_sync": self.last_sync.isoformat() if self.last_sync else None,
        "last_sync_result": self.last_sync_result,
      },
      "created_at": self.created_at,
      "updated_at": self.updated_at,
      "deleted_at": self.deleted_at,
    }
