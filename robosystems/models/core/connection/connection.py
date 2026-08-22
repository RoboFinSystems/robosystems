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
from sqlalchemy.dialects.postgresql import JSONB
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
  """Connection source-of-truth (write) policy.

  Governs whether RoboSystems-originated entries (manual JE, schedule
  drafts) flow into the source-of-truth system on the way to posted GL.

  - ``NATIVE``: RoboSystems IS the source of truth. RL-originated events
    write GL rows locally on dispatch; no outbound publish. The
    connection's inbound sync (if any) captures-to-inbox.
  - ``QB_AUTHORITATIVE``: QuickBooks IS the source of truth. RL-originated
    events publish to QB via ``execute-event-block``; local GL holds as
    DRAFT until QB accepts. Inbound QB sync auto-commits. The
    cross-source matcher recognises round-tripped entries by
    ``metadata.qb_external_id`` and skips re-creation.
  - ``HYBRID``: QB authoritative with exception-flagging heuristics
    (low-confidence mapping, manual JE source class, amount-over-
    threshold). Currently only NATIVE + QB_AUTHORITATIVE are wired;
    HYBRID is reserved and not yet implemented.
  """

  NATIVE = "native"
  QB_AUTHORITATIVE = "qb_authoritative"
  HYBRID = "hybrid"


# Per-provider default for the OUTBOUND write-back policy. An active
# QuickBooks connection is authoritative by nature — QB is the general
# ledger, so RoboLedger-originated entries (manual JEs, schedule drafts)
# write back to it. There is no steady state where you keep QB connected
# but deliberately diverge the two ledgers. Other providers have no
# external GL to write to (SEC repos; the future Plaid/native-accounting
# world), so they stay NATIVE. `native` therefore describes a graph with no
# active authoritative external connection — pre-connect, post-disconnect
# (the connection's events lose their connection_id and `execute_event_block`
# no-ops), or native-accounting — NOT a "connected-but-don't-write" choice.
# Inbound sync-down is decoupled from this and mirrors QB regardless.
_PROVIDER_WRITE_POLICY_DEFAULTS: dict[str, str] = {
  "quickbooks": WritePolicy.QB_AUTHORITATIVE.value,
}


def default_write_policy_for_provider(provider: str) -> str:
  """Resolve the default outbound write policy for a newly-created
  connection of ``provider``. QuickBooks → ``qb_authoritative``; everything
  else → ``native`` (the column's server_default)."""
  return _PROVIDER_WRITE_POLICY_DEFAULTS.get(provider, WritePolicy.NATIVE.value)


class Connection(Model):
  """Data source connection metadata."""

  __tablename__ = "connections"
  __table_args__ = (
    Index("idx_connections_graph", "graph_id"),
    Index("idx_connections_user", "user_id"),
    Index("idx_connections_provider", "provider"),
    Index("idx_connections_graph_provider", "graph_id", "provider"),
    # The re-OAuth reuse path queries this index to find a
    # soft-deleted connection for a freshly-OAuthed realm. Partial because
    # only soft-deleted rows are interesting to that query.
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
  # External-provider registered source slug — the value the integration
  # stamps on everything it writes (Event.source). The registration IS the
  # allow-list entry the event-source validation reads; the platform runs
  # nothing and holds no credentials for external sources.
  source_name = Column(String, nullable=True)

  # Sync tracking
  auto_sync_enabled = Column(Boolean, default=True, nullable=False)
  last_sync = Column(DateTime, nullable=True)
  # Outcome summary of the most recent sync ATTEMPT (success or failure):
  # status, window, per-category counts (captured / updated / drift /
  # dispatch_failed / …), truncated errors. This is what lets the surface
  # that triggered the sync — MCP operator, frontend — answer "did it
  # finish, and what did it do?" without CloudWatch. Written on success
  # alongside `last_sync`; on failure alone (`last_sync` only ever advances
  # on success — the close gate's sync-current check reads it).
  last_sync_result = Column(JSONB, nullable=True)

  # Source-of-truth policy. Default `'native'` means no outbound writes
  # without explicit operator opt-in via UI / API. The loader's
  # auto-commit branch reads this column.
  write_policy = Column(
    String, default=WritePolicy.NATIVE.value, server_default="native", nullable=False
  )

  # CDC watermark. The loader advances this ONLY after a
  # successful batch commit; a mid-batch failure replays from the prior
  # value on the next run (combined with SyncToken-gated UPSERT, replay
  # of already-ingested rows is a no-op). NULL means "no CDC sync has
  # succeeded yet for this connection" — extract layer falls back to
  # full-window lookback or `full_rebuild=True` semantics until the
  # first successful CDC batch advances it. Separate from `last_sync`,
  # which is a display-facing "when did we last try" timestamp.
  last_cdc_watermark = Column(DateTime, nullable=True)

  # Soft-delete marker. When non-null the row is invisible to the
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
    """Get connection by ID.

    Soft-deleted rows (`deleted_at IS NOT NULL`) are filtered out by
    default — the only callers that need to see them are the OAuth
    re-OAuth reuse path and admin tooling.
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
    """Find a soft-deleted connection for re-OAuth reuse.

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
    """Persist a sync outcome WITHOUT advancing `last_sync`.

    The failure path: `last_sync` feeds the close gate's sync-current
    check, so it only ever advances on success — but the failed
    attempt's outcome must still be legible to the operator surface.
    """
    self.last_sync_result = result
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def advance_cdc_watermark(self, watermark: datetime, session: Session) -> None:
    """Advance the CDC watermark after a successful batch
    commit. Callers MUST only invoke this once load has succeeded; a
    mid-batch failure should leave the prior watermark in place so the
    next sync replays from there (combined with SyncToken-gated UPSERT,
    replay of already-ingested rows is a no-op).

    The watermark stores naive UTC (matches the other DateTime columns on
    this table). Callers passing a tz-aware datetime get it stripped.
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
    """Set the source-of-truth write policy (operator opt-in surface).

    This is the explicit operator opt-in `connection_service` references:
    flipping a connection to ``qb_authoritative`` is what enables outbound
    write-back via ``execute-event-block``. ``HYBRID`` is rejected until its
    code path ships (see `WritePolicy`).
    """
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
    """Hard-delete the connection row (admin purge only).

    Almost always the wrong choice — prefer ``soft_delete`` so the
    tenant-side events/agents/elements scoped to this connection_id
    don't orphan.
    """
    try:
      session.delete(self)
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def soft_delete(self, session: Session) -> None:
    """Soft-delete — mark the row deleted without removing it.

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
    """Revive a soft-deleted connection (re-OAuth reuse path)."""
    self.deleted_at = None
    self.updated_at = datetime.now(UTC)
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
