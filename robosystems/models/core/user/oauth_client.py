"""OAuth 2.1 client registrations for the MCP authorization server.

A row is an MCP client that may ask a user for consent: registered
dynamically (RFC 7591 — an IDE or a gateway registering itself), resolved
from a Client ID Metadata Document (an HTTPS ``client_id`` URL whose document
we fetched and cached), or pre-registered by an operator (the Connectors
Directory's held credentials, an enterprise gateway's confidential client).

The client never holds tenant scope. Scope lives on ``OAuthGrant`` — one row
per user consent — so a client_id shared by every user of a directory
connector cannot widen anyone's access.
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Optional, cast

from sqlalchemy import Boolean, Column, DateTime, Index, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.config.constants import OAUTH_DCR_UNUSED_REGISTRATION_TTL_HOURS
from robosystems.database import Model
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.utils.ulid import generate_prefixed_ulid

REGISTRATION_DCR = "dcr"
REGISTRATION_CIMD = "cimd"
REGISTRATION_PREREGISTERED = "preregistered"

AUTH_METHOD_NONE = "none"
AUTH_METHOD_SECRET_POST = "client_secret_post"
AUTH_METHOD_SECRET_BASIC = "client_secret_basic"
SUPPORTED_AUTH_METHODS = frozenset(
  {AUTH_METHOD_NONE, AUTH_METHOD_SECRET_POST, AUTH_METHOD_SECRET_BASIC}
)


class OAuthClient(Model):
  """A registered OAuth client (MCP client application)."""

  __tablename__ = "oauth_clients"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("oac"))
  # The wire identifier. Opaque for DCR/pre-registered clients; the metadata
  # document URL itself for CIMD clients.
  client_id = Column(String, nullable=False, unique=True, index=True)
  # SHA-256 of the client secret; NULL for public clients. Secrets are
  # generated at full entropy, so a KDF buys nothing over a plain digest.
  client_secret_hash = Column(String(64), nullable=True)
  client_name = Column(String, nullable=False)
  redirect_uris = Column(JSONB, nullable=False)
  registration_source = Column(String(20), nullable=False)
  token_endpoint_auth_method = Column(
    String(32), nullable=False, default=AUTH_METHOD_NONE
  )
  client_uri = Column(String, nullable=True)
  logo_uri = Column(String, nullable=True)
  scope = Column(String, nullable=True)
  # Trusted clients (operator pre-registered, or CIMD hosts on the allowlist)
  # render on the consent page without the unknown-client hostname warning.
  is_trusted = Column(Boolean, default=False, nullable=False)
  is_active = Column(Boolean, default=True, nullable=False)
  registration_ip = Column(String(45), nullable=True)
  # Dynamic registrations that never complete a consent expire; the first
  # grant clears this. NULL = never expires.
  expires_at = Column(DateTime, nullable=True)
  last_used_at = Column(DateTime, nullable=True)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  __table_args__ = (
    Index("idx_oauth_clients_source_created", "registration_source", "created_at"),
  )

  def __repr__(self) -> str:
    return (
      f"<OAuthClient {self.id} {self.client_name} source={self.registration_source}>"
    )

  @property
  def is_confidential(self) -> bool:
    return self.client_secret_hash is not None

  @property
  def is_usable(self) -> bool:
    """Active and not past a registration expiry."""
    if not self.is_active:
      return False
    expires_at = self.expires_at
    if expires_at is not None:
      if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
      if datetime.now(UTC) > expires_at:
        return False
    return True

  @staticmethod
  def hash_secret(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()

  def verify_secret(self, presented: str | None) -> bool:
    """Constant-time secret check. Public clients never verify (no secret)."""
    if self.client_secret_hash is None or not presented:
      return False
    return secrets.compare_digest(
      self.hash_secret(presented), str(self.client_secret_hash)
    )

  @classmethod
  def get_by_client_id(
    cls, client_id: str, session: Session
  ) -> Optional["OAuthClient"]:
    if not client_id:
      return None
    return session.query(cls).filter(cls.client_id == client_id).first()

  @classmethod
  def get_by_id(cls, row_id: str, session: Session) -> Optional["OAuthClient"]:
    return session.query(cls).filter(cls.id == row_id).first()

  @classmethod
  def count_recent_dynamic_registrations(
    cls, registration_ip: str, window: timedelta, session: Session
  ) -> int:
    """Dynamic registrations from one address inside ``window`` — the
    per-IP cap the registration endpoint enforces on top of its rate limit."""
    since = datetime.now(UTC) - window
    return (
      session.query(cls)
      .filter(
        cls.registration_source == REGISTRATION_DCR,
        cls.registration_ip == registration_ip,
        cls.created_at >= since,
      )
      .count()
    )

  @classmethod
  def register_dynamic(
    cls,
    *,
    client_name: str,
    redirect_uris: list[str],
    token_endpoint_auth_method: str,
    session: Session,
    client_uri: str | None = None,
    logo_uri: str | None = None,
    scope: str | None = None,
    registration_ip: str | None = None,
  ) -> tuple["OAuthClient", str | None]:
    """RFC 7591 registration. Returns ``(row, client_secret_or_None)``.

    The caller has already validated the metadata (redirect URIs, auth
    method) — this only persists it. Registrations expire unused after
    ``OAUTH_DCR_UNUSED_REGISTRATION_TTL_HOURS``; the first consent clears the
    expiry (see ``mark_used``).
    """
    client_secret: str | None = None
    secret_hash: str | None = None
    if token_endpoint_auth_method != AUTH_METHOD_NONE:
      client_secret = f"rfsos{secrets.token_hex(32)}"
      secret_hash = cls.hash_secret(client_secret)

    client = cls(
      client_id=f"rfsoc_{secrets.token_urlsafe(24)}",
      client_secret_hash=secret_hash,
      client_name=client_name,
      redirect_uris=list(redirect_uris),
      registration_source=REGISTRATION_DCR,
      token_endpoint_auth_method=token_endpoint_auth_method,
      client_uri=client_uri,
      logo_uri=logo_uri,
      scope=scope,
      is_trusted=False,
      registration_ip=registration_ip,
      expires_at=datetime.now(UTC)
      + timedelta(hours=OAUTH_DCR_UNUSED_REGISTRATION_TTL_HOURS),
    )
    session.add(client)
    try:
      session.commit()
      session.refresh(client)
    except SQLAlchemyError:
      session.rollback()
      raise

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      details={
        "action": "oauth_client_registered",
        "oauth_client_id": client.id,
        "registration_source": REGISTRATION_DCR,
        "client_name": client_name,
        "redirect_uri_count": len(redirect_uris),
      },
      risk_level="low",
    )
    return client, client_secret

  @classmethod
  def register_preregistered(
    cls,
    *,
    client_name: str,
    redirect_uris: list[str],
    confidential: bool,
    session: Session,
    client_uri: str | None = None,
    logo_uri: str | None = None,
  ) -> tuple["OAuthClient", str | None]:
    """Operator-minted client (admin CLI). Trusted, never expires.

    Returns ``(row, client_secret_or_None)`` — the secret prints once for
    the operator to hand to the client (Anthropic's held credentials, an
    enterprise gateway) and is never recoverable.
    """
    client_secret: str | None = None
    secret_hash: str | None = None
    auth_method = AUTH_METHOD_NONE
    if confidential:
      client_secret = f"rfsos{secrets.token_hex(32)}"
      secret_hash = cls.hash_secret(client_secret)
      auth_method = AUTH_METHOD_SECRET_POST

    client = cls(
      client_id=f"rfsoc_{secrets.token_urlsafe(24)}",
      client_secret_hash=secret_hash,
      client_name=client_name,
      redirect_uris=list(redirect_uris),
      registration_source=REGISTRATION_PREREGISTERED,
      token_endpoint_auth_method=auth_method,
      client_uri=client_uri,
      logo_uri=logo_uri,
      is_trusted=True,
      expires_at=None,
    )
    session.add(client)
    try:
      session.commit()
      session.refresh(client)
    except SQLAlchemyError:
      session.rollback()
      raise

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.ADMIN_ACTION,
      details={
        "action": "oauth_client_preregistered",
        "oauth_client_id": client.id,
        "client_name": client_name,
        "confidential": confidential,
      },
      risk_level="low",
    )
    return client, client_secret

  @classmethod
  def upsert_cimd(
    cls,
    *,
    client_id: str,
    client_name: str,
    redirect_uris: list[str],
    session: Session,
    is_trusted: bool,
    client_uri: str | None = None,
    logo_uri: str | None = None,
    scope: str | None = None,
  ) -> "OAuthClient":
    """Mirror a validated Client ID Metadata Document into a row, updating
    the mirror when the document changed. Never expires (the document is
    the registration); ``is_active`` is left alone so an operator
    deactivation sticks across document refreshes."""
    client = cls.get_by_client_id(client_id, session)
    if client is None:
      client = cls(
        client_id=client_id,
        client_secret_hash=None,
        client_name=client_name,
        redirect_uris=list(redirect_uris),
        registration_source=REGISTRATION_CIMD,
        token_endpoint_auth_method=AUTH_METHOD_NONE,
        client_uri=client_uri,
        logo_uri=logo_uri,
        scope=scope,
        is_trusted=is_trusted,
        expires_at=None,
      )
      session.add(client)
      created = True
    else:
      created = False
      client.client_name = client_name
      client.redirect_uris = list(redirect_uris)
      client.client_uri = client_uri
      client.logo_uri = logo_uri
      client.scope = scope
      client.is_trusted = is_trusted
      client.expires_at = None
    try:
      session.commit()
      session.refresh(client)
    except IntegrityError:
      # Two first contacts with the same document raced on the unique
      # client_id; the loser re-reads the winner's row and updates it.
      session.rollback()
      if not created or cls.get_by_client_id(client_id, session) is None:
        raise
      return cls.upsert_cimd(
        client_id=client_id,
        client_name=client_name,
        redirect_uris=redirect_uris,
        session=session,
        is_trusted=is_trusted,
        client_uri=client_uri,
        logo_uri=logo_uri,
        scope=scope,
      )
    except SQLAlchemyError:
      session.rollback()
      raise
    if created:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.AUTH_SUCCESS,
        details={
          "action": "oauth_client_registered",
          "oauth_client_id": client.id,
          "registration_source": REGISTRATION_CIMD,
          "client_name": client_name,
          "trusted": is_trusted,
        },
        risk_level="low",
      )
    return client

  @classmethod
  def cleanup_expired_registrations(cls, session: Session) -> int:
    """Delete dynamic registrations that expired without a consent. The
    first consent clears ``expires_at`` (``mark_used``), so no row that
    holds a grant is ever eligible."""
    now = datetime.now(UTC)
    # The attribute reads as Optional to the type checker because mark_used
    # assigns None to it; the class-level column itself never is.
    expires_at = cast(Column[datetime | None], cls.expires_at)
    try:
      count = (
        session.query(cls)
        .filter(
          cls.registration_source == REGISTRATION_DCR,
          expires_at.isnot(None),
          expires_at < now,
        )
        .delete(synchronize_session=False)
      )
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
    if count:
      logger.info(f"Cleaned up {count} expired OAuth client registrations")
    return count

  def mark_used(self, session: Session) -> None:
    """Stamp ``last_used_at`` and clear a dynamic registration's expiry —
    a client that completed a consent is no longer an unused registration."""
    self.last_used_at = datetime.now(UTC)
    self.expires_at = None
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def deactivate(self, session: Session) -> tuple[int, int]:
    """Deactivate the client: no new consents, and every live grant and
    token it holds is revoked now — not at the next validation-cache miss.
    Returns ``(grants_revoked, tokens_revoked)``."""
    from .oauth_grant import OAuthGrant

    self.is_active = False
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
    grants, tokens = OAuthGrant.revoke_all_for_client(
      str(self.id), session, reason="client_deactivated"
    )
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.ADMIN_ACTION,
      details={
        "action": "oauth_client_deactivated",
        "oauth_client_id": self.id,
        "grants_revoked": grants,
        "tokens_revoked": tokens,
      },
      risk_level="low",
    )
    return grants, tokens
