"""One user consent to one MCP client for one graph at one resource URL.

The grant is the tenant scope of every token minted from it. A directory
connector's shared ``client_id`` is worthless on its own: what an access
token may reach is exactly this row's ``graph_id``, intersected with the
user's live role at every call (``validate_mcp_access``), and only at the
route the row's ``resource`` names.
"""

from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import Column, DateTime, ForeignKey, Index, String
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, relationship

from robosystems.database import Model
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.utils.ulid import generate_prefixed_ulid


class OAuthGrant(Model):
  """A consent record: user x client x graph x resource."""

  __tablename__ = "oauth_grants"

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("oag"))
  user_id = Column(String, ForeignKey("users.id"), nullable=False)
  oauth_client_id = Column(
    String, ForeignKey("oauth_clients.id"), nullable=False, index=True
  )
  # The one graph this consent covers. The consent page is where it is
  # chosen (the picker on /v1/mcp; fixed by the URL on a per-graph route).
  graph_id = Column(String, nullable=False, index=True)
  # Canonical resource URL the tokens are bound to — audience. A token
  # presented at any other route is refused.
  resource = Column(String, nullable=False)
  scope = Column(String, nullable=False)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  last_used_at = Column(DateTime, nullable=True)
  revoked_at = Column(DateTime, nullable=True)

  user = relationship("User")
  client = relationship("OAuthClient")

  __table_args__ = (
    Index("idx_oauth_grants_user_client", "user_id", "oauth_client_id"),
    Index("idx_oauth_grants_user_revoked", "user_id", "revoked_at"),
  )

  def __repr__(self) -> str:
    return f"<OAuthGrant {self.id} user={self.user_id} graph={self.graph_id}>"

  @property
  def is_revoked(self) -> bool:
    return self.revoked_at is not None

  @classmethod
  def create(
    cls,
    *,
    user_id: str,
    oauth_client_id: str,
    graph_id: str,
    resource: str,
    scope: str,
    session: Session,
  ) -> "OAuthGrant":
    grant = cls(
      user_id=user_id,
      oauth_client_id=oauth_client_id,
      graph_id=graph_id,
      resource=resource,
      scope=scope,
    )
    session.add(grant)
    try:
      session.commit()
      session.refresh(grant)
    except SQLAlchemyError:
      session.rollback()
      raise

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_SUCCESS,
      user_id=user_id,
      details={
        "action": "oauth_consent_granted",
        "oauth_grant_id": grant.id,
        "oauth_client_id": oauth_client_id,
        "graph_id": graph_id,
        "resource": resource,
        "scope": scope,
      },
      risk_level="low",
    )
    return grant

  @classmethod
  def get_by_id(cls, grant_id: str, session: Session) -> Optional["OAuthGrant"]:
    return session.query(cls).filter(cls.id == grant_id).first()

  @classmethod
  def get_active_by_user_id(cls, user_id: str, session: Session) -> list["OAuthGrant"]:
    return (
      session.query(cls)
      .filter(cls.user_id == user_id, cls.revoked_at.is_(None))
      .order_by(cls.created_at.desc())
      .all()
    )

  @classmethod
  def revoke_all_for_client(
    cls, oauth_client_id: str, session: Session, *, reason: str
  ) -> tuple[int, int]:
    """Revoke every live grant for a client and the tokens minted from
    them. Returns ``(grants_revoked, tokens_revoked)``."""
    grants = (
      session.query(cls)
      .filter(cls.oauth_client_id == oauth_client_id, cls.revoked_at.is_(None))
      .all()
    )
    tokens = 0
    for grant in grants:
      tokens += grant.revoke(session, reason=reason)
    return len(grants), tokens

  def touch(self, session: Session) -> None:
    self.last_used_at = datetime.now(UTC)
    try:
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise

  def revoke(self, session: Session, *, reason: str = "user_revoked") -> int:
    """Revoke the grant and every token minted from it. Returns the number
    of tokens revoked (their validation-cache entries are cleared too)."""
    from .oauth_token import OAuthToken

    if self.revoked_at is None:
      self.revoked_at = datetime.now(UTC)
      try:
        session.commit()
      except SQLAlchemyError:
        session.rollback()
        raise

    revoked = OAuthToken.revoke_for_grant(str(self.id), session)
    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTHORIZATION_DENIED,
      user_id=str(self.user_id),
      details={
        "action": "oauth_grant_revoked",
        "oauth_grant_id": self.id,
        "oauth_client_id": self.oauth_client_id,
        "graph_id": self.graph_id,
        "tokens_revoked": revoked,
        "reason": reason,
      },
      risk_level="low",
    )
    return revoked
