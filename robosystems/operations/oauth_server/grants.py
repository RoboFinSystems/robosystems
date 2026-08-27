"""Connected apps: a user's OAuth grants — list and revoke.

A grant is one consent (user x client x graph x resource). Listing shows
what a user has connected and where each connection reaches; revoking a
grant kills every token minted from it and clears their validation-cache
entries, so the connector stops at its next call rather than at the next
cache miss. This is the proportionate answer to "disconnect this app" —
the alternatives are a password change (every grant) or an operator
deactivating the client (every user of it).
"""

from datetime import datetime

from sqlalchemy.orm import Session

from robosystems.models.api.user import OAuthGrantInfo
from robosystems.models.core import Graph, OAuthGrant


class GrantNotFound(Exception):
  """No live grant with that id belongs to the user. Deliberately one
  answer for "unknown" and "someone else's" — no existence oracle."""


def _iso(value: datetime | None) -> str | None:
  return value.isoformat() if value else None


def list_user_grants(user_id: str, session: Session) -> list[OAuthGrantInfo]:
  """Active grants only. A revoked grant cannot be reinstated from this
  surface, so listing it would only grow the list with rows the user can
  take no action on (the API-key listing follows the same rule)."""
  grants = OAuthGrant.get_active_by_user_id(user_id, session)
  graph_ids = {str(grant.graph_id) for grant in grants}
  graph_names: dict[str, str] = {}
  if graph_ids:
    # Straight off the table, not Graph.get_by_id: a grant on a graph that
    # has since been deprovisioned still deserves its name in the listing.
    for graph_id, graph_name in (
      session.query(Graph.graph_id, Graph.graph_name)
      .filter(Graph.graph_id.in_(graph_ids))
      .all()
    ):
      graph_names[str(graph_id)] = str(graph_name)

  out: list[OAuthGrantInfo] = []
  for grant in grants:
    client = grant.client
    out.append(
      OAuthGrantInfo(
        id=str(grant.id),
        client_name=str(client.client_name) if client else "Unknown client",
        client_uri=client.client_uri if client else None,
        client_is_trusted=bool(client.is_trusted) if client else False,
        graph_id=str(grant.graph_id),
        graph_name=graph_names.get(str(grant.graph_id)),
        resource=str(grant.resource),
        scope=str(grant.scope),
        created_at=grant.created_at.isoformat(),
        last_used_at=_iso(grant.last_used_at),
      )
    )
  return out


def revoke_user_grant(user_id: str, grant_id: str, session: Session) -> int:
  """Revoke one of the user's grants and every token minted from it.
  Returns the number of tokens revoked. Idempotent: revoking an already
  revoked grant re-asserts the token revocation and returns 0."""
  grant = OAuthGrant.get_by_id(grant_id, session)
  if grant is None or str(grant.user_id) != str(user_id):
    raise GrantNotFound(grant_id)
  return grant.revoke(session, reason="user_revoked")
