"""The caller's selected graph: the one the apps open on their next load."""

from sqlalchemy.orm import Session

from robosystems.models.core import GraphUser


class GraphNotAccessible(Exception):
  """The caller has no membership on the graph."""


class GraphNotFound(Exception):
  """The membership vanished between the access check and the write."""


def select_graph(user_id: str, graph_id: str, session: Session) -> None:
  """Make `graph_id` the caller's selected graph, replacing any other."""
  memberships = GraphUser.get_by_user_id(user_id, session)
  if graph_id not in {membership.graph_id for membership in memberships}:
    raise GraphNotAccessible(graph_id)
  if not GraphUser.set_selected_graph(user_id, graph_id, session):
    raise GraphNotFound(graph_id)
