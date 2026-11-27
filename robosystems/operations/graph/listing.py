"""The graphs and shared repositories a user can reach."""

from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from robosystems.models.api.user import GraphInfo
from robosystems.models.core import Graph, GraphUser, OrgRole, OrgUser


@dataclass
class UserGraphListing:
  graphs: list[GraphInfo] = field(default_factory=list)
  selected_graph_id: str | None = None
  admin_graphs: int = 0
  member_graphs: int = 0
  repository_count: int = 0


def _label_fields(graph: Graph | None) -> tuple[str, list[str]]:
  """``(description, tags)`` off a graph; a repository row's graph can be None."""
  if graph is None:
    return "", []
  return graph.description, graph.tags


def list_user_graphs(user_id: str, session: Session) -> UserGraphListing:
  """Every graph, subgraph and shared repository the user can open.

  Membership graphs with their subgraphs, org-owned graphs held through an
  org owner or admin role, then repositories with a live grant. Deprovisioned
  graphs are left out.
  """
  from robosystems.models.core.user.user_repository import UserRepository

  listing = UserGraphListing()
  graphs = listing.graphs
  listed_graph_ids: set[str] = set()

  for user_graph in GraphUser.get_by_user_id(user_id, session):
    # A subgraph has no grant of its own (access is the parent's), so a row
    # on a subgraph id is stale; the subgraph is listed under its parent.
    if user_graph.graph.is_subgraph:
      continue

    graph_status = user_graph.graph.status or "active"
    if graph_status == "deprovisioned":
      continue

    if user_graph.is_selected:
      listing.selected_graph_id = user_graph.graph_id

    if user_graph.role == "admin":
      listing.admin_graphs += 1
    else:
      listing.member_graphs += 1

    listed_graph_ids.add(user_graph.graph_id)
    description, tags = _label_fields(user_graph.graph)
    graphs.append(
      GraphInfo(
        graphId=user_graph.graph_id,
        graphName=user_graph.graph.graph_name,
        description=description,
        tags=tags,
        role=user_graph.role,
        isSelected=user_graph.is_selected,
        createdAt=user_graph.created_at.isoformat(),
        isRepository=False,
        repositoryType=None,
        schemaExtensions=user_graph.graph.schema_extensions or [],
        isSubgraph=False,
        parentGraphId=None,
        graphType=user_graph.graph.graph_type,
        status=graph_status,
      )
    )

    for subgraph in Graph.get_subgraphs(user_graph.graph_id, session):
      subgraph_status = subgraph.status or "active"
      if subgraph_status == "deprovisioned":
        continue
      if user_graph.role == "admin":
        listing.admin_graphs += 1
      else:
        listing.member_graphs += 1
      listed_graph_ids.add(subgraph.graph_id)
      description, tags = _label_fields(subgraph)
      graphs.append(
        GraphInfo(
          graphId=subgraph.graph_id,
          graphName=subgraph.graph_name,
          description=description,
          tags=tags,
          role=user_graph.role,
          isSelected=False,
          createdAt=subgraph.created_at.isoformat(),
          isRepository=False,
          repositoryType=None,
          schemaExtensions=subgraph.schema_extensions or [],
          isSubgraph=True,
          parentGraphId=subgraph.parent_graph_id,
          graphType=subgraph.graph_type,
          status=subgraph_status,
        )
      )

  # Org-owned graphs held implicitly via org owner/admin (no GraphUser row).
  admin_org_ids = [
    ou.org_id
    for ou in OrgUser.get_user_orgs(user_id, session)
    if ou.role in (OrgRole.OWNER, OrgRole.ADMIN)
  ]
  if admin_org_ids:
    implicit_graphs = (
      session.query(Graph)
      .filter(
        Graph.org_id.in_(admin_org_ids),
        Graph.graph_id.notin_(listed_graph_ids)
        if listed_graph_ids
        else Graph.graph_id.isnot(None),
      )
      .all()
    )
    for graph in implicit_graphs:
      graph_status = graph.status or "active"
      if graph_status == "deprovisioned":
        continue

      listing.admin_graphs += 1
      description, tags = _label_fields(graph)
      graphs.append(
        GraphInfo(
          graphId=graph.graph_id,
          graphName=graph.graph_name,
          description=description,
          tags=tags,
          role="admin",
          isSelected=False,
          createdAt=graph.created_at.isoformat(),
          isRepository=False,
          repositoryType=None,
          schemaExtensions=graph.schema_extensions or [],
          isSubgraph=graph.is_subgraph or False,
          parentGraphId=graph.parent_graph_id,
          graphType=graph.graph_type,
          status=graph_status,
        )
      )

  # An expired grant (a lapsed trial) no longer opens the repository.
  # Shared repositories cannot be selected.
  for user_repo in UserRepository.get_user_repositories(
    user_id, session, active_only=True
  ):
    if user_repo.is_expired():
      continue
    listing.repository_count += 1

    graph_name = (
      user_repo.graph.graph_name
      if user_repo.graph
      else user_repo.repository_name.upper()
    )

    description, tags = _label_fields(user_repo.graph)
    graphs.append(
      GraphInfo(
        graphId=user_repo.repository_name,
        graphName=graph_name,
        description=description,
        tags=tags,
        role=user_repo.access_level.value,
        isSelected=False,
        createdAt=user_repo.created_at.isoformat(),
        isRepository=True,
        repositoryType=user_repo.repository_type,
        schemaExtensions=user_repo.graph.schema_extensions if user_repo.graph else [],
        isSubgraph=False,  # Repositories are never subgraphs
        parentGraphId=None,
        graphType=user_repo.graph.graph_type if user_repo.graph else "repository",
      )
    )

  return listing
