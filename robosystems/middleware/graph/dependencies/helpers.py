"""Helper dependency functions.

Common FastAPI dependencies for entity and graph requirements.
"""

from fastapi import Depends, HTTPException, status

from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.models.core import User


def require_entity(
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """The caller's selected entity, or 400 when none is selected."""
  selected_entity = getattr(current_user, "selected_entity", None)
  if not selected_entity:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No entity selected. Please select a entity first.",
    )

  return selected_entity


def optional_entity(
  current_user: User = Depends(get_current_user_with_graph),
) -> str | None:
  """Optionally get the user's selected entity."""
  return getattr(current_user, "selected_entity", None)


def require_user_graph(
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """The caller's selected graph, or 400 when none is selected or it is a
  shared repository rather than a user graph."""
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No graph selected. Please select a graph first.",
    )

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if not identity.is_user_graph:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Selected graph {graph_id} is not a user graph",
    )

  return graph_id


def optional_user_graph(
  current_user: User = Depends(get_current_user_with_graph),
) -> str | None:
  """Optionally get the user's selected graph if it's a user graph."""
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    return None

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  return graph_id if identity.is_user_graph else None


def require_graph_category(
  category: str,
  current_user: User = Depends(get_current_user_with_graph),
) -> str:
  """The caller's selected graph, or 400 when it is not of `category`."""
  graph_id = getattr(current_user, "selected_graph", None)
  if not graph_id:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="No graph selected. Please select a graph first.",
    )

  identity = MultiTenantUtils.get_graph_identity(graph_id)
  if identity.category.value != category:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"Selected graph must be of category '{category}', but got '{identity.category.value}'",
    )

  return graph_id
