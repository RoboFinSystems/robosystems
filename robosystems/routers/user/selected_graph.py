"""The user's selected graph: the one the apps open on their next load."""

from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from ...database import get_db_session
from ...logger import logger
from ...middleware.auth.dependencies import get_current_user
from ...middleware.otel.metrics import endpoint_metrics_decorator
from ...middleware.rate_limits import user_management_rate_limit_dependency
from ...models.api.common import (
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from ...models.api.user import SetSelectedGraphRequest
from ...models.core import User
from ...operations.graph import selection

router = APIRouter(tags=["User"])


@router.put(
  "/user/selected-graph",
  response_model=SuccessResponse,
  summary="Set Selected Graph",
  description=(
    "Remembers the graph as the caller's current one: `GET /v1/graphs` then "
    "reports it as `selectedGraphId`, and the apps open on it. One graph per "
    "user, replacing the previous selection. Only a graph the caller belongs "
    "to can be selected; shared repositories cannot be."
  ),
  operation_id="setSelectedGraph",
  responses={**RESOURCE_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/user/selected-graph", business_event_type="graph_selected"
)
async def set_selected_graph(
  request: SetSelectedGraphRequest,
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> SuccessResponse:
  try:
    selection.select_graph(current_user.id, request.graph_id, db)
  except selection.GraphNotAccessible:
    raise create_error_response(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Access denied to this graph",
      code=ErrorCode.FORBIDDEN,
    )
  except selection.GraphNotFound:
    raise create_error_response(
      status_code=status.HTTP_404_NOT_FOUND,
      detail="Graph not found",
      code=ErrorCode.NOT_FOUND,
    )
  except Exception as e:
    logger.error(f"Error selecting graph: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error selecting graph",
      code=ErrorCode.INTERNAL_ERROR,
    )

  return SuccessResponse(
    success=True,
    message="Graph selected successfully",
    data={"selectedGraphId": request.graph_id},
  )
