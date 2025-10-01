"""Schema extensions endpoint."""

from fastapi import APIRouter, Depends, HTTPException, status, Path
from typing import Dict, Any
from sqlalchemy.orm import Session

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.middleware.auth.dependencies import get_current_user
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.schemas.manager import SchemaManager
from robosystems.database import get_db_session

router = APIRouter()


@router.get(
  "/extensions",
  operation_id="listSchemaExtensions",
  summary="List Available Schema Extensions",
  description="Get list of available schema extensions and compatibility groups",
  status_code=status.HTTP_200_OK,
  response_model=Dict[str, Any],
)
async def list_schema_extensions(
  graph_id: str = Path(..., description="The graph ID to list extensions for"),
  current_user: User = Depends(get_current_user),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> Dict[str, Any]:
  """
  List available schema extensions.

  Returns information about all available schema extensions that can be
  used when creating graphs.
  """
  try:
    # Verify user has access to the graph (handle both user graphs and shared repositories)
    from robosystems.models.iam import UserGraph
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils
    from robosystems.models.iam.user_repository import UserRepository

    # Determine graph type and validate access accordingly
    identity = MultiTenantUtils.get_graph_identity(graph_id)

    if identity.is_shared_repository:
      # Check shared repository access
      if not UserRepository.user_has_access(str(current_user.id), graph_id, db):
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to shared repository {graph_id}",
        )
    elif identity.is_user_graph:
      # Check user graph access
      user_graph = (
        db.query(UserGraph)
        .filter_by(user_id=current_user.id, graph_id=graph_id)
        .first()
      )
      if not user_graph:
        raise HTTPException(
          status_code=status.HTTP_403_FORBIDDEN,
          detail=f"Access denied to user graph {graph_id}",
        )
    else:
      # Unknown graph type
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid graph identifier: {graph_id}",
      )

    manager = SchemaManager()
    extensions = manager.list_available_extensions()

    # Enhance RoboLedger extension info with context-aware details
    for ext in extensions:
      if ext.get("name") == "roboledger":
        ext["display_name"] = "RoboLedger - Accounting & Financial Reporting"
        ext["description"] = (
          "Complete accounting system with XBRL reporting and GL transactions. "
          "Context-aware: SEC repositories automatically get reporting-only tables "
          "(9 additional nodes), while entity graphs get full accounting capabilities "
          "(14 additional nodes including Account, Transaction, LineItem, etc.)."
        )
        # Add context information
        ext["context_aware"] = True
        ext["contexts"] = {
          "sec_repository": {
            "description": "Reporting-only mode for SEC shared repository",
            "node_count": 9,
            "includes": [
              "Report",
              "Element",
              "Fact",
              "Entity",
              "Unit",
              "Period",
              "Structure",
              "Label",
              "Taxonomy",
            ],
            "excludes": ["Account", "Transaction", "LineItem", "Process", "Disclosure"],
          },
          "full_accounting": {
            "description": "Complete accounting system for entity graphs",
            "node_count": 14,
            "includes": [
              "All reporting nodes",
              "Account",
              "Transaction",
              "LineItem",
              "Process",
              "Disclosure",
            ],
          },
        }

    # Get compatibility groups
    groups = manager.get_optimal_schema_groups()

    return {
      "extensions": extensions,
      "compatibility_groups": groups,
      "description": "Schema extensions can be combined when creating graphs. "
      "RoboLedger is context-aware and automatically adapts based on usage. "
      "Compatibility groups show recommended combinations.",
    }

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to list schema extensions: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list extensions: {str(e)}",
    )
