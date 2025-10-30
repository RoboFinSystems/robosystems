"""
Unified graph creation router.

This module handles creating new graph databases with flexible configurations,
optionally including initial entities like companies.
"""

from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from robosystems.logger import logger
from robosystems.models.iam import User, UserLimits, UserGraph
from robosystems.models.api.graph import (
  GraphMetadata,
  CustomSchemaDefinition,
)
from robosystems.models.api.user import (
  GraphInfo,
  UserGraphsResponse,
)
from robosystems.models.api.common import (
  ErrorResponse,
  SuccessResponse,
  ErrorCode,
  create_error_response,
)
from robosystems.middleware.sse import create_operation_response
from robosystems.models.api import AvailableExtensionsResponse, AvailableExtension
from robosystems.middleware.auth.dependencies import (
  get_current_user,
  get_current_user_with_graph,
)
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
  general_api_rate_limit_dependency,
  user_management_rate_limit_dependency,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.database import session


# Create router for unified graph creation
router = APIRouter(prefix="/v1/graphs", tags=["Graphs"])


class InitialEntityData(BaseModel):
  """Initial entity data for graph creation."""

  name: str = Field(..., min_length=1, max_length=255, description="Entity name")
  uri: str = Field(..., min_length=1, description="Entity website or URI")
  cik: Optional[str] = Field(None, description="CIK number for SEC filings")
  sic: Optional[str] = Field(None, description="SIC code")
  sic_description: Optional[str] = Field(None, description="SIC description")
  category: Optional[str] = Field(None, description="Business category")
  state_of_incorporation: Optional[str] = Field(
    None, description="State of incorporation"
  )
  fiscal_year_end: Optional[str] = Field(None, description="Fiscal year end (MMDD)")
  ein: Optional[str] = Field(None, description="Employer Identification Number")


class CreateGraphRequest(BaseModel):
  """Request model for creating a new graph."""

  # Core graph configuration
  metadata: GraphMetadata = Field(
    ..., description="Graph metadata including name, description, and schema extensions"
  )

  # Instance tier configuration
  instance_tier: str = Field(
    "kuzu-standard",
    description="Instance tier: kuzu-standard, kuzu-large, kuzu-xlarge, neo4j-community-large, neo4j-enterprise-xlarge",
    pattern="^(kuzu-standard|kuzu-large|kuzu-xlarge|neo4j-community-large|neo4j-enterprise-xlarge)$",
  )

  # Optional custom schema
  custom_schema: Optional[CustomSchemaDefinition] = Field(
    None,
    description="Custom schema definition to apply",
  )

  # Optional initial entity (for backward compatibility with entity-graph endpoint)
  initial_entity: Optional[InitialEntityData] = Field(
    None,
    description="Optional initial entity to create in the graph. If provided, creates a entity-focused graph.",
  )

  # Control whether to create entity node and upload initial data
  create_entity: bool = Field(
    default=True,
    description="Whether to create the entity node and upload initial data. Only applies when initial_entity is provided. Set to False to create graph without populating entity data (useful for file-based ingestion workflows).",
  )

  # Additional configuration
  tags: List[str] = Field(
    default_factory=list,
    description="Optional tags for organization",
    max_items=10,
  )  # type: ignore[call-arg]

  class Config:
    json_schema_extra = {
      "example": {
        "metadata": {
          "graph_name": "Acme Consulting LLC",
          "description": "Professional consulting services with full accounting integration",
          "schema_extensions": ["roboledger"],
        },
        "instance_tier": "kuzu-standard",
        "initial_entity": {
          "name": "Acme Consulting LLC",
          "uri": "https://acmeconsulting.com",
          "cik": "0001234567",
        },
        "tags": ["consulting", "professional-services"],
      }
    }


def _create_error_response(
  error_code: str,
  message: str,
  field: Optional[str] = None,
  details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
  """Create a standardized error response."""
  error_obj = {"code": error_code, "message": message}
  if field:
    error_obj["field"] = field
  if details:
    error_obj["details"] = details
  return {"error": error_obj}


def _raise_http_exception(
  status_code: int,
  error_code: str,
  message: str,
  field: Optional[str] = None,
  details: Optional[Dict[str, Any]] = None,
):
  """Raise an HTTPException with standardized error format."""
  raise HTTPException(
    status_code=status_code,
    detail=_create_error_response(error_code, message, field, details),
  )


@router.get(
  "",
  response_model=UserGraphsResponse,
  summary="Get User Graphs",
  description="""List all graph databases accessible to the current user with roles and selection status.

Returns a comprehensive list of all graphs the user can access, including their
role in each graph (admin or member) and which graph is currently selected as
the active workspace.

**Returned Information:**
- Graph ID and display name for each accessible graph
- User's role (admin/member) indicating permission level
- Selection status (one graph can be marked as "selected")
- Creation timestamp for each graph

**Graph Roles:**
- `admin`: Full access - can manage graph settings, invite users, delete graph
- `member`: Read/write access - can query and modify data, cannot manage settings

**Selected Graph Concept:**
The "selected" graph is the user's currently active workspace. Many API operations
default to the selected graph if no graph_id is provided. Users can change their
selected graph via the `POST /v1/graphs/{graph_id}/select` endpoint.

**Use Cases:**
- Display graph selector in UI
- Show user's accessible workspaces
- Identify which graph is currently active
- Filter graphs by role for permission-based features

**Empty Response:**
New users or users without graph access will receive an empty list with
`selectedGraphId: null`. Users should create a new graph or request access
to an existing graph.

**Note:**
Graph listing is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="getGraphs",
  responses={
    200: {
      "description": "Graphs retrieved successfully",
      "content": {
        "application/json": {
          "examples": {
            "with_graphs": {
              "summary": "User with multiple graphs",
              "value": {
                "graphs": [
                  {
                    "graphId": "kg1a2b3c4d5",
                    "graphName": "Acme Consulting LLC",
                    "role": "admin",
                    "isSelected": True,
                    "createdAt": "2024-01-15T10:00:00Z",
                  },
                  {
                    "graphId": "kg9z8y7x6w5",
                    "graphName": "TechCorp Enterprises",
                    "role": "member",
                    "isSelected": False,
                    "createdAt": "2024-02-20T14:30:00Z",
                  },
                ],
                "selectedGraphId": "kg1a2b3c4d5",
              },
            },
            "empty": {
              "summary": "New user without graphs",
              "value": {"graphs": [], "selectedGraphId": None},
            },
          }
        }
      },
    },
    500: {"description": "Error retrieving graphs"},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs", business_event_type="user_graphs_accessed"
)
async def get_graphs(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserGraphsResponse:
  """
  Get all graphs accessible to the current user.

  Args:
      current_user: The authenticated user from the API key

  Returns:
      UserGraphsResponse: List of graphs with selection status

  Raises:
      HTTPException: If there's an error retrieving the graphs
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Get all user-graph relationships
    user_graphs = UserGraph.get_by_user_id(current_user.id, session)

    # Find the selected graph
    selected_graph_id = None
    graphs = []
    admin_graphs = 0
    member_graphs = 0

    for user_graph in user_graphs:
      if user_graph.is_selected:
        selected_graph_id = user_graph.graph_id

      # Count roles
      if user_graph.role == "admin":
        admin_graphs += 1
      else:
        member_graphs += 1

      graphs.append(
        GraphInfo(
          graphId=user_graph.graph_id,
          graphName=user_graph.graph.graph_name,
          role=user_graph.role,
          isSelected=user_graph.is_selected,
          createdAt=user_graph.created_at.isoformat(),
        )
      )

    # Record business event for graphs access with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs",
      method="GET",
      event_type="user_graphs_accessed",
      event_data={
        "user_id": user_id,
        "total_graphs": len(graphs),
        "admin_graphs": admin_graphs,
        "member_graphs": member_graphs,
        "has_selected_graph": bool(selected_graph_id),
      },
      user_id=user_id,
    )

    return UserGraphsResponse(graphs=graphs, selectedGraphId=selected_graph_id)

  except Exception as e:
    logger.error(f"Error retrieving user graphs: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error retrieving user graphs",
      code=ErrorCode.INTERNAL_ERROR,
    )


@router.post(
  "",
  status_code=status.HTTP_202_ACCEPTED,
  operation_id="createGraph",
  summary="Create New Graph Database",
  description="""Create a new graph database with specified schema and optionally an initial entity.

This endpoint starts an asynchronous graph creation operation and returns
connection details for monitoring progress via Server-Sent Events (SSE).

**Operation Types:**
- **Generic Graph**: Creates empty graph with schema extensions
- **Entity Graph**: Creates graph with initial entity data

**Monitoring Progress:**
Use the returned `operation_id` to connect to the SSE stream:
```javascript
const eventSource = new EventSource('/v1/operations/{operation_id}/stream');
eventSource.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Progress:', data.progress_percent + '%');
};
```

**SSE Connection Limits:**
- Maximum 5 concurrent SSE connections per user
- Rate limited to 10 new connections per minute
- Automatic circuit breaker for Redis failures
- Graceful degradation if event system unavailable

**Events Emitted:**
- `operation_started`: Graph creation begins
- `operation_progress`: Schema loading, database setup, etc.
- `operation_completed`: Graph ready with connection details
- `operation_error`: Creation failed with error details

**Error Handling:**
- `429 Too Many Requests`: SSE connection limit exceeded
- `503 Service Unavailable`: SSE system temporarily disabled
- Clients should implement exponential backoff on errors

**Response includes:**
- `operation_id`: Unique identifier for monitoring
- `_links.stream`: SSE endpoint for real-time updates
- `_links.status`: Point-in-time status check endpoint""",
)
async def create_graph(
  request: CreateGraphRequest,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),  # noqa: ARG001
):
  """
  Create a new graph database asynchronously using SSE monitoring.

  This unified endpoint can:
  1. Create an empty graph with just schema (generic use case)
  2. Create a graph with an initial entity (entity-graph use case)
  3. Apply custom schemas
  4. Configure instance tiers

  Returns operation details for SSE monitoring instead of legacy task polling.
  """
  try:
    from robosystems.database import get_db_session
    from robosystems.models.iam.graph_credits import GraphTier

    # Get database session for user limits check
    db = next(get_db_session())

    try:
      # Check user's subscription tier and limits
      user_limits = UserLimits.get_by_user_id(current_user.id, db)
      if not user_limits:
        _raise_http_exception(
          status_code=status.HTTP_403_FORBIDDEN,
          error_code="user_limits_not_found",
          message="User limits not found. Please contact support.",
        )

      # Check if user can create more graphs
      can_create, reason = user_limits.can_create_user_graph(db)
      if not can_create:
        _raise_http_exception(
          status_code=status.HTTP_403_FORBIDDEN,
          error_code="graph_limit_reached",
          message=reason,
        )

      # Map tier to GraphTier enum
      tier_map = {
        "kuzu-standard": GraphTier.KUZU_STANDARD,
        "kuzu-large": GraphTier.KUZU_LARGE,
        "kuzu-xlarge": GraphTier.KUZU_XLARGE,
      }
      graph_tier = tier_map.get(request.instance_tier.lower(), GraphTier.KUZU_STANDARD)

      # Log the request
      logger.info("=== GRAPH CREATION REQUEST (SSE) ===")
      logger.info(f"User: {current_user.email} (ID: {current_user.id})")
      logger.info(f"Graph Name: {request.metadata.graph_name}")
      logger.info(f"Instance Tier: {request.instance_tier}")
      logger.info(f"Schema Extensions: {request.metadata.schema_extensions}")
      logger.info(f"Has Initial Entity: {request.initial_entity is not None}")
      logger.info(f"Has Custom Schema: {request.custom_schema is not None}")

      # Determine operation type and prepare data
      if request.initial_entity:
        # Entity-graph creation operation
        operation_type = "entity_graph_creation"
        logger.info("Using entity-graph creation workflow")

        # Prepare entity data with tier information
        operation_data = {
          "request_type": "entity_graph",
          "entity_data": {
            **request.initial_entity.model_dump(),
            "graph_tier": graph_tier.value,
            "subscription_tier": "standard",  # Default for tracking
            "extensions": request.metadata.schema_extensions,  # EntityCreate expects 'extensions' not 'schema_extensions'
            "graph_name": request.metadata.graph_name,
            "graph_description": request.metadata.description,
            "tags": request.tags or [],
            "create_entity": request.create_entity,
          },
        }

      else:
        # Generic graph creation operation
        operation_type = "graph_creation"
        logger.info("Using generic graph creation workflow")

        # Prepare task data
        operation_data = {
          "request_type": "generic_graph",
          "task_data": {
            "graph_id": None,  # Will be auto-generated with kg prefix
            "schema_extensions": request.metadata.schema_extensions,
            "metadata": request.metadata.model_dump(),
            "tier": request.instance_tier,
            "graph_tier": graph_tier.value,
            "initial_data": None,
            "user_id": str(current_user.id),
            "custom_schema": request.custom_schema.model_dump()
            if request.custom_schema
            else None,
            "tags": request.tags or [],
          },
        }

      # Create SSE operation instead of Celery task
      response = await create_operation_response(
        operation_type=operation_type,
        user_id=current_user.id,
        graph_id=None,  # Will be set when graph is created
      )

      # Queue the actual Celery task with operation_id for progress tracking
      operation_id = response["operation_id"]

      if request.initial_entity:
        from robosystems.tasks.graph_operations.create_entity_graph import (
          create_entity_with_new_graph_sse_task,
        )

        # Launch task with operation ID for SSE progress tracking
        task = create_entity_with_new_graph_sse_task.delay(  # type: ignore[attr-defined]
          operation_data["entity_data"], current_user.id, operation_id
        )

      else:
        from robosystems.tasks.graph_operations.create_graph import (
          create_graph_sse_task,
        )

        # Launch task with operation ID for SSE progress tracking
        task = create_graph_sse_task.delay(  # type: ignore[attr-defined]
          operation_data["task_data"], operation_id
        )

      logger.info(f"✓ Created SSE operation {operation_id} and queued task {task.id}")
      logger.info("=== END GRAPH CREATION REQUEST (SSE) ===")

      return response

    finally:
      db.close()

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create graph creation operation: {e}")
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create graph creation operation: {str(e)}",
    )


@router.get(
  "/extensions",
  response_model=AvailableExtensionsResponse,
  operation_id="getAvailableExtensions",
  summary="Get Available Schema Extensions",
  description="""List all available schema extensions for graph creation.

Schema extensions provide pre-built industry-specific data models that extend
the base graph schema with specialized nodes, relationships, and properties.

**Available Extensions:**
- **RoboLedger**: Complete accounting system with XBRL reporting, general ledger, and financial statements
- **RoboInvestor**: Investment portfolio management and tracking
- **RoboSCM**: Supply chain management and logistics
- **RoboFO**: Front office operations and CRM
- **RoboHRM**: Human resources management
- **RoboEPM**: Enterprise performance management
- **RoboReport**: Business intelligence and reporting

**Extension Information:**
Each extension includes:
- Display name and description
- Node and relationship counts
- Context-aware capabilities (e.g., SEC repositories get different features than entity graphs)

**Use Cases:**
- Browse available extensions before creating a graph
- Understand extension capabilities and data models
- Plan graph schema based on business requirements
- Combine multiple extensions for comprehensive data modeling

**Note:**
Extension listing is included - no credit consumption required.""",
  responses={
    200: {
      "description": "Extensions retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "extensions": [
              {
                "name": "roboledger",
                "description": "Complete accounting system with XBRL reporting and GL transactions",
                "enabled": False,
              },
              {
                "name": "roboinvestor",
                "description": "Investment portfolio management and tracking",
                "enabled": False,
              },
            ]
          }
        }
      },
    },
    500: {"description": "Failed to retrieve extensions"},
  },
)
async def get_available_extensions(
  _rate_limit: None = Depends(general_api_rate_limit_dependency),  # noqa: ARG001
):
  """
  Get available schema extensions for graph creation.

  Returns information about all available extensions, their descriptions,
  and recommended combinations for different use cases.
  """
  try:
    from robosystems.schemas.manager import SchemaManager

    manager = SchemaManager()
    extensions_info = manager.list_available_extensions()
    logger.info(f"Got {len(extensions_info)} extensions from schema manager")

    # Convert to response format
    available_extensions = []
    for ext_info in extensions_info:
      logger.debug(
        f"Extension {ext_info['name']}: available={ext_info.get('available', False)}"
      )
      if ext_info["available"]:
        # Get display names for extensions
        display_names = {
          "roboledger": "RoboLedger - Accounting & Financial Reporting",
          "roboinvestor": "RoboInvestor - Investment Management",
          "roboscm": "RoboSCM - Supply Chain Management",
          "robofo": "RoboFO - Front Office Operations",
          "robohrm": "RoboHRM - Human Resources Management",
          "roboepm": "RoboEPM - Enterprise Performance Management",
          "roboreport": "RoboReport - Business Intelligence & Reporting",
        }

        # Try to get actual node/relationship counts
        try:
          from robosystems.schemas.loader import (
            get_schema_loader,
            get_contextual_schema_loader,
          )

          # Use context-aware loading for RoboLedger to show accurate counts
          if ext_info["name"] == "roboledger":
            # For display purposes, show the full accounting context
            # which represents what entity graphs will get
            loader = get_contextual_schema_loader("application", "roboledger")
            # Override description with context-aware information
            description = (
              "Complete accounting system with XBRL reporting and GL transactions. "
              "Context-aware: SEC repositories get reporting-only tables (9 nodes), "
              "entity graphs get full accounting capabilities (12 nodes)."
            )
          else:
            loader = get_schema_loader([ext_info["name"]])
            description = ext_info["description"]  # Use original description

          node_count = len(loader.list_node_types()) - 8  # Subtract base nodes
          relationship_count = (
            len(loader.list_relationship_types()) - 12
          )  # Subtract base relationships
        except Exception:
          node_count = 0
          relationship_count = 0
          description = ext_info["description"]  # Fallback to original

        available_extensions.append(
          {
            "name": ext_info["name"],
            "display_name": display_names.get(
              ext_info["name"], ext_info["name"].title()
            ),
            "description": description,  # Use the correctly set description
            "node_count": node_count,
            "relationship_count": relationship_count,
          }
        )

    # Convert dictionaries to AvailableExtension objects
    extension_objects = [
      AvailableExtension(
        name=str(ext["name"]), description=str(ext["description"]), enabled=False
      )
      for ext in available_extensions
    ]

    return AvailableExtensionsResponse(
      extensions=extension_objects,
    )

  except Exception as e:
    # Fallback response if schema manager fails
    logger.error(f"Failed to load schema extensions: {e}")
    return AvailableExtensionsResponse(
      extensions=[
        AvailableExtension(
          name="roboledger",
          description=(
            "Complete accounting system with XBRL reporting and GL transactions. "
            "Context-aware: SEC repositories get reporting-only tables (9 nodes), "
            "entity graphs get full accounting capabilities (12 nodes)."
          ),
          enabled=False,
        ),
        AvailableExtension(
          name="roboinvestor",
          description="Investment portfolio management and tracking",
          enabled=False,
        ),
      ],
    )


@router.post(
  "/{graph_id}/select",
  response_model=SuccessResponse,
  summary="Select Graph",
  description="""Select a specific graph as the active workspace for the user.

The selected graph becomes the default context for operations in client applications
and can be used to maintain user workspace preferences across sessions.

**Functionality:**
- Sets the specified graph as the user's currently selected graph
- Deselects any previously selected graph (only one can be selected at a time)
- Persists selection across sessions until changed
- Returns confirmation with the selected graph ID

**Requirements:**
- User must have access to the graph (as admin or member)
- Graph must exist and not be deleted
- User can only select graphs they have permission to access

**Use Cases:**
- Switch between multiple graphs in a multi-graph environment
- Set default workspace after creating a new graph
- Restore user's preferred workspace on login
- Support graph context switching in client applications

**Client Integration:**
Many client operations can default to the selected graph, simplifying API calls
by eliminating the need to specify graph_id repeatedly. Check the selected
graph with `GET /v1/graphs` which returns `selectedGraphId`.

**Note:**
Graph selection is included - no credit consumption required.""",
  status_code=status.HTTP_200_OK,
  operation_id="selectGraph",
  responses={
    200: {
      "description": "Graph selected successfully",
      "content": {
        "application/json": {
          "example": {
            "success": True,
            "message": "Graph selected successfully",
            "data": {"selectedGraphId": "kg1a2b3c4d5"},
          }
        }
      },
    },
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    404: {"description": "Graph not found", "model": ErrorResponse},
    500: {"description": "Error selecting graph", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs/{graph_id}/select",
  business_event_type="graph_selected",
)
async def select_graph(
  graph_id: str,
  current_user: User = Depends(get_current_user_with_graph),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
):
  """
  Select a specific graph as the active graph for the user.

  Args:
      graph_id: The graph ID to select
      current_user: The authenticated user from the API key

  Returns:
      Success status with selected graph ID

  Raises:
      HTTPException: If user doesn't have access or graph not found
  """
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    user_graphs = UserGraph.get_by_user_id(current_user.id, session)
    user_graph_ids = [ug.graph_id for ug in user_graphs]

    if graph_id not in user_graph_ids:
      # Record business event for access denied
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/select",
        method="POST",
        event_type="graph_selection_access_denied",
        event_data={
          "user_id": user_id,
          "requested_graph_id": graph_id,
          "available_graphs_count": len(user_graph_ids),
        },
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Access denied to this graph",
        code=ErrorCode.FORBIDDEN,
      )

    # Set this graph as selected
    success = UserGraph.set_selected_graph(current_user.id, graph_id, session)

    if not success:
      # Record business event for graph not found
      metrics_instance = get_endpoint_metrics()
      metrics_instance.record_business_event(
        endpoint="/v1/graphs/{graph_id}/select",
        method="POST",
        event_type="graph_selection_not_found",
        event_data={"user_id": user_id, "requested_graph_id": graph_id},
        user_id=user_id,
      )
      raise create_error_response(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Graph not found",
        code=ErrorCode.NOT_FOUND,
      )

    # Record business event for successful graph selection with additional details
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/select",
      method="POST",
      event_type="graph_selected",
      event_data={"user_id": user_id, "selected_graph_id": graph_id},
      user_id=user_id,
    )

    return SuccessResponse(
      success=True,
      message="Graph selected successfully",
      data={"selectedGraphId": graph_id},
    )

  except HTTPException:
    raise

  except Exception as e:
    logger.error(f"Error selecting graph: {str(e)}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error selecting graph",
      code=ErrorCode.INTERNAL_ERROR,
    )
