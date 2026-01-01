"""
Unified graph creation router.

This module handles creating new graph databases with flexible configurations,
optionally including initial entities like companies.
"""

from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from robosystems.database import session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user,
  get_current_user_with_graph,
)
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.rate_limits import (
  general_api_rate_limit_dependency,
  subscription_aware_rate_limit_dependency,
  user_management_rate_limit_dependency,
)
from robosystems.middleware.sse import create_operation_response
from robosystems.models.api import (
  AvailableExtension,
  AvailableExtensionsResponse,
  AvailableGraphTiersResponse,
)
from robosystems.models.api.common import (
  ErrorCode,
  ErrorResponse,
  SuccessResponse,
  create_error_response,
)
from robosystems.models.api.graphs.core import CreateGraphRequest
from robosystems.models.api.user import (
  GraphInfo,
  UserGraphsResponse,
)
from robosystems.models.iam import GraphUser, OrgLimits, OrgUser, User

# Create router for unified graph creation
router = APIRouter(prefix="/v1/graphs", tags=["Graphs"])


def _create_error_response(
  error_code: str,
  message: str,
  field: str | None = None,
  details: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
  field: str | None = None,
  details: dict[str, Any] | None = None,
):
  """Raise an HTTPException with standardized error format."""
  raise HTTPException(
    status_code=status_code,
    detail=_create_error_response(error_code, message, field, details),
  )


@router.get(
  "",
  response_model=UserGraphsResponse,
  summary="Get User Graphs and Repositories",
  description="""List all graph databases and shared repositories accessible to the current user.

Returns a unified list of both user-created graphs and shared repositories (like SEC data)
that the user has access to, including their role/access level and selection status.

**Returned Information:**
- Graph/Repository ID and display name
- User's role/access level (admin/member for graphs, read/write/admin for repositories)
- Selection status (only user graphs can be selected)
- Creation timestamp
- Repository type indicator (isRepository: true for shared repositories)

**User Graphs (isRepository: false):**
- Collaborative workspaces that can be shared with other users
- Roles: `admin` (full access, can invite users) or `member` (read/write access)
- Can be selected as active workspace
- Graphs you create or have been invited to

**Shared Repositories (isRepository: true):**
- Read-only data repositories like SEC filings, industry benchmarks
- Access levels: `read`, `write` (for data contributions), `admin`
- Cannot be selected (each has separate subscription)
- Require separate subscriptions (personal, cannot be shared)

**Selected Graph Concept:**
The "selected" graph is the user's currently active workspace (user graphs only).
Many API operations default to the selected graph if no graph_id is provided.
Users can change their selected graph via `POST /v1/graphs/{graph_id}/select`.

**Use Cases:**
- Display unified graph/repository selector in UI
- Show all accessible data sources (both owned graphs and subscribed repositories)
- Identify currently active workspace
- Filter by type (user graphs vs repositories)

**Empty Response:**
New users receive an empty list with `selectedGraphId: null`. Users should create
a graph or subscribe to a repository.

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
            "with_graphs_and_repositories": {
              "summary": "User with graphs and repository subscriptions",
              "value": {
                "graphs": [
                  {
                    "graphId": "kg1a2b3c4d5",
                    "graphName": "Acme Consulting LLC",
                    "role": "admin",
                    "isSelected": True,
                    "createdAt": "2024-01-15T10:00:00Z",
                    "isRepository": False,
                    "repositoryType": None,
                  },
                  {
                    "graphId": "kg9z8y7x6w5",
                    "graphName": "TechCorp Enterprises",
                    "role": "member",
                    "isSelected": False,
                    "createdAt": "2024-02-20T14:30:00Z",
                    "isRepository": False,
                    "repositoryType": None,
                  },
                  {
                    "graphId": "sec",
                    "graphName": "SEC",
                    "role": "read",
                    "isSelected": False,
                    "createdAt": "2024-03-01T09:00:00Z",
                    "isRepository": True,
                    "repositoryType": "sec",
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
    # Get all user-graph relationships (user graphs)
    user_graphs = GraphUser.get_by_user_id(current_user.id, session)

    # Get all user-repository relationships (shared repositories)
    from robosystems.models.iam.user_repository import UserRepository

    user_repositories = UserRepository.get_user_repositories(
      current_user.id, session, active_only=True
    )

    # Find the selected graph
    selected_graph_id = None
    graphs = []
    admin_graphs = 0
    member_graphs = 0
    repository_count = 0

    # Add user graphs
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
          isRepository=False,
          repositoryType=None,
          schemaExtensions=user_graph.graph.schema_extensions or [],
          isSubgraph=user_graph.graph.is_subgraph or False,
          parentGraphId=user_graph.graph.parent_graph_id,
          graphType=user_graph.graph.graph_type,
        )
      )

    # Add repositories (shared repositories cannot be selected)
    for user_repo in user_repositories:
      repository_count += 1

      graph_name = (
        user_repo.graph.graph_name
        if user_repo.graph
        else user_repo.repository_name.upper()
      )

      graphs.append(
        GraphInfo(
          graphId=user_repo.repository_name,
          graphName=graph_name,
          role=user_repo.access_level.value,
          isSelected=False,
          createdAt=user_repo.created_at.isoformat(),
          isRepository=True,
          repositoryType=user_repo.repository_type.value,
          schemaExtensions=user_repo.graph.schema_extensions if user_repo.graph else [],
          isSubgraph=False,  # Repositories are never subgraphs
          parentGraphId=None,
          graphType=user_repo.graph.graph_type if user_repo.graph else "repository",
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
        "repository_count": repository_count,
        "has_selected_graph": bool(selected_graph_id),
      },
      user_id=user_id,
    )

    return UserGraphsResponse(graphs=graphs, selectedGraphId=selected_graph_id)

  except Exception as e:
    logger.error(f"Error retrieving user graphs: {e!s}")
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

**Graph Creation Options:**

1. **Entity Graph with Initial Entity** (`initial_entity` provided, `create_entity=True`):
   - Creates graph structure with entity schema extensions
   - Populates an initial entity node with provided data
   - Useful when you want a pre-configured entity to start with
   - Example: Creating a company graph with the company already populated

2. **Entity Graph without Initial Entity** (`initial_entity=None`, `create_entity=False`):
   - Creates graph structure with entity schema extensions
   - Graph starts empty, ready for data import
   - Useful for bulk data imports or custom workflows
   - Example: Creating a graph structure before importing from CSV/API

3. **Generic Graph** (no `initial_entity` provided):
   - Creates empty graph with custom schema extensions
   - General-purpose knowledge graph
   - Example: Analytics graphs, custom data models

**Required Fields:**
- `metadata.graph_name`: Unique name for the graph
- `instance_tier`: Resource tier (ladybug-standard, ladybug-large, ladybug-xlarge)

**Optional Fields:**
- `metadata.description`: Human-readable description of the graph's purpose
- `metadata.schema_extensions`: List of schema extensions (roboledger, roboinvestor, etc.)
- `tags`: Organizational tags (max 10)
- `initial_entity`: Entity data (required for entity graphs with initial data)
- `create_entity`: Whether to populate initial entity (default: true when initial_entity provided)

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
  background_tasks: BackgroundTasks,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
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
    from robosystems.config.graph_tier import GraphTier
    from robosystems.database import get_db_session
    from robosystems.middleware.billing.enforcement import (
      check_can_provision_graph,
    )

    # Get database session for user limits check
    db = next(get_db_session())

    try:
      # Check org's graph limits
      user_orgs = OrgUser.get_user_orgs(current_user.id, db)
      if not user_orgs:
        _raise_http_exception(
          status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
          error_code="org_not_found",
          message="User organization not found. Please contact support.",
        )

      org_id = user_orgs[0].org_id
      org_limits = OrgLimits.get_or_create_for_org(org_id, db)

      # Check if org can create more graphs
      can_create, reason = org_limits.can_create_graph(db)
      if not can_create:
        _raise_http_exception(
          status_code=status.HTTP_403_FORBIDDEN,
          error_code="graph_limit_reached",
          message=reason,
        )

      # Map tier to GraphTier enum
      tier_map = {
        "ladybug-standard": GraphTier.LADYBUG_STANDARD,
        "ladybug-large": GraphTier.LADYBUG_LARGE,
        "ladybug-xlarge": GraphTier.LADYBUG_XLARGE,
      }
      graph_tier = tier_map.get(
        request.instance_tier.lower(), GraphTier.LADYBUG_STANDARD
      )

      # Check billing before allowing graph creation
      can_provision, billing_error = check_can_provision_graph(
        user_id=current_user.id,
        requested_tier=graph_tier,
        session=db,
      )
      if not can_provision:
        _raise_http_exception(
          status_code=status.HTTP_402_PAYMENT_REQUIRED,
          error_code="payment_required",
          message=billing_error or "Valid payment method required to create graphs.",
        )

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
            "subscription_tier": "ladybug-standard",  # Default for tracking
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

      # Create SSE operation for async tracking
      response = await create_operation_response(
        operation_type=operation_type,
        user_id=current_user.id,
        graph_id=None,  # Will be set when graph is created
      )

      # Queue Dagster job with SSE monitoring via FastAPI background task
      operation_id = response["operation_id"]

      from robosystems.middleware.sse import (
        build_graph_job_config,
        run_and_monitor_dagster_job,
      )

      if request.initial_entity:
        # Entity graph creation via Dagster
        job_name = "create_entity_graph_job"
        run_config = build_graph_job_config(
          job_name,
          user_id=str(current_user.id),
          entity_name=operation_data["entity_data"]["name"],
          entity_identifier=operation_data["entity_data"].get("identifier"),
          entity_identifier_type=operation_data["entity_data"].get("identifier_type"),
          tier=request.instance_tier,
          graph_name=request.metadata.graph_name,
          description=request.metadata.description,
          schema_extensions=request.metadata.schema_extensions,
          tags=request.tags or [],
          create_entity=request.create_entity,
          skip_billing=False,
          operation_id=operation_id,
        )
      else:
        # Generic graph creation via Dagster
        job_name = "create_graph_job"
        run_config = build_graph_job_config(
          job_name,
          user_id=str(current_user.id),
          tier=request.instance_tier,
          graph_name=request.metadata.graph_name,
          description=request.metadata.description,
          schema_extensions=request.metadata.schema_extensions,
          tags=request.tags or [],
          skip_billing=False,
          operation_id=operation_id,
          custom_schema=request.custom_schema.model_dump()
          if request.custom_schema
          else None,
        )

      # Run Dagster job with SSE monitoring in background
      background_tasks.add_task(
        run_and_monitor_dagster_job,
        job_name=job_name,
        operation_id=operation_id,
        run_config=run_config,
      )

      logger.info(
        f"✓ Created SSE operation {operation_id} and queued Dagster job {job_name}"
      )
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
      detail=f"Failed to create graph creation operation: {e!s}",
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
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
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
            get_contextual_schema_loader,
            get_schema_loader,
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


@router.get(
  "/tiers",
  response_model=AvailableGraphTiersResponse,
  summary="Get Available Graph Tiers",
  description="""List all available graph database tier configurations.

This endpoint provides comprehensive technical specifications for each available
graph database tier, including instance types, resource limits, and features.

**Tier Information:**
Each tier includes:
- Technical specifications (instance type, memory, storage)
- Resource limits (subgraphs, credits, rate limits)
- Feature list with capabilities
- Availability status

**Available Tiers:**
- **ladybug-standard**: Multi-tenant entry-level tier
- **ladybug-large**: Dedicated professional tier with subgraph support
- **ladybug-xlarge**: Enterprise tier with maximum resources
- **neo4j-community-large**: Neo4j Community Edition (optional, if enabled)
- **neo4j-enterprise-xlarge**: Neo4j Enterprise Edition (optional, if enabled)

**Use Cases:**
- Display tier options in graph creation UI
- Show technical specifications for tier selection
- Validate tier availability before graph creation
- Display feature comparisons

**Note:**
Tier listing is included - no credit consumption required.""",
  operation_id="getAvailableGraphTiers",
  responses={
    200: {
      "description": "Tiers retrieved successfully",
      "content": {
        "application/json": {
          "example": {
            "tiers": [
              {
                "tier": "ladybug-standard",
                "name": "ladybug-standard",
                "display_name": "LadybugDB Standard",
                "description": "Multi-tenant LadybugDB tier for cost-efficient entry",
                "backend": "ladybug",
                "enabled": True,
                "max_subgraphs": 0,
                "storage_limit_gb": 500,
                "monthly_credits": 10000,
                "api_rate_multiplier": 1.0,
                "monthly_price": 49.99,
                "features": [
                  "10,000 AI credits per month",
                  "500GB storage limit",
                  "Single database only",
                  "14GB RAM",
                  "30-day backup retention",
                ],
                "instance": {
                  "type": "r7g.large",
                  "memory_mb": 14336,
                  "databases_per_instance": 10,
                },
                "limits": {
                  "storage_gb": 500,
                  "monthly_credits": 10000,
                  "max_subgraphs": 0,
                  "copy_operations": {
                    "max_file_size_gb": 1.0,
                    "timeout_seconds": 300,
                    "concurrent_operations": 1,
                    "max_files_per_operation": 100,
                    "daily_copy_operations": 10,
                  },
                  "backup": {
                    "max_backup_size_gb": 10,
                    "backup_retention_days": 7,
                    "max_backups_per_day": 2,
                  },
                },
              }
            ]
          }
        }
      },
    },
    500: {"description": "Failed to retrieve tiers"},
  },
)
async def get_available_graph_tiers(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
  include_disabled: bool = False,
) -> AvailableGraphTiersResponse:
  """
  Get available graph database tiers with technical specifications.

  Returns comprehensive information about all available graph tiers,
  including technical specifications, resource limits, and features.

  Args:
      include_disabled: Whether to include disabled/optional tiers (default: False)
  """
  try:
    from robosystems.config import BillingConfig
    from robosystems.config.graph_tier import GraphTierConfig

    # Get tier configurations from graph.yml
    tiers = GraphTierConfig.get_available_tiers(include_disabled=include_disabled)

    # Filter out internal-only and not-yet-available tiers
    excluded_tiers = [
      "ladybug-shared",
      "neo4j-community-large",
      "neo4j-enterprise-xlarge",
    ]
    tiers = [tier for tier in tiers if tier.get("tier") not in excluded_tiers]

    # Try to add pricing information from billing config
    try:
      pricing_info = BillingConfig.get_all_pricing_info()
      tier_pricing = pricing_info.get("subscription_tiers", {})

      for tier in tiers:
        tier_key = tier.get("tier", "")

        # tier_pricing uses the same keys as tier names (e.g., "ladybug-standard")
        if tier_pricing.get(tier_key):
          # Convert cents to dollars for monthly_price
          base_price_cents = tier_pricing[tier_key].get("base_price_cents", 0)
          tier["monthly_price"] = base_price_cents / 100.0
          tier["monthly_credits"] = tier_pricing[tier_key].get(
            "monthly_credit_allocation", 0
          )
        else:
          # Default pricing if not found (matching current billing config)
          default_prices = {
            "ladybug-standard": 50.0,
            "ladybug-large": 300.0,
            "ladybug-xlarge": 700.0,
            "neo4j-community-large": 299.99,
            "neo4j-enterprise-xlarge": 999.99,
          }
          tier["monthly_price"] = default_prices.get(tier_key)

    except Exception as pricing_error:
      logger.warning(f"Could not load pricing information: {pricing_error}")
      # Pricing remains None if we can't load it

    return AvailableGraphTiersResponse(tiers=tiers)

  except Exception as e:
    logger.error(f"Failed to load tier configurations: {e}")
    _raise_http_exception(
      status.HTTP_500_INTERNAL_SERVER_ERROR,
      ErrorCode.INTERNAL_ERROR,
      f"Failed to retrieve tier configurations: {e!s}",
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
    user_graphs = GraphUser.get_by_user_id(current_user.id, session)
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
    success = GraphUser.set_selected_graph(current_user.id, graph_id, session)

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
    logger.error(f"Error selecting graph: {e!s}")
    raise create_error_response(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Error selecting graph",
      code=ErrorCode.INTERNAL_ERROR,
    )
