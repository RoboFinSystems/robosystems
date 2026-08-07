"""Graph creation and listing endpoints."""

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session, session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user,
  get_current_user_with_graph,
)
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  check_idempotency,
  fingerprint_body,
  get_idempotency_cache,
  log_operation_audit,
  wrap_pending,
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
from robosystems.models.api import (
  AvailableExtension,
  AvailableExtensionsResponse,
  AvailableGraphTiersResponse,
  GraphCapacityResponse,
  TierCapacity,
)
from robosystems.models.api.common import (
  AUTHENTICATED_ERROR_RESPONSES,
  OPERATION_ERROR_RESPONSES,
  RESOURCE_ERROR_RESPONSES,
  ErrorCode,
  SuccessResponse,
  create_error_response,
)
from robosystems.models.api.graphs.core import CreateGraphRequest
from robosystems.models.api.user import (
  GraphInfo,
  UserGraphsResponse,
)
from robosystems.models.core import Graph, GraphUser, OrgLimits, OrgRole, OrgUser, User

router = APIRouter(prefix="/v1/graphs", tags=["Graphs"])


def _create_error_response(
  error_code: str,
  message: str,
  field: str | None = None,
  details: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
  raise HTTPException(
    status_code=status_code,
    detail=_create_error_response(error_code, message, field, details),
  )


@router.get(
  "",
  response_model=UserGraphsResponse,
  summary="List User Graphs and Repositories",
  operation_id="getGraphs",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs", business_event_type="user_graphs_accessed"
)
async def get_graphs(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(user_management_rate_limit_dependency),
) -> UserGraphsResponse:
  user_id = getattr(current_user, "id", None) if current_user else None

  try:
    # Get all user-graph relationships (user graphs)
    user_graphs = GraphUser.get_by_user_id(current_user.id, session)

    # Get all user-repository relationships (shared repositories)
    from robosystems.models.core.user.user_repository import UserRepository

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
      # Skip deprovisioned graphs
      graph_status = user_graph.graph.status or "active"
      if graph_status == "deprovisioned":
        continue

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
          status=graph_status,
        )
      )

    # Add org-owned graphs the user holds implicitly as org owner/admin
    # (no explicit GraphUser row — access derives from the org role).
    explicit_graph_ids = {ug.graph_id for ug in user_graphs}
    admin_org_ids = [
      ou.org_id
      for ou in OrgUser.get_user_orgs(current_user.id, session)
      if ou.role in (OrgRole.OWNER, OrgRole.ADMIN)
    ]
    if admin_org_ids:
      implicit_graphs = (
        session.query(Graph)
        .filter(
          Graph.org_id.in_(admin_org_ids),
          Graph.graph_id.notin_(explicit_graph_ids)
          if explicit_graph_ids
          else Graph.graph_id.isnot(None),
        )
        .all()
      )
      for graph in implicit_graphs:
        graph_status = graph.status or "active"
        if graph_status == "deprovisioned":
          continue

        admin_graphs += 1
        graphs.append(
          GraphInfo(
            graphId=graph.graph_id,
            graphName=graph.graph_name,
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
          repositoryType=user_repo.repository_type,
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
  response_model=OperationEnvelope,
  status_code=status.HTTP_202_ACCEPTED,
  summary="Create New Graph Database",
  description="Creates a graph asynchronously. Returns an `OperationEnvelope` with `operation_id` for SSE progress at `/v1/operations/{operation_id}/stream`. Entity graphs require `initial_entity`. Supports `Idempotency-Key` header.",
  operation_id="createGraph",
  responses={
    **OPERATION_ERROR_RESPONSES,
    402: {
      "description": "Payment required — add a payment method or check org graph limit"
    },
  },
)
@endpoint_metrics_decorator(
  endpoint_name="/v1/graphs",
  method="POST",
  business_event_type="graph_created",
)
async def create_graph(
  request: CreateGraphRequest,
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
  idempotency_key: str | None = Header(None, alias="Idempotency-Key"),
  cache: IdempotencyCache = Depends(get_idempotency_cache),
  db: Session = Depends(get_async_db_session),
) -> OperationEnvelope:
  from robosystems.config.graph_tier import GraphTier
  from robosystems.middleware.billing.enforcement import check_can_provision_graph
  from robosystems.worker.client import enqueue_task

  op_name = "create-graph"
  user_id = str(current_user.id)
  # No graph_id exists yet — use sentinel for idempotency scoping.
  # The cache key is (user_id, "new", op_name, idempotency_key, fingerprint),
  # which is unique enough to prevent duplicate graph creation retries.
  _graph_id = "new"
  body_fp = fingerprint_body(request)

  replay = await check_idempotency(
    cache, user_id, _graph_id, op_name, idempotency_key, body_fp
  )
  if replay is not None:
    return replay

  try:
    # Check org's graph limits
    user_orgs = OrgUser.get_user_orgs(current_user.id, db)
    if not user_orgs:
      _raise_http_exception(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        error_code="org_not_found",
        message="User organization not found. Please contact support.",
      )

    membership = user_orgs[0]
    org_id = membership.org_id

    # Creating a graph starts a recurring charge on the org's payment method
    # and consumes org quota, so it is an owner/admin action. Members cannot
    # add a payment method either (checkout is owner-only), so without this a
    # member could commit the org's stored card without its billing
    # administrators knowing until the invoice arrived.
    if not membership.can_create_graphs():
      _raise_http_exception(
        status_code=status.HTTP_403_FORBIDDEN,
        error_code="graph_creation_requires_admin",
        message=(
          "Only organization owners and admins can create graphs. "
          "Ask an owner or admin to create one, or to grant you access to "
          "an existing graph."
        ),
      )

    org_limits = OrgLimits.get_or_create_for_org(org_id, db)

    can_create, reason = org_limits.can_create_graph(db)
    if not can_create:
      _raise_http_exception(
        status_code=status.HTTP_403_FORBIDDEN,
        error_code="graph_limit_reached",
        message=reason,
      )

    tier_map = {
      "ladybug-standard": GraphTier.LADYBUG_STANDARD,
      "ladybug-large": GraphTier.LADYBUG_LARGE,
      "ladybug-xlarge": GraphTier.LADYBUG_XLARGE,
    }
    graph_tier = tier_map.get(request.instance_tier.lower(), GraphTier.LADYBUG_STANDARD)

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

    is_entity = request.initial_entity is not None
    entity_data = None
    if is_entity:
      entity_data = {
        **request.initial_entity.model_dump(),
        "extensions": request.metadata.schema_extensions,
      }

    logger.info(
      f"Enqueuing graph creation: user={current_user.id}, "
      f"type={'entity' if is_entity else 'generic'}, tier={request.instance_tier}"
    )

    response = await enqueue_task(
      task_type="graph_creation",
      graph_id=None,
      user_id=user_id,
      params={
        "graph_type": "entity" if is_entity else "generic",
        "graph_name": request.metadata.graph_name,
        "tier": request.instance_tier,
        "schema_extensions": request.metadata.schema_extensions,
        "custom_schema": request.custom_schema.model_dump()
        if request.custom_schema
        else None,
        "entity_data": entity_data,
        "create_entity": request.create_entity if is_entity else False,
        "description": request.metadata.description,
        "tags": request.tags or [],
      },
    )

    operation_id = response["operation_id"]
    envelope = wrap_pending(
      op_name,
      operation_id=operation_id,
      partial_result=response,
      created_by=user_id,
    )

    # Known limitation: cache.put is after enqueue_task. A Valkey failure here
    # leaves the task dispatched without idempotency protection — the next retry
    # would dispatch again. Fixing requires a two-phase write (placeholder before
    # dispatch, update after), which is a systemic change tracked separately.
    if idempotency_key is not None:
      await cache.put(user_id, _graph_id, op_name, idempotency_key, envelope, body_fp)

    log_operation_audit(
      operation_name=op_name,
      operation_id=operation_id,
      user_id=user_id,
      graph_id=_graph_id,
      duration_ms=0.0,
      status="pending",
      idempotency_key=idempotency_key,
      event="graph.operation",
    )
    return envelope

  except HTTPException:
    raise
  except Exception as e:
    logger.error(f"Failed to create graph creation operation: {e}", exc_info=True)
    raise HTTPException(
      status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail="Failed to create graph creation operation.",
    )


@router.get(
  "/extensions",
  response_model=AvailableExtensionsResponse,
  summary="Get Available Schema Extensions",
  operation_id="getAvailableExtensions",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
async def get_available_extensions(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
):
  try:
    from robosystems.schemas.runtime.manager import SchemaManager

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
              "Context-aware: SEC repositories get reporting-only tables, "
              "entity graphs get full accounting capabilities."
            )
          elif ext_info["name"] == "roboinvestor":
            loader = get_schema_loader([ext_info["name"]])
            description = (
              "Investment portfolio management with securities tracking, trade execution, "
              "and risk analysis. Includes market data, dividends, benchmarks, and "
              "position-level performance monitoring."
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
            "Context-aware: SEC repositories get reporting-only tables, "
            "entity graphs get full accounting capabilities."
          ),
          enabled=False,
        ),
        AvailableExtension(
          name="roboinvestor",
          description=(
            "Investment portfolio management with securities tracking, trade execution, "
            "and risk analysis. Includes market data, dividends, benchmarks, and "
            "position-level performance monitoring."
          ),
          enabled=False,
        ),
      ],
    )


@router.get(
  "/tiers",
  response_model=AvailableGraphTiersResponse,
  summary="Get Available Graph Tiers",
  operation_id="getAvailableGraphTiers",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
async def get_available_graph_tiers(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
  include_disabled: bool = False,
) -> AvailableGraphTiersResponse:
  try:
    from robosystems.config import BillingConfig
    from robosystems.config.graph_tier import GraphTierConfig

    # include_disabled=True is deliberate, same reasoning as /v1/offering:
    # get_available_tiers gates on deployment.always_enabled/enabled_default,
    # which answers "is the CloudFormation stack deployed by default" — not
    # "can a customer buy this". Large and XLarge carry enabled_default: false
    # plus an enable_var only the deploy workflow reads, so honouring the flag
    # here made both tiers invisible in production. What is sellable is
    # decided by the billing catalog below.
    tiers = GraphTierConfig.get_available_tiers(include_disabled=True)

    # Filter out internal-only and not-yet-available tiers
    excluded_tiers = [
      "ladybug-shared",
    ]
    tiers = [tier for tier in tiers if tier.get("tier") not in excluded_tiers]

    # Attach pricing from the billing catalog, which also decides what is
    # offered: a tier billing cannot price is not purchasable here and is
    # omitted unless the caller asked for disabled tiers. No hardcoded price
    # fallback — a fabricated number that matches neither billing nor Stripe
    # is worse than omission.
    try:
      tier_pricing = BillingConfig.get_all_pricing_info().get("subscription_tiers", {})
    except Exception as pricing_error:
      logger.warning(f"Could not load pricing information: {pricing_error}")
      tier_pricing = None

    if tier_pricing is not None:
      priced_tiers = []
      for tier in tiers:
        plan_data = tier_pricing.get(tier.get("tier", ""))
        if plan_data:
          tier["monthly_price"] = plan_data.get("base_price_cents", 0) / 100.0
          tier["monthly_credits"] = plan_data.get("monthly_credit_allocation", 0)
          priced_tiers.append(tier)
        elif include_disabled:
          priced_tiers.append(tier)
      tiers = priced_tiers

    return AvailableGraphTiersResponse(tiers=tiers)

  except Exception as e:
    logger.error(f"Failed to load tier configurations: {e}")
    _raise_http_exception(
      status.HTTP_500_INTERNAL_SERVER_ERROR,
      ErrorCode.INTERNAL_ERROR,
      f"Failed to retrieve tier configurations: {e!s}",
    )


@router.get(
  "/capacity",
  response_model=GraphCapacityResponse,
  summary="Get Graph Tier Capacity",
  description="Status per tier: `ready` (slot available), `scalable` (can provision, 3-5 min), `at_capacity` (contact support). Cached 60s.",
  operation_id="getGraphCapacity",
  responses={**AUTHENTICATED_ERROR_RESPONSES},
)
async def get_graph_capacity(
  current_user: User = Depends(get_current_user),
  _rate_limit: None = Depends(general_api_rate_limit_dependency),
) -> GraphCapacityResponse:
  try:
    from robosystems.config import env as app_env
    from robosystems.config.billing.core import DEFAULT_GRAPH_BILLING_PLANS
    from robosystems.middleware.graph.allocation_manager import (
      LadybugAllocationManager,
    )
    from robosystems.middleware.graph.types import GraphTier

    tier_map = {
      "ladybug-standard": GraphTier.LADYBUG_STANDARD,
      "ladybug-large": GraphTier.LADYBUG_LARGE,
      "ladybug-xlarge": GraphTier.LADYBUG_XLARGE,
    }

    display_names = {
      plan["name"]: plan["display_name"] for plan in DEFAULT_GRAPH_BILLING_PLANS
    }

    status_messages = {
      "ready": "Available",
      "scalable": "Available — provisioning takes 3-5 minutes",
      "at_capacity": "Currently at capacity — contact us for access",
    }

    manager = LadybugAllocationManager(environment=app_env.ENVIRONMENT)

    tiers: list[TierCapacity] = []
    for tier_name, tier_enum in tier_map.items():
      try:
        capacity_status = await manager.check_tier_capacity(tier_enum)
      except Exception as e:
        logger.warning(f"Failed to check capacity for {tier_name}: {e}")
        capacity_status = "at_capacity"

      # Auto-scaling queue removed — scalable means unavailable
      if capacity_status == "scalable":
        capacity_status = "at_capacity"

      tiers.append(
        TierCapacity(
          tier=tier_name,
          display_name=display_names.get(tier_name, tier_name),
          status=capacity_status,
          message=status_messages.get(capacity_status, "Unknown"),
        )
      )

    return GraphCapacityResponse(tiers=tiers)

  except Exception as e:
    logger.error(f"Failed to check graph capacity: {e}")
    _raise_http_exception(
      status.HTTP_500_INTERNAL_SERVER_ERROR,
      ErrorCode.INTERNAL_ERROR,
      f"Failed to check capacity: {e!s}",
    )


@router.post(
  "/{graph_id}/select",
  response_model=SuccessResponse,
  summary="Select Graph",
  operation_id="selectGraph",
  responses={**RESOURCE_ERROR_RESPONSES},
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
