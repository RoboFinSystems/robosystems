"""Graph creation and listing endpoints."""

from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy.orm import Session

from robosystems.database import get_async_db_session, session
from robosystems.logger import logger
from robosystems.middleware.auth.dependencies import (
  get_current_user,
)
from robosystems.middleware.operations import (
  IdempotencyCache,
  OperationEnvelope,
  fingerprint_body,
  get_idempotency_cache,
  idempotent_dispatch,
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
  ErrorCode,
  create_error_response,
)
from robosystems.models.api.graphs.core import CreateGraphRequest
from robosystems.models.api.user import (
  UserGraphsResponse,
)
from robosystems.models.core import OrgLimits, OrgUser, User
from robosystems.operations.graph.listing import list_user_graphs

router = APIRouter(prefix="/v1/graphs", tags=["Graphs"])

# Shown to prospects at graph creation, so these describe only what is built.
# Single-sourced for the schema-loading path and its fallback.
_DISPLAY_NAMES = {
  "roboledger": "RoboLedger - Accounting & Financial Reporting",
  "roboinvestor": "RoboInvestor - Investment Management",
}

_ROBOLEDGER_DESCRIPTION = (
  "A general ledger synced from QuickBooks, with every account mapped to a "
  "reporting concept. Build statements and forecasts, run schedules for "
  "depreciation and prepaids, and close the month with a receipt — in the app "
  "or from Claude, ChatGPT or any MCP client."
)

_ROBOINVESTOR_DESCRIPTION = (
  "Private-market portfolio tracking — positions with cost basis, and a "
  "securities registry for stock, LLC and LP units, SAFEs, notes, warrants and "
  "options. Link a holding to its issuer's graph, and the reports that company "
  "shares with you attach to it."
)


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
    listing = list_user_graphs(current_user.id, session)
    graphs = listing.graphs
    selected_graph_id = listing.selected_graph_id

    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs",
      method="GET",
      event_type="user_graphs_accessed",
      event_data={
        "user_id": user_id,
        "total_graphs": len(graphs),
        "admin_graphs": listing.admin_graphs,
        "member_graphs": listing.member_graphs,
        "repository_count": listing.repository_count,
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
  # No graph_id exists yet, so a sentinel scopes the idempotency key.
  _graph_id = "new"
  body_fp = fingerprint_body(request)

  # The key stays reserved until the pending envelope is recorded, so a
  # concurrent retry can't dispatch a second creation; every exit that doesn't
  # record (402/403 below, enqueue failure) releases it.
  async with idempotent_dispatch(
    cache, user_id, _graph_id, op_name, idempotency_key, body_fp
  ) as idem:
    if idem.replay is not None:
      return idem.replay

    try:
      user_orgs = OrgUser.get_user_orgs(current_user.id, db)
      if not user_orgs:
        # Reachable: leaving a last org leaves no membership. 403 like every
        # other org-resolving surface, not a 500.
        _raise_http_exception(
          status_code=status.HTTP_403_FORBIDDEN,
          error_code="org_not_found",
          message=(
            "You are not a member of any organization, so there is nothing to "
            "bill a new graph to. Please contact support."
          ),
        )

      membership = user_orgs[0]
      org_id = membership.org_id

      # Starts a recurring charge on the org's card and consumes org quota, so
      # owner/admin only (members can't add a payment method either).
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
      graph_tier = tier_map.get(
        request.instance_tier.lower(), GraphTier.LADYBUG_STANDARD
      )

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

      # If creation later fails, the terminal-status hook evicts this envelope
      # so a retry under the same key dispatches again.
      await idem.record(envelope)

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
    from robosystems.schemas.runtime.manager import (
      ENTITY_GRAPH_EXTENSIONS,
      SchemaManager,
    )

    manager = SchemaManager()
    # `knowledge` is installed on a subgraph, not composed into an entity
    # graph, so the create path would reject it.
    extensions_info = [
      info
      for info in manager.list_available_extensions()
      if info["name"] in ENTITY_GRAPH_EXTENSIONS
    ]
    logger.info(f"Got {len(extensions_info)} extensions from schema manager")

    available_extensions = []
    for ext_info in extensions_info:
      logger.debug(
        f"Extension {ext_info['name']}: available={ext_info.get('available', False)}"
      )
      if ext_info["available"]:
        try:
          from robosystems.schemas.loader import (
            get_contextual_schema_loader,
            get_schema_loader,
          )

          if ext_info["name"] == "roboledger":
            # The full accounting context entity graphs get.
            loader = get_contextual_schema_loader("application", "roboledger")
            description = _ROBOLEDGER_DESCRIPTION
          elif ext_info["name"] == "roboinvestor":
            loader = get_schema_loader([ext_info["name"]])
            description = _ROBOINVESTOR_DESCRIPTION
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
            "display_name": _DISPLAY_NAMES.get(
              ext_info["name"], ext_info["name"].title()
            ),
            "description": description,  # Use the correctly set description
            "node_count": node_count,
            "relationship_count": relationship_count,
          }
        )

    extension_objects = [
      AvailableExtension(
        name=str(ext["name"]),
        display_name=str(ext["display_name"]),
        description=str(ext["description"]),
        enabled=False,
      )
      for ext in available_extensions
    ]

    return AvailableExtensionsResponse(
      extensions=extension_objects,
    )

  except Exception as e:
    logger.error(f"Failed to load schema extensions: {e}")
    return AvailableExtensionsResponse(
      extensions=[
        AvailableExtension(
          name="roboledger",
          display_name=_DISPLAY_NAMES["roboledger"],
          description=_ROBOLEDGER_DESCRIPTION,
          enabled=False,
        ),
        AvailableExtension(
          name="roboinvestor",
          display_name=_DISPLAY_NAMES["roboinvestor"],
          description=_ROBOINVESTOR_DESCRIPTION,
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

    # include_disabled=True: enabled_default answers "is the stack deployed by
    # default", not "can a customer buy this" (Large/XLarge would vanish in
    # production). The billing catalog below decides what is sellable.
    tiers = GraphTierConfig.get_available_tiers(include_disabled=True)

    excluded_tiers = [
      "ladybug-shared",
    ]
    tiers = [tier for tier in tiers if tier.get("tier") not in excluded_tiers]

    # The billing catalog also decides what is offered: a tier it can't price
    # is omitted (unless disabled tiers were requested). No hardcoded price
    # fallback: a number matching neither billing nor Stripe is worse.
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
  description="Status per tier: `ready` (slot available) or `at_capacity` (contact support).",
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

    manager = LadybugAllocationManager(environment=app_env.STACK_ENVIRONMENT)

    tiers: list[TierCapacity] = []
    for tier_name, tier_enum in tier_map.items():
      try:
        capacity_status = await manager.check_tier_capacity(tier_enum)
      except Exception as e:
        logger.warning(f"Failed to check capacity for {tier_name}: {e}")
        capacity_status = "at_capacity"

      # No auto-scaling queue, so scalable means unavailable.
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
