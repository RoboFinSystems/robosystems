"""Admin API for subscription management."""

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status

from ...config.billing import BillingConfig
from ...database import get_db_session
from ...logger import get_logger
from ...middleware.auth.admin import require_admin
from ...models.api.admin import (
  SubscriptionCreateRequest,
  SubscriptionResponse,
  SubscriptionUpdateRequest,
)
from ...models.billing import (
  BillingAuditLog,
  BillingCustomer,
  BillingSubscription,
  SubscriptionStatus,
)
from ...models.iam import Graph, User

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/subscriptions", tags=["admin"])


@router.get("", response_model=list[SubscriptionResponse])
@require_admin(permissions=["subscription:read"])
async def list_subscriptions(
  request: Request,
  resource_type: str | None = Query("graph", description="Filter by resource type"),
  status_filter: str | None = Query(None, description="Filter by status"),
  user_email: str | None = None,
  include_canceled: bool = False,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all subscriptions with optional filters."""
  session = next(get_db_session())
  try:
    from ...models.iam import OrgUser

    query = session.query(BillingSubscription)

    if resource_type:
      query = query.filter(BillingSubscription.resource_type == resource_type)

    if status_filter:
      query = query.filter(BillingSubscription.status == status_filter)

    if user_email:
      query = query.join(OrgUser, BillingSubscription.org_id == OrgUser.org_id).join(
        User, OrgUser.user_id == User.id
      )
      query = query.filter(User.email.ilike(f"%{user_email}%"))

    if not include_canceled:
      query = query.filter(
        BillingSubscription.status != SubscriptionStatus.CANCELED.value
      )

    total = query.count()
    subscriptions = query.offset(offset).limit(limit).all()

    results = []
    for sub in subscriptions:
      from ...models.iam import Org

      org = session.query(Org).filter(Org.id == sub.org_id).first()

      customer = (
        session.query(BillingCustomer)
        .filter(BillingCustomer.org_id == sub.org_id)
        .first()
      )

      owner = (
        session.query(OrgUser)
        .join(User, OrgUser.user_id == User.id)
        .filter(
          OrgUser.org_id == sub.org_id,
          OrgUser.role == "OWNER",
        )
        .first()
      )

      user = owner.user if owner else None

      results.append(
        SubscriptionResponse(
          id=sub.id,
          org_id=sub.org_id,
          org_name=org.name if org else None,
          owner_email=user.email if user else None,
          owner_name=user.name if user else None,
          has_payment_method=customer.has_payment_method if customer else False,
          invoice_billing_enabled=customer.invoice_billing_enabled
          if customer
          else False,
          resource_type=sub.resource_type,
          resource_id=sub.resource_id,
          plan_name=sub.plan_name,
          billing_interval=sub.billing_interval,
          base_price_cents=sub.base_price_cents,
          stripe_subscription_id=sub.stripe_subscription_id,
          status=sub.status,
          started_at=sub.started_at,
          current_period_start=sub.current_period_start,
          current_period_end=sub.current_period_end,
          canceled_at=sub.canceled_at,
          ends_at=sub.ends_at,
          created_at=sub.created_at,
          updated_at=sub.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} subscriptions",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
        "filters": {
          "resource_type": resource_type,
          "status": status_filter,
          "user_email": user_email,
        },
      },
    )

    return results
  finally:
    session.close()


@router.get("/{subscription_id}", response_model=SubscriptionResponse)
@require_admin(permissions=["subscription:read"])
async def get_subscription(request: Request, subscription_id: str):
  """Get detailed information about a specific subscription."""
  session = next(get_db_session())
  try:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Subscription {subscription_id} not found",
      )

    from ...models.iam import Org, OrgUser

    org = session.query(Org).filter(Org.id == subscription.org_id).first()

    customer = (
      session.query(BillingCustomer)
      .filter(BillingCustomer.org_id == subscription.org_id)
      .first()
    )

    owner = (
      session.query(OrgUser)
      .join(User, OrgUser.user_id == User.id)
      .filter(
        OrgUser.org_id == subscription.org_id,
        OrgUser.role == "OWNER",
      )
      .first()
    )

    user = owner.user if owner else None

    logger.info(
      f"Admin retrieved subscription {subscription_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription_id,
      },
    )

    return SubscriptionResponse(
      id=subscription.id,
      org_id=subscription.org_id,
      org_name=org.name if org else None,
      owner_email=user.email if user else None,
      owner_name=user.name if user else None,
      has_payment_method=customer.has_payment_method if customer else False,
      invoice_billing_enabled=customer.invoice_billing_enabled if customer else False,
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id,
      plan_name=subscription.plan_name,
      billing_interval=subscription.billing_interval,
      base_price_cents=subscription.base_price_cents,
      stripe_subscription_id=subscription.stripe_subscription_id,
      status=subscription.status,
      started_at=subscription.started_at,
      current_period_start=subscription.current_period_start,
      current_period_end=subscription.current_period_end,
      canceled_at=subscription.canceled_at,
      ends_at=subscription.ends_at,
      created_at=subscription.created_at,
      updated_at=subscription.updated_at,
    )
  finally:
    session.close()


@router.post(
  "", response_model=SubscriptionResponse, status_code=status.HTTP_201_CREATED
)
@require_admin(permissions=["subscription:write"])
async def create_subscription(request: Request, data: SubscriptionCreateRequest):
  """Create a new subscription for any resource type."""
  session = next(get_db_session())
  try:
    from ...models.iam import Org

    org = Org.get_by_id(data.org_id, session)
    if not org:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Organization {data.org_id} not found",
      )

    if data.resource_type == "graph":
      graph = session.query(Graph).filter(Graph.graph_id == data.resource_id).first()
      if not graph:
        raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Graph {data.resource_id} not found",
        )

    existing = BillingSubscription.get_by_resource(
      resource_type=data.resource_type, resource_id=data.resource_id, session=session
    )
    if existing:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Subscription already exists for {data.resource_type} {data.resource_id}",
      )

    plan_config = BillingConfig.get_subscription_plan(data.plan_name)
    if not plan_config:
      raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Invalid plan name: {data.plan_name}",
      )

    customer = BillingCustomer.get_or_create(data.org_id, session)

    subscription = BillingSubscription.create_subscription(
      org_id=data.org_id,
      resource_type=data.resource_type,
      resource_id=data.resource_id,
      plan_name=data.plan_name,
      base_price_cents=plan_config["base_price_cents"],
      session=session,
      billing_interval=data.billing_interval,
    )

    subscription.activate(session)

    from ...models.iam import OrgUser

    owner = (
      session.query(OrgUser)
      .join(User, OrgUser.user_id == User.id)
      .filter(
        OrgUser.org_id == data.org_id,
        OrgUser.role == "OWNER",
      )
      .first()
    )

    user = owner.user if owner else None

    BillingAuditLog.log_event(
      session=session,
      event_type="subscription.created",
      org_id=data.org_id,
      subscription_id=subscription.id,
      actor_type="admin",
      actor_user_id=user.id if user else None,
      description=f"Subscription created by admin {request.state.admin.get('name', 'unknown')}",
      event_data={
        "admin_key_id": request.state.admin_key_id,
        "admin_name": request.state.admin.get("name"),
        "resource_type": data.resource_type,
        "resource_id": data.resource_id,
        "plan_name": data.plan_name,
        "org_id": data.org_id,
      },
    )

    logger.info(
      f"Admin created subscription {subscription.id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription.id,
        "resource_type": data.resource_type,
        "resource_id": data.resource_id,
        "org_id": data.org_id,
      },
    )

    return SubscriptionResponse(
      id=subscription.id,
      org_id=data.org_id,
      org_name=org.name if org else None,
      owner_email=user.email if user else None,
      owner_name=user.name if user else None,
      has_payment_method=customer.has_payment_method,
      invoice_billing_enabled=customer.invoice_billing_enabled,
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id,
      plan_name=subscription.plan_name,
      billing_interval=subscription.billing_interval,
      base_price_cents=subscription.base_price_cents,
      stripe_subscription_id=subscription.stripe_subscription_id,
      status=subscription.status,
      started_at=subscription.started_at,
      current_period_start=subscription.current_period_start,
      current_period_end=subscription.current_period_end,
      canceled_at=subscription.canceled_at,
      ends_at=subscription.ends_at,
      created_at=subscription.created_at,
      updated_at=subscription.updated_at,
    )
  finally:
    session.close()


@router.patch("/{subscription_id}", response_model=SubscriptionResponse)
@require_admin(permissions=["subscription:write"])
async def update_subscription(
  request: Request, subscription_id: str, data: SubscriptionUpdateRequest
):
  """Update an existing subscription."""
  session = next(get_db_session())
  try:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Subscription {subscription_id} not found",
      )

    old_values = {}
    new_values = {}

    if data.status and data.status.value != subscription.status:
      old_values["status"] = subscription.status
      new_values["status"] = data.status.value

      if data.status == SubscriptionStatus.CANCELED:
        subscription.cancel(session)
      elif data.status == SubscriptionStatus.PAUSED:
        subscription.pause(session)
      elif data.status == SubscriptionStatus.ACTIVE:
        subscription.activate(session)

    if data.plan_name and data.plan_name != subscription.plan_name:
      plan_config = BillingConfig.get_subscription_plan(data.plan_name)
      if not plan_config:
        raise HTTPException(
          status_code=status.HTTP_400_BAD_REQUEST,
          detail=f"Invalid plan name: {data.plan_name}",
        )
      old_values["plan_name"] = subscription.plan_name
      new_values["plan_name"] = data.plan_name
      subscription.plan_name = data.plan_name

      if data.base_price_cents is None:
        subscription.base_price_cents = plan_config["base_price_cents"]

    if (
      data.base_price_cents is not None
      and data.base_price_cents != subscription.base_price_cents
    ):
      old_values["base_price_cents"] = subscription.base_price_cents
      new_values["base_price_cents"] = data.base_price_cents
      subscription.base_price_cents = data.base_price_cents

    subscription.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(subscription)

    if old_values:
      BillingAuditLog.log_event(
        session=session,
        event_type="subscription.updated",
        org_id=subscription.org_id,
        subscription_id=subscription.id,
        actor_type="admin",
        description=f"Subscription updated by admin {request.state.admin.get('name', 'unknown')}",
        event_data={
          "admin_key_id": request.state.admin_key_id,
          "admin_name": request.state.admin.get("name"),
          "old_values": old_values,
          "new_values": new_values,
        },
      )

    from ...models.iam import Org, OrgUser

    org = session.query(Org).filter(Org.id == subscription.org_id).first()

    customer = (
      session.query(BillingCustomer)
      .filter(BillingCustomer.org_id == subscription.org_id)
      .first()
    )

    owner = (
      session.query(OrgUser)
      .join(User, OrgUser.user_id == User.id)
      .filter(
        OrgUser.org_id == subscription.org_id,
        OrgUser.role == "OWNER",
      )
      .first()
    )

    user = owner.user if owner else None

    logger.info(
      f"Admin updated subscription {subscription_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription_id,
        "changes": new_values,
      },
    )

    return SubscriptionResponse(
      id=subscription.id,
      org_id=subscription.org_id,
      org_name=org.name if org else None,
      owner_email=user.email if user else None,
      owner_name=user.name if user else None,
      has_payment_method=customer.has_payment_method if customer else False,
      invoice_billing_enabled=customer.invoice_billing_enabled if customer else False,
      resource_type=subscription.resource_type,
      resource_id=subscription.resource_id,
      plan_name=subscription.plan_name,
      billing_interval=subscription.billing_interval,
      base_price_cents=subscription.base_price_cents,
      stripe_subscription_id=subscription.stripe_subscription_id,
      status=subscription.status,
      started_at=subscription.started_at,
      current_period_start=subscription.current_period_start,
      current_period_end=subscription.current_period_end,
      canceled_at=subscription.canceled_at,
      ends_at=subscription.ends_at,
      created_at=subscription.created_at,
      updated_at=subscription.updated_at,
    )
  finally:
    session.close()


@router.get("/{subscription_id}/audit", response_model=list[dict[str, Any]])
@require_admin(permissions=["subscription:read"])
async def get_subscription_audit_log(
  request: Request,
  subscription_id: str,
  event_type: str | None = None,
  limit: int = Query(100, ge=1, le=1000),
):
  """Get audit history for a subscription."""
  session = next(get_db_session())
  try:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Subscription {subscription_id} not found",
      )

    query = session.query(BillingAuditLog).filter(
      BillingAuditLog.subscription_id == subscription_id
    )

    if event_type:
      query = query.filter(BillingAuditLog.event_type == event_type)

    audit_logs = (
      query.order_by(BillingAuditLog.event_timestamp.desc()).limit(limit).all()
    )

    results = []
    for log in audit_logs:
      results.append(
        {
          "id": log.id,
          "event_type": log.event_type,
          "event_timestamp": log.event_timestamp.isoformat(),
          "actor_user_id": log.actor_user_id,
          "actor_type": log.actor_type,
          "actor_ip": log.actor_ip,
          "event_data": log.event_data,
          "description": log.description,
        }
      )

    logger.info(
      f"Admin retrieved audit log for subscription {subscription_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription_id,
        "log_count": len(results),
      },
    )

    return results
  finally:
    session.close()
