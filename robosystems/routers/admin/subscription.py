"""Admin API for subscription management."""

from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, Request, HTTPException, status, Query
from pydantic import BaseModel, Field

from ...database import get_db_session
from ...models.billing import (
  BillingCustomer,
  BillingSubscription,
  BillingAuditLog,
  SubscriptionStatus,
)
from ...models.iam import User, Graph
from ...middleware.auth.admin import require_admin
from ...logger import get_logger
from ...config.billing import BillingConfig

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/subscriptions", tags=["admin"])


class SubscriptionCreateRequest(BaseModel):
  """Request to create a new subscription."""

  resource_type: str = "graph"
  resource_id: str
  user_id: str
  plan_name: str
  billing_interval: str = "monthly"


class SubscriptionUpdateRequest(BaseModel):
  """Request to update a subscription."""

  status: Optional[SubscriptionStatus] = None
  plan_name: Optional[str] = None
  base_price_cents: Optional[int] = Field(None, ge=0)
  cancel_at_period_end: Optional[bool] = None


class SubscriptionResponse(BaseModel):
  """Response with subscription details."""

  id: str
  billing_customer_user_id: str
  customer_email: Optional[str]
  customer_name: Optional[str]
  has_payment_method: bool
  invoice_billing_enabled: bool
  resource_type: str
  resource_id: str
  plan_name: str
  billing_interval: str
  base_price_cents: int
  stripe_subscription_id: Optional[str]
  status: str
  started_at: Optional[datetime]
  current_period_start: Optional[datetime]
  current_period_end: Optional[datetime]
  canceled_at: Optional[datetime]
  ends_at: Optional[datetime]
  created_at: datetime
  updated_at: datetime


class CustomerResponse(BaseModel):
  """Response with customer billing details."""

  user_id: str
  user_email: Optional[str]
  user_name: Optional[str]
  stripe_customer_id: Optional[str]
  has_payment_method: bool
  default_payment_method_id: Optional[str]
  invoice_billing_enabled: bool
  billing_email: Optional[str]
  billing_contact_name: Optional[str]
  payment_terms: str
  created_at: datetime
  updated_at: datetime


@router.get("", response_model=List[SubscriptionResponse])
@require_admin(permissions=["subscription:read"])
async def list_subscriptions(
  request: Request,
  resource_type: Optional[str] = Query("graph", description="Filter by resource type"),
  status_filter: Optional[str] = Query(None, description="Filter by status"),
  user_email: Optional[str] = None,
  include_canceled: bool = False,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all subscriptions with optional filters."""
  session = next(get_db_session())
  try:
    query = session.query(BillingSubscription).join(
      User, BillingSubscription.billing_customer_user_id == User.id
    )

    if resource_type:
      query = query.filter(BillingSubscription.resource_type == resource_type)

    if status_filter:
      query = query.filter(BillingSubscription.status == status_filter)

    if user_email:
      query = query.filter(User.email.ilike(f"%{user_email}%"))

    if not include_canceled:
      query = query.filter(
        BillingSubscription.status != SubscriptionStatus.CANCELED.value
      )

    total = query.count()
    subscriptions = query.offset(offset).limit(limit).all()

    results = []
    for sub in subscriptions:
      user = session.query(User).filter(User.id == sub.billing_customer_user_id).first()
      customer = (
        session.query(BillingCustomer)
        .filter(BillingCustomer.user_id == sub.billing_customer_user_id)
        .first()
      )

      results.append(
        SubscriptionResponse(
          id=sub.id,
          billing_customer_user_id=sub.billing_customer_user_id,
          customer_email=user.email if user else None,
          customer_name=user.name if user else None,
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

    user = (
      session.query(User)
      .filter(User.id == subscription.billing_customer_user_id)
      .first()
    )
    customer = (
      session.query(BillingCustomer)
      .filter(BillingCustomer.user_id == subscription.billing_customer_user_id)
      .first()
    )

    logger.info(
      f"Admin retrieved subscription {subscription_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription_id,
      },
    )

    return SubscriptionResponse(
      id=subscription.id,
      billing_customer_user_id=subscription.billing_customer_user_id,
      customer_email=user.email if user else None,
      customer_name=user.name if user else None,
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
    user = session.query(User).filter(User.id == data.user_id).first()
    if not user:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND, detail=f"User {data.user_id} not found"
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

    customer = BillingCustomer.get_or_create(data.user_id, session)

    subscription = BillingSubscription.create_subscription(
      user_id=data.user_id,
      resource_type=data.resource_type,
      resource_id=data.resource_id,
      plan_name=data.plan_name,
      base_price_cents=plan_config["base_price_cents"],
      session=session,
      billing_interval=data.billing_interval,
    )

    subscription.activate(session)

    BillingAuditLog.log_event(
      session=session,
      event_type="subscription.created",
      billing_customer_user_id=data.user_id,
      subscription_id=subscription.id,
      actor_type="admin",
      description=f"Subscription created by admin {request.state.admin.get('name', 'unknown')}",
      event_data={
        "admin_key_id": request.state.admin_key_id,
        "admin_name": request.state.admin.get("name"),
        "resource_type": data.resource_type,
        "resource_id": data.resource_id,
        "plan_name": data.plan_name,
      },
    )

    logger.info(
      f"Admin created subscription {subscription.id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "subscription_id": subscription.id,
        "resource_type": data.resource_type,
        "resource_id": data.resource_id,
        "user_id": data.user_id,
      },
    )

    return SubscriptionResponse(
      id=subscription.id,
      billing_customer_user_id=subscription.billing_customer_user_id,
      customer_email=user.email,
      customer_name=user.name,
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

    subscription.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(subscription)

    if old_values:
      BillingAuditLog.log_event(
        session=session,
        event_type="subscription.updated",
        billing_customer_user_id=subscription.billing_customer_user_id,
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

    user = (
      session.query(User)
      .filter(User.id == subscription.billing_customer_user_id)
      .first()
    )
    customer = (
      session.query(BillingCustomer)
      .filter(BillingCustomer.user_id == subscription.billing_customer_user_id)
      .first()
    )

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
      billing_customer_user_id=subscription.billing_customer_user_id,
      customer_email=user.email if user else None,
      customer_name=user.name if user else None,
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


@router.get("/{subscription_id}/audit", response_model=List[Dict[str, Any]])
@require_admin(permissions=["subscription:read"])
async def get_subscription_audit_log(
  request: Request,
  subscription_id: str,
  event_type: Optional[str] = None,
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


@router.get("/customers/all", response_model=List[CustomerResponse])
@require_admin(permissions=["subscription:read"])
async def list_customers(
  request: Request,
  has_payment_method: Optional[bool] = None,
  invoice_billing_enabled: Optional[bool] = None,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all billing customers."""
  session = next(get_db_session())
  try:
    query = session.query(BillingCustomer).join(
      User, BillingCustomer.user_id == User.id
    )

    if has_payment_method is not None:
      query = query.filter(BillingCustomer.has_payment_method == has_payment_method)

    if invoice_billing_enabled is not None:
      query = query.filter(
        BillingCustomer.invoice_billing_enabled == invoice_billing_enabled
      )

    total = query.count()
    customers = query.offset(offset).limit(limit).all()

    results = []
    for customer in customers:
      user = session.query(User).filter(User.id == customer.user_id).first()
      results.append(
        CustomerResponse(
          user_id=customer.user_id,
          user_email=user.email if user else None,
          user_name=user.name if user else None,
          stripe_customer_id=customer.stripe_customer_id,
          has_payment_method=customer.has_payment_method,
          default_payment_method_id=customer.default_payment_method_id,
          invoice_billing_enabled=customer.invoice_billing_enabled,
          billing_email=customer.billing_email,
          billing_contact_name=customer.billing_contact_name,
          payment_terms=customer.payment_terms,
          created_at=customer.created_at,
          updated_at=customer.updated_at,
        )
      )

    logger.info(
      f"Admin listed {len(results)} customers",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
      },
    )

    return results
  finally:
    session.close()


@router.patch("/customers/{user_id}")
@require_admin(permissions=["subscription:write"])
async def update_customer(
  request: Request,
  user_id: str,
  invoice_billing_enabled: Optional[bool] = None,
  billing_email: Optional[str] = None,
  billing_contact_name: Optional[str] = None,
  payment_terms: Optional[str] = None,
):
  """Update billing customer settings."""
  session = next(get_db_session())
  try:
    customer = (
      session.query(BillingCustomer).filter(BillingCustomer.user_id == user_id).first()
    )

    if not customer:
      customer = BillingCustomer.get_or_create(user_id, session)

    old_values = {}
    new_values = {}

    if (
      invoice_billing_enabled is not None
      and invoice_billing_enabled != customer.invoice_billing_enabled
    ):
      old_values["invoice_billing_enabled"] = customer.invoice_billing_enabled
      new_values["invoice_billing_enabled"] = invoice_billing_enabled
      customer.invoice_billing_enabled = invoice_billing_enabled

    if billing_email is not None and billing_email != customer.billing_email:
      old_values["billing_email"] = customer.billing_email
      new_values["billing_email"] = billing_email
      customer.billing_email = billing_email

    if (
      billing_contact_name is not None
      and billing_contact_name != customer.billing_contact_name
    ):
      old_values["billing_contact_name"] = customer.billing_contact_name
      new_values["billing_contact_name"] = billing_contact_name
      customer.billing_contact_name = billing_contact_name

    if payment_terms is not None and payment_terms != customer.payment_terms:
      old_values["payment_terms"] = customer.payment_terms
      new_values["payment_terms"] = payment_terms
      customer.payment_terms = payment_terms

    customer.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(customer)

    if old_values:
      BillingAuditLog.log_event(
        session=session,
        event_type="customer.updated",
        billing_customer_user_id=user_id,
        actor_type="admin",
        description=f"Customer updated by admin {request.state.admin.get('name', 'unknown')}",
        event_data={
          "admin_key_id": request.state.admin_key_id,
          "admin_name": request.state.admin.get("name"),
          "old_values": old_values,
          "new_values": new_values,
        },
      )

    user = session.query(User).filter(User.id == user_id).first()

    logger.info(
      f"Admin updated customer {user_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "user_id": user_id,
        "changes": new_values,
      },
    )

    return CustomerResponse(
      user_id=customer.user_id,
      user_email=user.email if user else None,
      user_name=user.name if user else None,
      stripe_customer_id=customer.stripe_customer_id,
      has_payment_method=customer.has_payment_method,
      default_payment_method_id=customer.default_payment_method_id,
      invoice_billing_enabled=customer.invoice_billing_enabled,
      billing_email=customer.billing_email,
      billing_contact_name=customer.billing_contact_name,
      payment_terms=customer.payment_terms,
      created_at=customer.created_at,
      updated_at=customer.updated_at,
    )
  finally:
    session.close()
