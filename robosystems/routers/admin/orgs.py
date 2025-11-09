"""Admin API for organization management."""

from datetime import datetime, timezone
from typing import List, Optional
from fastapi import APIRouter, Request, HTTPException, status, Query
from sqlalchemy import func

from ...database import get_db_session
from ...models.iam import Org, OrgUser, User, Graph
from ...models.iam.graph_credits import GraphCredits
from ...models.billing import BillingCustomer, BillingAuditLog
from ...models.api.admin import OrgResponse, OrgUserInfo, OrgGraphInfo
from ...middleware.auth.admin import require_admin
from ...logger import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/admin/v1/orgs", tags=["admin-orgs"])


def _get_org_total_credits(org_id: str, session) -> float:
  """Get total credits across all graphs for an organization.

  Args:
      org_id: Organization ID
      session: Database session

  Returns:
      Total credits as float
  """
  total = (
    session.query(func.sum(GraphCredits.current_balance))
    .join(Graph, GraphCredits.graph_id == Graph.graph_id)
    .filter(Graph.org_id == org_id)
    .scalar()
  ) or 0.0
  return float(total)


def _get_org_customer(org_id: str, session) -> Optional[BillingCustomer]:
  """Get billing customer for an organization.

  Args:
      org_id: Organization ID
      session: Database session

  Returns:
      BillingCustomer if found, None otherwise
  """
  return session.query(BillingCustomer).filter(BillingCustomer.org_id == org_id).first()


def _get_org_users(org_id: str, session) -> List[OrgUserInfo]:
  """Get user list for an organization.

  Args:
      org_id: Organization ID
      session: Database session

  Returns:
      List of OrgUserInfo objects
  """
  org_users = (
    session.query(OrgUser, User)
    .join(User, OrgUser.user_id == User.id)
    .filter(OrgUser.org_id == org_id)
    .all()
  )

  return [
    OrgUserInfo(
      user_id=user.id,
      email=user.email,
      name=user.name,
      role=org_user.role.value,
      created_at=org_user.joined_at,
    )
    for org_user, user in org_users
  ]


def _get_org_graphs(org_id: str, session) -> List[OrgGraphInfo]:
  """Get graph list for an organization.

  Args:
      org_id: Organization ID
      session: Database session

  Returns:
      List of OrgGraphInfo objects
  """
  graphs = session.query(Graph).filter(Graph.org_id == org_id).all()

  return [
    OrgGraphInfo(
      graph_id=g.graph_id,
      name=g.graph_name,
      tier=g.graph_tier,
      created_at=g.created_at,
    )
    for g in graphs
  ]


@router.get("", response_model=List[OrgResponse])
@require_admin(permissions=["orgs:read"])
async def list_orgs(
  request: Request,
  limit: int = Query(100, ge=1, le=1000),
  offset: int = Query(0, ge=0),
):
  """List all organizations."""
  session = next(get_db_session())
  try:
    query = session.query(Org).filter(Org.deleted_at.is_(None))
    total = query.count()
    orgs = query.offset(offset).limit(limit).all()

    results = []
    for org in orgs:
      user_count = session.query(OrgUser).filter(OrgUser.org_id == org.id).count()
      graph_count = session.query(Graph).filter(Graph.org_id == org.id).count()

      total_credits_sum = _get_org_total_credits(org.id, session)
      customer = _get_org_customer(org.id, session)

      results.append(
        OrgResponse(
          org_id=org.id,
          name=org.name,
          org_type=org.org_type.value,
          user_count=user_count,
          graph_count=graph_count,
          total_credits=total_credits_sum,
          stripe_customer_id=customer.stripe_customer_id if customer else None,
          has_payment_method=customer.has_payment_method if customer else False,
          default_payment_method_id=customer.default_payment_method_id
          if customer
          else None,
          invoice_billing_enabled=customer.invoice_billing_enabled
          if customer
          else False,
          billing_email=customer.billing_email if customer else None,
          billing_contact_name=customer.billing_contact_name if customer else None,
          payment_terms=customer.payment_terms if customer else "net_30",
          created_at=org.created_at,
          updated_at=org.updated_at,
          users=[],
          graphs=[],
        )
      )

    logger.info(
      f"Admin listed {len(results)} orgs",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "total": total,
      },
    )

    return results
  finally:
    session.close()


@router.get("/{org_id}", response_model=OrgResponse)
@require_admin(permissions=["orgs:read"])
async def get_org(request: Request, org_id: str):
  """Get detailed information about a specific organization."""
  session = next(get_db_session())
  try:
    org = Org.get_by_id(org_id, session)

    if not org:
      raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Organization {org_id} not found",
      )

    users = _get_org_users(org_id, session)
    graph_infos = _get_org_graphs(org_id, session)
    total_credits_sum = _get_org_total_credits(org_id, session)
    customer = _get_org_customer(org_id, session)

    logger.info(
      f"Admin retrieved org {org_id}",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "org_id": org_id,
      },
    )

    return OrgResponse(
      org_id=org.id,
      name=org.name,
      org_type=org.org_type.value,
      user_count=len(users),
      graph_count=len(graph_infos),
      total_credits=total_credits_sum,
      stripe_customer_id=customer.stripe_customer_id if customer else None,
      has_payment_method=customer.has_payment_method if customer else False,
      default_payment_method_id=customer.default_payment_method_id
      if customer
      else None,
      invoice_billing_enabled=customer.invoice_billing_enabled if customer else False,
      billing_email=customer.billing_email if customer else None,
      billing_contact_name=customer.billing_contact_name if customer else None,
      payment_terms=customer.payment_terms if customer else "net_30",
      created_at=org.created_at,
      updated_at=org.updated_at,
      users=users,
      graphs=graph_infos,
    )
  finally:
    session.close()


@router.patch("/{org_id}", response_model=OrgResponse)
@require_admin(permissions=["orgs:write"])
async def update_org(
  request: Request,
  org_id: str,
  invoice_billing_enabled: Optional[bool] = None,
  billing_email: Optional[str] = None,
  billing_contact_name: Optional[str] = None,
  payment_terms: Optional[str] = None,
):
  """Update organization billing settings."""
  session = next(get_db_session())
  try:
    org = Org.get_by_id(org_id, session)
    if not org:
      raise HTTPException(
        status_code=404,
        detail=f"Organization {org_id} not found",
      )

    customer = BillingCustomer.get_or_create(org_id, session)

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
      sanitized_old_values = {
        k: v for k, v in old_values.items() if k != "payment_terms"
      }
      sanitized_new_values = {
        k: v for k, v in new_values.items() if k != "payment_terms"
      }

      if "payment_terms" in old_values:
        sanitized_old_values["payment_terms"] = "[REDACTED]"
        sanitized_new_values["payment_terms"] = "[REDACTED]"

      BillingAuditLog.log_event(
        session=session,
        event_type="customer.updated",
        org_id=customer.org_id,
        actor_type="admin",
        description=f"Org billing updated by admin {request.state.admin.get('name', 'unknown')}",
        event_data={
          "admin_key_id": request.state.admin_key_id,
          "admin_name": request.state.admin.get("name"),
          "org_id": org_id,
          "old_values": sanitized_old_values,
          "new_values": sanitized_new_values,
        },
      )

    user_infos = _get_org_users(org_id, session)
    graph_infos = _get_org_graphs(org_id, session)
    total_credits_sum = _get_org_total_credits(org_id, session)

    log_changes = {k: v for k, v in new_values.items() if k != "payment_terms"}
    if "payment_terms" in new_values:
      log_changes["payment_terms"] = "[REDACTED]"

    logger.info(
      f"Admin updated org {org_id} billing",
      extra={
        "admin_key_id": request.state.admin_key_id,
        "org_id": org_id,
        "changes": log_changes,
      },
    )

    return OrgResponse(
      org_id=org.id,
      name=org.name,
      org_type=org.org_type.value,
      user_count=len(user_infos),
      graph_count=len(graph_infos),
      total_credits=total_credits_sum,
      stripe_customer_id=customer.stripe_customer_id,
      has_payment_method=customer.has_payment_method,
      default_payment_method_id=customer.default_payment_method_id,
      invoice_billing_enabled=customer.invoice_billing_enabled,
      billing_email=customer.billing_email,
      billing_contact_name=customer.billing_contact_name,
      payment_terms=customer.payment_terms,
      created_at=org.created_at,
      updated_at=org.updated_at,
      users=user_infos,
      graphs=graph_infos,
    )
  finally:
    session.close()
