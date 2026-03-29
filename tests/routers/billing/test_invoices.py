"""Tests for billing invoices endpoints."""

from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.core import User
from robosystems.models.core.billing import BillingCustomer
from robosystems.routers.billing.invoices import (
  get_upcoming_invoice,
  list_invoices,
)


class TestListInvoices:
  """Tests for list_invoices endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  def _make_invoice(
    self,
    id="binv_1",
    invoice_number="INV-2026-01-0001",
    status="paid",
    total_cents=2999,
    currency="usd",
    invoice_pdf="https://invoice.stripe.com/pdf/1",
    hosted_invoice_url="https://invoice.stripe.com/i/1",
    line_items=None,
  ):
    """Helper to create a mock BillingInvoice."""
    inv = Mock()
    inv.id = id
    inv.invoice_number = invoice_number
    inv.status = status
    inv.total_cents = total_cents
    inv.currency = currency
    inv.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    inv.due_date = datetime(2026, 1, 31, tzinfo=UTC)
    inv.paid_at = datetime(2026, 1, 5, tzinfo=UTC) if status == "paid" else None
    inv.invoice_pdf = invoice_pdf
    inv.hosted_invoice_url = hosted_invoice_url

    if line_items is None:
      line = Mock()
      line.description = "LadybugDB Large - Monthly"
      line.amount_cents = 2999
      line.quantity = 1
      line.period_start = datetime(2026, 1, 1, tzinfo=UTC)
      line.period_end = datetime(2026, 2, 1, tzinfo=UTC)
      line.subscription_id = "bsub_123"
      inv.line_items = [line]
    else:
      inv.line_items = line_items

    return inv

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_empty(self, mock_get_org_user, mock_user, mock_db):
    """Test listing invoices when none exist."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    # Mock the SQLAlchemy query chain
    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = []
    mock_db.query.return_value = mock_query

    result = await list_invoices("org_123", 10, mock_user, mock_db, None)

    assert result.invoices == []
    assert result.total_count == 0
    assert result.has_more is False

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_success(self, mock_get_org_user, mock_user, mock_db):
    """Test successful invoice listing from database."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    inv1 = self._make_invoice(id="binv_1", status="paid", total_cents=2999)
    inv2 = self._make_invoice(
      id="binv_2",
      invoice_number="INV-2026-02-0001",
      status="open",
      total_cents=4999,
      invoice_pdf=None,
      hosted_invoice_url=None,
    )

    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = [inv1, inv2]
    mock_db.query.return_value = mock_query

    result = await list_invoices("org_123", 10, mock_user, mock_db, None)

    assert len(result.invoices) == 2
    assert result.total_count == 2
    assert result.has_more is False

    assert result.invoices[0].id == "binv_1"
    assert result.invoices[0].status == "paid"
    assert result.invoices[0].amount_due == 2999
    assert result.invoices[0].amount_paid == 2999
    assert len(result.invoices[0].line_items) == 1
    assert result.invoices[0].line_items[0].description == "LadybugDB Large - Monthly"

    assert result.invoices[1].id == "binv_2"
    assert result.invoices[1].status == "open"
    assert result.invoices[1].amount_paid == 0

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_has_more(self, mock_get_org_user, mock_user, mock_db):
    """Test has_more flag when more invoices exist than limit."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    # Return limit+1 invoices to trigger has_more
    invoices = [self._make_invoice(id=f"binv_{i}") for i in range(3)]

    mock_query = Mock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = invoices
    mock_db.query.return_value = mock_query

    result = await list_invoices("org_123", 2, mock_user, mock_db, None)

    assert len(result.invoices) == 2
    assert result.has_more is True

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_error_handling(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test error handling in invoice listing."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_db.query.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc:
      await list_invoices("org_123", 10, mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve invoices" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_requires_membership(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Non-members should receive 403."""
    mock_get_org_user.return_value = None

    with pytest.raises(HTTPException) as exc:
      await list_invoices("org_123", 10, mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "You are not a member of this organization"

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_list_invoices_requires_admin_or_owner(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Members without elevated roles should be blocked."""
    from robosystems.models.core import OrgRole

    membership = Mock()
    membership.role = OrgRole.MEMBER
    mock_get_org_user.return_value = membership

    with pytest.raises(HTTPException) as exc:
      await list_invoices("org_123", 10, mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Only owners and admins can view invoices"


class TestGetUpcomingInvoice:
  """Tests for get_upcoming_invoice endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  async def test_get_upcoming_invoice_no_stripe_customer(
    self, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting upcoming invoice when no Stripe customer exists."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = None
    mock_get_customer.return_value = mock_customer

    result = await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert result is None

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_none(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting upcoming invoice when none exists."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.return_value = None
    mock_get_provider.return_value = mock_provider

    result = await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert result is None

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_success(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test successful upcoming invoice retrieval."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.return_value = {
      "id": "in_upcoming",
      "amount_due": 2999,
      "currency": "usd",
      "period_start": 1706745600,
      "period_end": 1709337600,
      "lines": [
        {
          "description": "Standard Plan - Monthly",
          "amount": 2999,
          "quantity": 1,
          "period_start": 1706745600,
          "period_end": 1709337600,
        }
      ],
      "subscription": "sub_123",
    }
    mock_get_provider.return_value = mock_provider

    result = await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert result is not None
    assert result.amount_due == 2999
    assert result.currency == "usd"
    assert len(result.line_items) == 1
    assert result.line_items[0].description == "Standard Plan - Monthly"
    assert result.line_items[0].amount == 2999
    assert result.subscription_id == "sub_123"

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_with_multiple_line_items(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test upcoming invoice with multiple line items."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.return_value = {
      "id": "in_upcoming",
      "amount_due": 7998,
      "currency": "usd",
      "period_start": 1706745600,
      "period_end": 1709337600,
      "lines": [
        {
          "description": "Standard Plan - Monthly",
          "amount": 2999,
          "quantity": 1,
          "period_start": 1706745600,
          "period_end": 1709337600,
        },
        {
          "description": "Storage Overage - 100GB",
          "amount": 4999,
          "quantity": 100,
          "period_start": 1706745600,
          "period_end": 1709337600,
        },
      ],
      "subscription": "sub_123",
    }
    mock_get_provider.return_value = mock_provider

    result = await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert result is not None
    assert result.amount_due == 7998
    assert len(result.line_items) == 2
    assert result.line_items[1].description == "Storage Overage - 100GB"
    assert result.line_items[1].quantity == 100

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_error_handling(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test error handling in upcoming invoice retrieval."""
    from robosystems.models.core import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.side_effect = Exception("Stripe API error")
    mock_get_provider.return_value = mock_provider

    with pytest.raises(HTTPException) as exc:
      await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve upcoming invoice" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_get_upcoming_invoice_requires_membership(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Non-members cannot view upcoming invoices."""
    mock_get_org_user.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "You are not a member of this organization"

  @pytest.mark.asyncio
  @patch("robosystems.models.core.OrgUser.get_by_org_and_user")
  async def test_get_upcoming_invoice_requires_admin_or_owner(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Members without admin role should be denied."""
    from robosystems.models.core import OrgRole

    membership = Mock()
    membership.role = OrgRole.MEMBER
    mock_get_org_user.return_value = membership

    with pytest.raises(HTTPException) as exc:
      await get_upcoming_invoice("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Only owners and admins can view upcoming invoices"
