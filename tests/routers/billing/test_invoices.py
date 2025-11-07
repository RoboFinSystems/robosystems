"""Tests for billing invoices endpoints."""

import pytest
from unittest.mock import Mock, patch
from fastapi import HTTPException
from robosystems.routers.billing.invoices import (
  list_invoices,
  get_upcoming_invoice,
)
from robosystems.models.billing import BillingCustomer
from robosystems.models.iam import User


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

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  async def test_list_invoices_no_stripe_customer(
    self, mock_get_customer, mock_user, mock_db
  ):
    """Test listing invoices when no Stripe customer exists."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = None
    mock_get_customer.return_value = mock_customer

    result = await list_invoices(10, mock_user, mock_db, None)

    assert result.invoices == []
    assert result.total_count == 0
    assert result.has_more is False

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_list_invoices_success(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test successful invoice listing."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.list_invoices.return_value = {
      "invoices": [
        {
          "id": "in_1",
          "number": "INV-001",
          "status": "paid",
          "amount_due": 2999,
          "amount_paid": 2999,
          "currency": "usd",
          "created": 1704067200,
          "due_date": 1706745600,
          "paid_at": 1704153600,
          "invoice_pdf": "https://invoice.stripe.com/i/acct_1/test_1",
          "hosted_invoice_url": "https://invoice.stripe.com/i/acct_1/test_1/pdf",
          "lines": [
            {
              "description": "Standard Plan - Monthly",
              "amount": 2999,
              "quantity": 1,
              "period_start": 1704067200,
              "period_end": 1706745600,
            }
          ],
          "subscription": "sub_123",
        },
        {
          "id": "in_2",
          "number": "INV-002",
          "status": "open",
          "amount_due": 4999,
          "amount_paid": 0,
          "currency": "usd",
          "created": 1706745600,
          "due_date": None,
          "paid_at": None,
          "invoice_pdf": None,
          "hosted_invoice_url": None,
          "lines": [],
          "subscription": "sub_123",
        },
      ],
      "has_more": True,
    }
    mock_get_provider.return_value = mock_provider

    result = await list_invoices(10, mock_user, mock_db, None)

    assert len(result.invoices) == 2
    assert result.total_count == 2
    assert result.has_more is True

    assert result.invoices[0].id == "in_1"
    assert result.invoices[0].number == "INV-001"
    assert result.invoices[0].status == "paid"
    assert result.invoices[0].amount_due == 2999
    assert result.invoices[0].amount_paid == 2999
    assert len(result.invoices[0].line_items) == 1
    assert result.invoices[0].line_items[0].description == "Standard Plan - Monthly"

    assert result.invoices[1].id == "in_2"
    assert result.invoices[1].status == "open"
    assert result.invoices[1].paid_at is None

    mock_provider.list_invoices.assert_called_once_with("cus_123", limit=10)

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_list_invoices_with_limit(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test invoice listing respects limit parameter."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.list_invoices.return_value = {"invoices": [], "has_more": False}
    mock_get_provider.return_value = mock_provider

    await list_invoices(25, mock_user, mock_db, None)

    mock_provider.list_invoices.assert_called_once_with("cus_123", limit=25)

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_list_invoices_error_handling(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test error handling in invoice listing."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.list_invoices.side_effect = Exception("Stripe API error")
    mock_get_provider.return_value = mock_provider

    with pytest.raises(HTTPException) as exc:
      await list_invoices(10, mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve invoices" in exc.value.detail


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
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  async def test_get_upcoming_invoice_no_stripe_customer(
    self, mock_get_customer, mock_user, mock_db
  ):
    """Test getting upcoming invoice when no Stripe customer exists."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = None
    mock_get_customer.return_value = mock_customer

    result = await get_upcoming_invoice(mock_user, mock_db, None)

    assert result is None

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_none(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test getting upcoming invoice when none exists."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.return_value = None
    mock_get_provider.return_value = mock_provider

    result = await get_upcoming_invoice(mock_user, mock_db, None)

    assert result is None

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_success(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test successful upcoming invoice retrieval."""
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

    result = await get_upcoming_invoice(mock_user, mock_db, None)

    assert result is not None
    assert result.amount_due == 2999
    assert result.currency == "usd"
    assert len(result.line_items) == 1
    assert result.line_items[0].description == "Standard Plan - Monthly"
    assert result.line_items[0].amount == 2999
    assert result.subscription_id == "sub_123"

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_with_multiple_line_items(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test upcoming invoice with multiple line items."""
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

    result = await get_upcoming_invoice(mock_user, mock_db, None)

    assert result is not None
    assert result.amount_due == 7998
    assert len(result.line_items) == 2
    assert result.line_items[1].description == "Storage Overage - 100GB"
    assert result.line_items[1].quantity == 100

  @pytest.mark.asyncio
  @patch("robosystems.routers.billing.invoices.BillingCustomer.get_or_create")
  @patch("robosystems.routers.billing.invoices.get_payment_provider")
  async def test_get_upcoming_invoice_error_handling(
    self, mock_get_provider, mock_get_customer, mock_user, mock_db
  ):
    """Test error handling in upcoming invoice retrieval."""
    mock_customer = Mock(spec=BillingCustomer)
    mock_customer.stripe_customer_id = "cus_123"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.get_upcoming_invoice.side_effect = Exception("Stripe API error")
    mock_get_provider.return_value = mock_provider

    with pytest.raises(HTTPException) as exc:
      await get_upcoming_invoice(mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve upcoming invoice" in exc.value.detail
