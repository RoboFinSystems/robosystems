"""Tests for admin invoice endpoints."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from fastapi import HTTPException, Request
from robosystems.models.billing import BillingInvoice, BillingInvoiceLineItem
from robosystems.models.iam import User


def mock_require_admin(permissions=None):
  """Mock decorator that bypasses admin authentication."""

  def decorator(func):
    return func

  return decorator


@pytest.fixture(autouse=True)
def patch_admin_auth(monkeypatch):
  """Automatically patch admin authentication for all tests in this module."""

  async def mock_admin_auth(self, request, credentials=None):
    """Mock admin auth that does nothing."""
    request.state.admin = {
      "key_id": "test_key",
      "permissions": ["subscription:read", "subscription:write"],
    }
    request.state.admin_key_id = "test_key"

  monkeypatch.setattr(
    "robosystems.middleware.auth.admin.AdminAuthMiddleware.__call__",
    mock_admin_auth,
  )


class TestListInvoices:
  """Tests for list_invoices admin endpoint."""

  @pytest.fixture
  def mock_request(self):
    request = Mock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.fixture
  def mock_get_db_session(self):
    with patch("robosystems.routers.admin.invoice.get_db_session") as mock:
      mock_session = MagicMock()
      mock.return_value = iter([mock_session])
      yield mock, mock_session

  @pytest.mark.asyncio
  async def test_list_invoices_no_filters(self, mock_request, mock_get_db_session):
    """Test listing all invoices without filters."""
    from robosystems.routers.admin.invoice import list_invoices

    mock_get_db, mock_session = mock_get_db_session

    mock_user = Mock(spec=User)
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"

    mock_invoice = Mock(spec=BillingInvoice)
    mock_invoice.id = "inv_123"
    mock_invoice.invoice_number = "INV-001"
    mock_invoice.billing_customer_user_id = "user_123"
    mock_invoice.status = "DRAFT"
    mock_invoice.subtotal_cents = 2999
    mock_invoice.tax_cents = 300
    mock_invoice.discount_cents = 0
    mock_invoice.total_cents = 3299
    mock_invoice.period_start = datetime(2025, 1, 1)
    mock_invoice.period_end = datetime(2025, 2, 1)
    mock_invoice.due_date = datetime(2025, 2, 15)
    mock_invoice.payment_terms = "net_30"
    mock_invoice.payment_method = None
    mock_invoice.payment_reference = None
    mock_invoice.sent_at = None
    mock_invoice.paid_at = None
    mock_invoice.voided_at = None
    mock_invoice.created_at = datetime(2025, 1, 1)

    mock_line_item = Mock(spec=BillingInvoiceLineItem)
    mock_line_item.id = "line_1"
    mock_line_item.subscription_id = "sub_123"
    mock_line_item.resource_type = "graph"
    mock_line_item.resource_id = "kg_456"
    mock_line_item.description = "Standard Plan - Monthly"
    mock_line_item.quantity = 1
    mock_line_item.unit_price_cents = 2999
    mock_line_item.amount_cents = 2999
    mock_line_item.line_metadata = {}

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 1
    mock_query.order_by.return_value.offset.return_value.limit.return_value.all.return_value = [
      mock_invoice
    ]

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_user,
      None,
    ]

    def query_side_effect(model):
      if model == BillingInvoice:
        return mock_query
      elif model == BillingInvoiceLineItem:
        line_query = Mock()
        line_query.filter.return_value.all.return_value = [mock_line_item]
        return line_query
      elif model == User:
        user_query = Mock()
        user_query.filter.return_value.first.return_value = mock_user
        return user_query
      return Mock()

    mock_session.query.side_effect = query_side_effect

    result = await list_invoices(mock_request, None, None, 100, 0)

    assert len(result) == 1
    assert result[0].id == "inv_123"
    assert result[0].invoice_number == "INV-001"
    assert result[0].user_email == "test@example.com"
    assert result[0].user_name == "Test User"
    assert result[0].status == "DRAFT"
    assert result[0].total_cents == 3299
    assert len(result[0].line_items) == 1
    assert result[0].line_items[0].description == "Standard Plan - Monthly"

  @pytest.mark.asyncio
  async def test_list_invoices_with_status_filter(
    self, mock_request, mock_get_db_session
  ):
    """Test listing invoices filtered by status."""
    from robosystems.routers.admin.invoice import list_invoices

    mock_get_db, mock_session = mock_get_db_session

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value = mock_query
    mock_query.count.return_value = 0
    mock_query.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

    result = await list_invoices(mock_request, "PAID", None, 100, 0)

    assert result == []
    mock_query.filter.assert_called()

  @pytest.mark.asyncio
  async def test_list_invoices_with_user_id_filter(
    self, mock_request, mock_get_db_session
  ):
    """Test listing invoices filtered by user ID."""
    from robosystems.routers.admin.invoice import list_invoices

    mock_get_db, mock_session = mock_get_db_session

    mock_query = mock_session.query.return_value
    mock_query.filter.return_value = mock_query
    mock_query.count.return_value = 0
    mock_query.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

    result = await list_invoices(mock_request, None, "user_123", 100, 0)

    assert result == []
    mock_query.filter.assert_called()

  @pytest.mark.asyncio
  async def test_list_invoices_pagination(self, mock_request, mock_get_db_session):
    """Test invoice listing with pagination."""
    from robosystems.routers.admin.invoice import list_invoices

    mock_get_db, mock_session = mock_get_db_session

    mock_query = mock_session.query.return_value
    mock_query.count.return_value = 150
    mock_query.order_by.return_value.offset.return_value.limit.return_value.all.return_value = []

    result = await list_invoices(mock_request, None, None, 50, 100)

    assert result == []
    mock_query.order_by.return_value.offset.assert_called_once_with(100)
    mock_query.order_by.return_value.offset.return_value.limit.assert_called_once_with(
      50
    )


class TestGetInvoice:
  """Tests for get_invoice admin endpoint."""

  @pytest.fixture
  def mock_request(self):
    request = Mock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.fixture
  def mock_get_db_session(self):
    with patch("robosystems.routers.admin.invoice.get_db_session") as mock:
      mock_session = MagicMock()
      mock.return_value = iter([mock_session])
      yield mock, mock_session

  @pytest.mark.asyncio
  async def test_get_invoice_success(self, mock_request, mock_get_db_session):
    """Test successful invoice retrieval."""
    from robosystems.routers.admin.invoice import get_invoice

    mock_get_db, mock_session = mock_get_db_session

    mock_user = Mock(spec=User)
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"

    mock_invoice = Mock(spec=BillingInvoice)
    mock_invoice.id = "inv_123"
    mock_invoice.invoice_number = "INV-001"
    mock_invoice.billing_customer_user_id = "user_123"
    mock_invoice.status = "PAID"
    mock_invoice.subtotal_cents = 2999
    mock_invoice.tax_cents = 300
    mock_invoice.discount_cents = 0
    mock_invoice.total_cents = 3299
    mock_invoice.period_start = datetime(2025, 1, 1)
    mock_invoice.period_end = datetime(2025, 2, 1)
    mock_invoice.due_date = datetime(2025, 2, 15)
    mock_invoice.payment_terms = "net_30"
    mock_invoice.payment_method = "stripe"
    mock_invoice.payment_reference = "pi_123"
    mock_invoice.sent_at = datetime(2025, 1, 2)
    mock_invoice.paid_at = datetime(2025, 1, 10)
    mock_invoice.voided_at = None
    mock_invoice.created_at = datetime(2025, 1, 1)

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = mock_invoice

    user_query = Mock()
    user_query.filter.return_value.first.return_value = mock_user

    line_item_query = Mock()
    line_item_query.filter.return_value.all.return_value = []

    def query_side_effect(model):
      if model == BillingInvoice:
        return invoice_query
      elif model == User:
        return user_query
      elif model == BillingInvoiceLineItem:
        return line_item_query
      return Mock()

    mock_session.query.side_effect = query_side_effect

    result = await get_invoice(mock_request, "inv_123")

    assert result.id == "inv_123"
    assert result.invoice_number == "INV-001"
    assert result.user_email == "test@example.com"
    assert result.status == "PAID"
    assert result.payment_method == "stripe"
    assert result.payment_reference == "pi_123"

  @pytest.mark.asyncio
  async def test_get_invoice_not_found(self, mock_request, mock_get_db_session):
    """Test getting invoice that doesn't exist."""
    from robosystems.routers.admin.invoice import get_invoice

    mock_get_db, mock_session = mock_get_db_session

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = None
    mock_session.query.return_value = invoice_query

    with pytest.raises(HTTPException) as exc:
      await get_invoice(mock_request, "inv_999")

    assert exc.value.status_code == 404
    assert "Invoice not found" in exc.value.detail


class TestMarkInvoicePaid:
  """Tests for mark_invoice_paid admin endpoint."""

  @pytest.fixture
  def mock_request(self):
    request = Mock(spec=Request)
    request.state.admin_key_id = "admin_key_123"
    return request

  @pytest.fixture
  def mock_get_db_session(self):
    with patch("robosystems.routers.admin.invoice.get_db_session") as mock:
      mock_session = MagicMock()
      mock.return_value = iter([mock_session])
      yield mock, mock_session

  @pytest.mark.asyncio
  async def test_mark_invoice_paid_success(self, mock_request, mock_get_db_session):
    """Test successfully marking invoice as paid."""
    from robosystems.routers.admin.invoice import mark_invoice_paid

    mock_get_db, mock_session = mock_get_db_session

    mock_user = Mock(spec=User)
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"

    mock_invoice = Mock(spec=BillingInvoice)
    mock_invoice.id = "inv_123"
    mock_invoice.invoice_number = "INV-001"
    mock_invoice.billing_customer_user_id = "user_123"
    mock_invoice.status = "SENT"
    mock_invoice.subtotal_cents = 2999
    mock_invoice.tax_cents = 300
    mock_invoice.discount_cents = 0
    mock_invoice.total_cents = 3299
    mock_invoice.period_start = datetime(2025, 1, 1)
    mock_invoice.period_end = datetime(2025, 2, 1)
    mock_invoice.due_date = datetime(2025, 2, 15)
    mock_invoice.payment_terms = "net_30"
    mock_invoice.payment_method = None
    mock_invoice.payment_reference = None
    mock_invoice.sent_at = datetime(2025, 1, 2)
    mock_invoice.paid_at = None
    mock_invoice.voided_at = None
    mock_invoice.created_at = datetime(2025, 1, 1)

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = mock_invoice

    user_query = Mock()
    user_query.filter.return_value.first.return_value = mock_user

    line_item_query = Mock()
    line_item_query.filter.return_value.all.return_value = []

    def query_side_effect(model):
      if model == BillingInvoice:
        return invoice_query
      elif model == User:
        return user_query
      elif model == BillingInvoiceLineItem:
        return line_item_query
      return Mock()

    mock_session.query.side_effect = query_side_effect

    result = await mark_invoice_paid(
      mock_request, "inv_123", "bank_transfer", "REF12345"
    )

    mock_invoice.mark_paid.assert_called_once_with(
      session=mock_session,
      payment_method="bank_transfer",
      payment_reference="REF12345",
    )
    assert result.id == "inv_123"

  @pytest.mark.asyncio
  async def test_mark_invoice_paid_not_found(self, mock_request, mock_get_db_session):
    """Test marking non-existent invoice as paid."""
    from robosystems.routers.admin.invoice import mark_invoice_paid

    mock_get_db, mock_session = mock_get_db_session

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = None
    mock_session.query.return_value = invoice_query

    with pytest.raises(HTTPException) as exc:
      await mark_invoice_paid(mock_request, "inv_999", "bank_transfer", None)

    assert exc.value.status_code == 404
    assert "Invoice not found" in exc.value.detail

  @pytest.mark.asyncio
  async def test_mark_invoice_paid_already_paid(
    self, mock_request, mock_get_db_session
  ):
    """Test marking already paid invoice."""
    from robosystems.routers.admin.invoice import mark_invoice_paid

    mock_get_db, mock_session = mock_get_db_session

    mock_invoice = Mock(spec=BillingInvoice)
    mock_invoice.status = "PAID"

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = mock_invoice
    mock_session.query.return_value = invoice_query

    with pytest.raises(HTTPException) as exc:
      await mark_invoice_paid(mock_request, "inv_123", "bank_transfer", None)

    assert exc.value.status_code == 400
    assert "already paid" in exc.value.detail.lower()

  @pytest.mark.asyncio
  async def test_mark_invoice_paid_without_reference(
    self, mock_request, mock_get_db_session
  ):
    """Test marking invoice as paid without payment reference."""
    from robosystems.routers.admin.invoice import mark_invoice_paid

    mock_get_db, mock_session = mock_get_db_session

    mock_user = Mock(spec=User)
    mock_user.email = "test@example.com"
    mock_user.name = "Test User"

    mock_invoice = Mock(spec=BillingInvoice)
    mock_invoice.id = "inv_123"
    mock_invoice.status = "SENT"
    mock_invoice.invoice_number = "INV-001"
    mock_invoice.billing_customer_user_id = "user_123"
    mock_invoice.subtotal_cents = 2999
    mock_invoice.tax_cents = 300
    mock_invoice.discount_cents = 0
    mock_invoice.total_cents = 3299
    mock_invoice.period_start = datetime(2025, 1, 1)
    mock_invoice.period_end = datetime(2025, 2, 1)
    mock_invoice.due_date = datetime(2025, 2, 15)
    mock_invoice.payment_terms = "net_30"
    mock_invoice.payment_method = None
    mock_invoice.payment_reference = None
    mock_invoice.sent_at = datetime(2025, 1, 2)
    mock_invoice.paid_at = None
    mock_invoice.voided_at = None
    mock_invoice.created_at = datetime(2025, 1, 1)

    invoice_query = Mock()
    invoice_query.filter.return_value.first.return_value = mock_invoice

    user_query = Mock()
    user_query.filter.return_value.first.return_value = mock_user

    line_item_query = Mock()
    line_item_query.filter.return_value.all.return_value = []

    def query_side_effect(model):
      if model == BillingInvoice:
        return invoice_query
      elif model == User:
        return user_query
      elif model == BillingInvoiceLineItem:
        return line_item_query
      return Mock()

    mock_session.query.side_effect = query_side_effect

    result = await mark_invoice_paid(mock_request, "inv_123", "cash", None)

    mock_invoice.mark_paid.assert_called_once_with(
      session=mock_session, payment_method="cash", payment_reference=None
    )
    assert result.id == "inv_123"
