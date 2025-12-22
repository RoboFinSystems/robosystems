"""Tests for billing customer endpoints."""

from unittest.mock import Mock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.billing import BillingCustomer as BillingCustomerModel
from robosystems.models.iam import User
from robosystems.routers.billing.customer import get_customer


class TestGetCustomer:
  """Tests for get_customer endpoint."""

  @pytest.fixture
  def mock_user(self):
    user = Mock(spec=User)
    user.id = "user_123"
    return user

  @pytest.fixture
  def mock_db(self):
    return Mock()

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.customer.BillingCustomerModel.get_or_create")
  async def test_get_customer_no_payment_methods(
    self, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting customer with no payment methods."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomerModel)
    mock_customer.org_id = "org_123"
    mock_customer.has_payment_method = False
    mock_customer.invoice_billing_enabled = False
    mock_customer.stripe_customer_id = None
    mock_customer.created_at = Mock()
    mock_customer.created_at.isoformat.return_value = "2025-01-01T00:00:00"
    mock_get_customer.return_value = mock_customer

    result = await get_customer("org_123", mock_user, mock_db, None)

    assert result.org_id == "org_123"
    assert result.has_payment_method is False
    assert result.invoice_billing_enabled is False
    assert len(result.payment_methods) == 0

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.customer.BillingCustomerModel.get_or_create")
  @patch("robosystems.routers.billing.customer.get_payment_provider")
  async def test_get_customer_with_payment_methods(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting customer with payment methods."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomerModel)
    mock_customer.org_id = "org_123"
    mock_customer.has_payment_method = True
    mock_customer.invoice_billing_enabled = False
    mock_customer.stripe_customer_id = "cus_123"
    mock_customer.created_at = Mock()
    mock_customer.created_at.isoformat.return_value = "2025-01-01T00:00:00"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.list_payment_methods.return_value = [
      {
        "id": "pm_1",
        "type": "card",
        "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2025},
        "is_default": True,
      },
      {
        "id": "pm_2",
        "type": "card",
        "card": {
          "brand": "mastercard",
          "last4": "5555",
          "exp_month": 6,
          "exp_year": 2026,
        },
        "is_default": False,
      },
    ]
    mock_get_provider.return_value = mock_provider

    result = await get_customer("org_123", mock_user, mock_db, None)

    assert result.org_id == "org_123"
    assert result.has_payment_method is True
    assert len(result.payment_methods) == 2
    assert result.payment_methods[0].id == "pm_1"
    assert result.payment_methods[0].brand == "visa"
    assert result.payment_methods[0].last4 == "4242"
    assert result.payment_methods[0].is_default is True
    assert result.payment_methods[1].id == "pm_2"
    assert result.payment_methods[1].brand == "mastercard"

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.customer.BillingCustomerModel.get_or_create")
  async def test_get_customer_invoice_billing_enabled(
    self, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test getting enterprise customer with invoice billing."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomerModel)
    mock_customer.org_id = "org_123"
    mock_customer.has_payment_method = False
    mock_customer.invoice_billing_enabled = True
    mock_customer.stripe_customer_id = None
    mock_customer.created_at = Mock()
    mock_customer.created_at.isoformat.return_value = "2025-01-01T00:00:00"
    mock_get_customer.return_value = mock_customer

    result = await get_customer("org_123", mock_user, mock_db, None)

    assert result.invoice_billing_enabled is True
    assert len(result.payment_methods) == 0

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.customer.BillingCustomerModel.get_or_create")
  @patch("robosystems.routers.billing.customer.get_payment_provider")
  async def test_get_customer_payment_method_fetch_error(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Test that payment method fetch errors are handled gracefully."""
    from robosystems.models.iam import OrgRole

    mock_org_user = Mock()
    mock_org_user.role = OrgRole.OWNER
    mock_get_org_user.return_value = mock_org_user

    mock_customer = Mock(spec=BillingCustomerModel)
    mock_customer.org_id = "org_123"
    mock_customer.has_payment_method = True
    mock_customer.invoice_billing_enabled = False
    mock_customer.stripe_customer_id = "cus_123"
    mock_customer.created_at = Mock()
    mock_customer.created_at.isoformat.return_value = "2025-01-01T00:00:00"
    mock_get_customer.return_value = mock_customer

    mock_provider = Mock()
    mock_provider.list_payment_methods.side_effect = Exception("Stripe API error")
    mock_get_provider.return_value = mock_provider

    result = await get_customer("org_123", mock_user, mock_db, None)

    assert result.org_id == "org_123"
    assert result.has_payment_method is True
    assert len(result.payment_methods) == 0

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_customer_unexpected_error(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Test handling of unexpected errors."""
    mock_get_org_user.side_effect = Exception("Database error")

    with pytest.raises(HTTPException) as exc:
      await get_customer("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 500
    assert "Failed to retrieve customer information" in exc.value.detail

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  async def test_get_customer_requires_membership(
    self, mock_get_org_user, mock_user, mock_db
  ):
    """Users outside the org should receive 403."""
    mock_get_org_user.return_value = None

    with pytest.raises(HTTPException) as exc:
      await get_customer("org_123", mock_user, mock_db, None)

    assert exc.value.status_code == 403
    assert exc.value.detail == "You are not a member of this organization"

  @pytest.mark.asyncio
  @patch("robosystems.models.iam.OrgUser.get_by_org_and_user")
  @patch("robosystems.routers.billing.customer.BillingCustomerModel.get_or_create")
  @patch("robosystems.routers.billing.customer.get_payment_provider")
  async def test_get_customer_non_owner_hides_payment_details(
    self, mock_get_provider, mock_get_customer, mock_get_org_user, mock_user, mock_db
  ):
    """Admins should see limited data with payment methods hidden."""
    from robosystems.models.iam import OrgRole

    membership = Mock()
    membership.role = OrgRole.ADMIN
    mock_get_org_user.return_value = membership

    mock_customer = Mock(spec=BillingCustomerModel)
    mock_customer.org_id = "org_123"
    mock_customer.has_payment_method = True
    mock_customer.invoice_billing_enabled = True
    mock_customer.stripe_customer_id = "cus_123"
    mock_customer.created_at = Mock()
    mock_customer.created_at.isoformat.return_value = "2025-01-01T00:00:00"
    mock_get_customer.return_value = mock_customer

    provider = Mock()
    provider.list_payment_methods.return_value = [
      {
        "id": "pm_1",
        "type": "card",
        "card": {"brand": "visa", "last4": "1111", "exp_month": 1, "exp_year": 2030},
        "is_default": True,
      }
    ]
    mock_get_provider.return_value = provider

    result = await get_customer("org_123", mock_user, mock_db, None)

    assert result.payment_methods == []
    assert result.stripe_customer_id is None
