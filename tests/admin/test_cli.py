"""Tests for the admin CLI commands.

Tests CLI layer only: argument parsing, request construction, output formatting,
error handling. All API calls are mocked via AdminAPIClient._make_request.
"""

import json
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from robosystems.admin.cli import AdminAPIClient, cli
from robosystems.admin.ssm_executor import SSMExecutor


@pytest.fixture
def runner():
  return CliRunner()


@pytest.fixture(autouse=True)
def wide_console():
  """Force Rich console to use wide output so tables don't wrap."""
  from rich.console import Console

  import robosystems.admin.cli as cli_module

  original_console = cli_module.console
  cli_module.console = Console(width=200, highlight=False, force_terminal=False)
  yield
  cli_module.console = original_console


@pytest.fixture(autouse=True)
def mock_admin_key():
  """Patch _get_admin_key to skip AWS Secrets Manager / env var lookup."""
  patcher = patch.object(AdminAPIClient, "_get_admin_key", return_value="test-key")
  patcher.start()
  yield patcher
  patcher.stop()


@pytest.fixture
def mock_make_request():
  """Patch AdminAPIClient._make_request to return controlled responses."""
  with patch.object(AdminAPIClient, "_make_request") as mock:
    yield mock


# ---------------------------------------------------------------------------
# AdminAPIClient
# ---------------------------------------------------------------------------
class TestAdminAPIClient:
  def test_url_resolution_dev(self):
    client = AdminAPIClient(environment="dev")
    assert client.api_base_url == "http://localhost:8000"

  def test_url_resolution_direct_staging(self):
    with patch.object(AdminAPIClient, "_check_api_access_mode"):
      client = AdminAPIClient(environment="staging", use_direct=True)
    assert client.api_base_url == "https://api.staging.robosystems.ai"

  def test_url_resolution_direct_prod(self):
    with patch.object(AdminAPIClient, "_check_api_access_mode"):
      client = AdminAPIClient(environment="prod", use_direct=True)
    assert client.api_base_url == "https://api.robosystems.ai"

  def test_url_resolution_custom(self):
    client = AdminAPIClient(api_base_url="http://custom:9999")
    assert client.api_base_url == "http://custom:9999"

  def test_get_admin_key_dev(self, mock_admin_key):
    # Stop the autouse mock so we can call the real method
    mock_admin_key.stop()
    try:
      with patch.dict("os.environ", {"ADMIN_API_KEY": "dev-key-123"}):
        with patch.object(AdminAPIClient, "__init__", lambda self, **kw: None):
          client = AdminAPIClient()
          client.environment = "dev"
          client.use_direct = False
          key = AdminAPIClient._get_admin_key(client)
          assert key == "dev-key-123"
    finally:
      mock_admin_key.start()

  def test_get_admin_key_dev_missing(self, mock_admin_key):
    mock_admin_key.stop()
    try:
      with patch.dict("os.environ", {}, clear=True):
        with patch.object(AdminAPIClient, "__init__", lambda self, **kw: None):
          client = AdminAPIClient()
          client.environment = "dev"
          client.use_direct = False
          client.aws_profile = "robosystems-sso"
          # When env var is missing in dev, it falls through to secrets manager
          with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("No AWS access")
            with pytest.raises(click.ClickException):
              AdminAPIClient._get_admin_key(client)
    finally:
      mock_admin_key.start()

  def test_get_admin_key_prod(self, mock_admin_key):
    mock_admin_key.stop()
    try:
      mock_result = MagicMock()
      mock_result.stdout = json.dumps({"ADMIN_API_KEY": "prod-secret-key"})
      mock_result.returncode = 0

      with patch.object(AdminAPIClient, "__init__", lambda self, **kw: None):
        client = AdminAPIClient()
        client.environment = "prod"
        client.use_direct = False
        client.aws_profile = "robosystems-sso"
        with patch("subprocess.run", return_value=mock_result):
          key = AdminAPIClient._get_admin_key(client)
          assert key == "prod-secret-key"
    finally:
      mock_admin_key.start()

  def test_make_request_success(self):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.ok = True
    mock_response.text = '{"id": "sub-123"}'
    mock_response.json.return_value = {"id": "sub-123"}

    with patch("requests.request", return_value=mock_response) as mock_req:
      client = AdminAPIClient(environment="dev")
      result = client._make_request(
        "GET", "/admin/v1/subscriptions", params={"limit": 10}
      )

      mock_req.assert_called_once_with(
        method="GET",
        url="http://localhost:8000/admin/v1/subscriptions",
        headers={
          "Authorization": "Bearer test-key",
          "Content-Type": "application/json",
        },
        json=None,
        params={"limit": 10},
        timeout=30,
      )
      assert result == {"id": "sub-123"}

  def test_make_request_401(self):
    mock_response = MagicMock()
    mock_response.status_code = 401
    mock_response.ok = False

    with patch("requests.request", return_value=mock_response):
      client = AdminAPIClient(environment="dev")
      with pytest.raises(click.ClickException, match="Authentication failed"):
        client._make_request("GET", "/admin/v1/test")

  def test_make_request_404(self):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.ok = False

    with patch("requests.request", return_value=mock_response):
      client = AdminAPIClient(environment="dev")
      with pytest.raises(click.ClickException, match="Resource not found"):
        client._make_request("GET", "/admin/v1/test")

  def test_make_request_timeout(self):
    import requests as req_lib

    with patch("requests.request", side_effect=req_lib.Timeout()):
      client = AdminAPIClient(environment="dev")
      with pytest.raises(click.ClickException, match="timed out"):
        client._make_request("GET", "/admin/v1/test")


# ---------------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------------
class TestStatsCommand:
  def test_stats_output(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "id": "sub-1",
        "status": "ACTIVE",
        "plan_name": "ladybug-standard",
        "billing_interval": "monthly",
        "base_price_cents": 9900,
      },
      {
        "id": "sub-2",
        "status": "ACTIVE",
        "plan_name": "ladybug-large",
        "billing_interval": "monthly",
        "base_price_cents": 24900,
      },
      {
        "id": "sub-3",
        "status": "CANCELED",
        "plan_name": "ladybug-standard",
        "billing_interval": "annual",
        "base_price_cents": 0,
      },
    ]

    result = runner.invoke(cli, ["-e", "dev", "stats"])
    assert result.exit_code == 0
    assert "ACTIVE: 2" in result.output
    assert "CANCELED: 1" in result.output
    assert "ladybug-standard: 2" in result.output
    assert "ladybug-large: 1" in result.output
    # MRR = (9900 + 24900) / 100 = $348.00
    assert "$348.00" in result.output

  def test_stats_empty(self, runner, mock_make_request):
    mock_make_request.return_value = []
    result = runner.invoke(cli, ["-e", "dev", "stats"])
    assert result.exit_code == 0
    assert "No subscriptions found" in result.output


# ---------------------------------------------------------------------------
# subscriptions
# ---------------------------------------------------------------------------
class TestSubscriptionsCommands:
  def test_list_default(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "id": "sub-1",
        "resource_id": "kg0000000000000001",
        "owner_email": "user@test.com",
        "status": "ACTIVE",
        "plan_name": "ladybug-standard",
        "billing_interval": "monthly",
        "base_price_cents": 9900,
        "created_at": "2025-01-15T00:00:00Z",
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "subscriptions", "list"])
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "GET",
      "/admin/v1/subscriptions",
      params={"limit": 100, "include_canceled": False},
    )
    assert "sub-1" in result.output
    assert "user@test.com" in result.output

  def test_list_with_filters(self, runner, mock_make_request):
    mock_make_request.return_value = []
    result = runner.invoke(
      cli,
      [
        "-e",
        "dev",
        "subscriptions",
        "list",
        "--status",
        "ACTIVE",
        "--tier",
        "ladybug-large",
      ],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "GET",
      "/admin/v1/subscriptions",
      params={
        "limit": 100,
        "include_canceled": False,
        "status": "ACTIVE",
        "tier": "ladybug-large",
      },
    )

  def test_get(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "id": "sub-1",
      "resource_type": "graph",
      "resource_id": "kg0000000000000001",
      "org_id": "org-1",
      "org_name": "Test Org",
      "owner_name": "John",
      "owner_email": "john@test.com",
      "status": "ACTIVE",
      "plan_name": "ladybug-large",
      "billing_interval": "monthly",
      "base_price_cents": 24900,
      "has_payment_method": True,
      "invoice_billing_enabled": False,
      "started_at": "2025-01-01T00:00:00Z",
      "current_period_start": "2025-02-01T00:00:00Z",
      "current_period_end": "2025-03-01T00:00:00Z",
    }

    result = runner.invoke(cli, ["-e", "dev", "subscriptions", "get", "sub-1"])
    assert result.exit_code == 0
    assert "SUBSCRIPTION DETAILS" in result.output
    assert "sub-1" in result.output
    assert "ladybug-large" in result.output
    assert "john@test.com" in result.output

  def test_create(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "id": "sub-new",
      "resource_type": "graph",
      "resource_id": "kg0000000000000001",
      "org_name": "Test Org",
      "plan_name": "ladybug-standard",
      "status": "ACTIVE",
    }

    result = runner.invoke(
      cli,
      [
        "-e",
        "dev",
        "subscriptions",
        "create",
        "--resource-id",
        "kg0000000000000001",
        "--org-id",
        "org-1",
        "--plan-name",
        "ladybug-standard",
      ],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "POST",
      "/admin/v1/subscriptions",
      data={
        "resource_type": "graph",
        "resource_id": "kg0000000000000001",
        "org_id": "org-1",
        "plan_name": "ladybug-standard",
        "billing_interval": "monthly",
      },
    )
    assert "Created subscription sub-new" in result.output

  def test_audit(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "event_timestamp": "2025-01-15T10:30:00Z",
        "event_type": "CREATED",
        "actor_type": "system",
        "description": "Subscription created",
      }
    ]

    result = runner.invoke(
      cli,
      ["-e", "dev", "subscriptions", "audit", "sub-1", "--event-type", "CREATED"],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "GET",
      "/admin/v1/subscriptions/sub-1/audit",
      params={"limit": 50, "event_type": "CREATED"},
    )
    assert "CREATED" in result.output


# ---------------------------------------------------------------------------
# invoices
# ---------------------------------------------------------------------------
class TestInvoicesCommands:
  def test_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "invoice_number": "INV-001",
        "user_email": "user@test.com",
        "status": "OPEN",
        "total_cents": 9900,
        "due_date": "2025-02-28T00:00:00Z",
        "payment_terms": "net_30",
        "created_at": "2025-01-15T00:00:00Z",
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "invoices", "list"])
    assert result.exit_code == 0
    assert "INV-001" in result.output
    assert "$99.00" in result.output

  def test_get(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "id": "inv-1",
      "invoice_number": "INV-001",
      "status": "PAID",
      "billing_customer_user_id": "user-1",
      "user_name": "John Doe",
      "user_email": "john@test.com",
      "subtotal_cents": 9900,
      "tax_cents": 0,
      "discount_cents": 0,
      "total_cents": 9900,
      "period_start": "2025-01-01T00:00:00Z",
      "period_end": "2025-02-01T00:00:00Z",
      "due_date": "2025-02-01T00:00:00Z",
      "payment_terms": "net_30",
      "paid_at": "2025-01-20T00:00:00Z",
      "payment_method": "bank_transfer",
      "payment_reference": "TXN-123",
      "line_items": [
        {
          "description": "LadybugDB Standard",
          "quantity": 1,
          "unit_price_cents": 9900,
          "amount_cents": 9900,
          "subscription_id": "sub-1",
        }
      ],
    }

    result = runner.invoke(cli, ["-e", "dev", "invoices", "get", "inv-1"])
    assert result.exit_code == 0
    assert "INVOICE DETAILS" in result.output
    assert "INV-001" in result.output
    assert "John Doe" in result.output
    assert "$99.00" in result.output
    assert "bank_transfer" in result.output

  def test_mark_paid(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "invoice_number": "INV-001",
      "user_email": "user@test.com",
      "total_cents": 9900,
      "payment_method": "bank_transfer",
      "payment_reference": "TXN-123",
    }

    result = runner.invoke(
      cli,
      [
        "-e",
        "dev",
        "invoices",
        "mark-paid",
        "inv-1",
        "--payment-method",
        "bank_transfer",
        "--payment-reference",
        "TXN-123",
      ],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "PATCH",
      "/admin/v1/invoices/inv-1/mark-paid",
      params={"payment_method": "bank_transfer", "payment_reference": "TXN-123"},
    )
    assert "Marked invoice INV-001 as paid" in result.output


# ---------------------------------------------------------------------------
# credits
# ---------------------------------------------------------------------------
class TestCreditsCommands:
  def test_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "graph_id": "kg0000000000000001",
        "user_id": "user-1",
        "graph_tier": "ladybug-standard",
        "current_balance": 5000.0,
        "monthly_allocation": 8000.0,
        "credit_multiplier": 1.0,
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "credits", "list"])
    assert result.exit_code == 0
    assert "kg0000000000000001" in result.output
    assert "5,000.00" in result.output

  def test_bonus(self, runner, mock_make_request):
    mock_make_request.return_value = {"current_balance": 6000.0}

    result = runner.invoke(
      cli,
      [
        "-e",
        "dev",
        "credits",
        "bonus",
        "kg0000000000000001",
        "--amount",
        "1000",
        "--description",
        "Promotional credits",
      ],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "POST",
      "/admin/v1/credits/graphs/kg0000000000000001/bonus",
      data={"amount": 1000.0, "description": "Promotional credits"},
    )
    assert "1,000.00 bonus credits" in result.output

  def test_health(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "status": "healthy",
      "total_pools": 10,
      "pools_with_issues": 0,
      "graph_health": {"total_pools": 8, "pools_with_issues": 0},
      "repository_health": {"total_pools": 2, "pools_with_issues": 0},
    }

    result = runner.invoke(cli, ["-e", "dev", "credits", "health"])
    assert result.exit_code == 0
    assert "HEALTHY" in result.output

  def test_health_warning(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "status": "warning",
      "total_pools": 10,
      "pools_with_issues": 2,
      "graph_health": {
        "total_pools": 8,
        "pools_with_issues": 2,
        "low_balance_pools": [
          {
            "graph_id": "kg001",
            "balance": 100.0,
            "allocation": 8000.0,
            "tier": "standard",
          },
        ],
      },
      "repository_health": {"total_pools": 2, "pools_with_issues": 0},
    }

    result = runner.invoke(cli, ["-e", "dev", "credits", "health"])
    assert result.exit_code == 0
    assert "WARNING" in result.output

  def test_repos_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "user_repository_id": "ur-00000000000000001",
        "user_id": "user-00000000000000001",
        "repository_type": "sec",
        "repository_plan": "sec-standard",
        "current_balance": 500.0,
        "monthly_allocation": 1000.0,
        "is_active": True,
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "credits", "repos", "list"])
    assert result.exit_code == 0
    assert "sec" in result.output


# ---------------------------------------------------------------------------
# graphs
# ---------------------------------------------------------------------------
class TestGraphsCommands:
  def test_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "graph_id": "kg0000000000000001",
        "name": "My Graph",
        "graph_tier": "ladybug-standard",
        "backend": "ladybug",
        "status": "active",
        "storage_gb": 0.5,
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "graphs", "list"])
    assert result.exit_code == 0
    assert "kg0000000000000001" in result.output
    assert "My Graph" in result.output
    assert "0.50 GB" in result.output

  def test_analytics(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "total_graphs": 5,
      "by_tier": {"ladybug-standard": 3, "ladybug-large": 2},
      "by_backend": {"ladybug": 5},
      "by_status": {"active": 4, "suspended": 1},
      "total_storage_gb": 12.5,
      "largest_graphs": [
        {"graph_id": "kg0000000000000001", "storage_gb": 5.0},
      ],
    }

    result = runner.invoke(cli, ["-e", "dev", "graphs", "analytics"])
    assert result.exit_code == 0
    assert "TOTAL GRAPHS" in result.output or "5" in result.output
    assert "ladybug-standard: 3" in result.output
    assert "ladybug-large: 2" in result.output
    assert "12.50 GB" in result.output


# ---------------------------------------------------------------------------
# users
# ---------------------------------------------------------------------------
class TestUsersCommands:
  def test_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "id": "user-1",
        "email": "alice@test.com",
        "name": "Alice",
        "email_verified": True,
        "org_role": "owner",
        "created_at": "2025-01-01T00:00:00Z",
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "users", "list"])
    assert result.exit_code == 0
    assert "alice@test.com" in result.output
    assert "Alice" in result.output

  def test_get(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "id": "user-1",
      "email": "alice@test.com",
      "name": "Alice",
      "email_verified": True,
      "org_id": "org-1",
      "org_role": "owner",
      "created_at": "2025-01-01T00:00:00Z",
      "last_login_at": "2025-02-01T00:00:00Z",
    }

    result = runner.invoke(cli, ["-e", "dev", "users", "get", "user-1"])
    assert result.exit_code == 0
    assert "USER DETAILS" in result.output
    assert "alice@test.com" in result.output
    assert "Last Login" in result.output


# ---------------------------------------------------------------------------
# orgs
# ---------------------------------------------------------------------------
class TestOrgsCommands:
  def test_list(self, runner, mock_make_request):
    mock_make_request.return_value = [
      {
        "org_id": "org-1",
        "name": "Acme Corp",
        "org_type": "team",
        "user_count": 5,
        "graph_count": 2,
        "total_credits": 16000.0,
        "created_at": "2025-01-01T00:00:00Z",
      }
    ]

    result = runner.invoke(cli, ["-e", "dev", "orgs", "list"])
    assert result.exit_code == 0
    assert "Acme Corp" in result.output
    assert "org-1" in result.output

  def test_get(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "org_id": "org-1",
      "name": "Acme Corp",
      "org_type": "team",
      "user_count": 5,
      "graph_count": 2,
      "total_credits": 16000.0,
      "has_payment_method": True,
      "invoice_billing_enabled": True,
      "payment_terms": "net_30",
      "billing_email": "billing@acme.com",
      "stripe_customer_id": "cus_123",
      "created_at": "2025-01-01T00:00:00Z",
      "updated_at": "2025-02-01T00:00:00Z",
    }

    result = runner.invoke(cli, ["-e", "dev", "orgs", "get", "org-1"])
    assert result.exit_code == 0
    assert "ORGANIZATION DETAILS" in result.output
    assert "Acme Corp" in result.output
    assert "Invoice Billing: Yes" in result.output
    assert "billing@acme.com" in result.output

  def test_update(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "org_id": "org-1",
      "name": "Acme Corp",
      "invoice_billing_enabled": True,
      "payment_terms": "net_60",
      "billing_email": "new@acme.com",
    }

    result = runner.invoke(
      cli,
      [
        "-e",
        "dev",
        "orgs",
        "update",
        "org-1",
        "--invoice-billing",
        "--billing-email",
        "new@acme.com",
        "--payment-terms",
        "net_60",
      ],
    )
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with(
      "PATCH",
      "/admin/v1/orgs/org-1",
      params={
        "invoice_billing_enabled": True,
        "billing_email": "new@acme.com",
        "payment_terms": "net_60",
      },
    )
    assert "Updated org org-1" in result.output


# ---------------------------------------------------------------------------
# cache
# ---------------------------------------------------------------------------
class TestCacheCommands:
  def test_info_all(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "databases": [
        {
          "db_number": 0,
          "name": "auth",
          "key_count": 10,
          "purpose": "Authentication tokens",
        },
        {
          "db_number": 1,
          "name": "rate_limits",
          "key_count": 50,
          "purpose": "Rate limiting",
        },
      ],
      "total_keys": 60,
    }

    result = runner.invoke(cli, ["-e", "dev", "cache", "info"])
    assert result.exit_code == 0
    assert "auth" in result.output
    assert "rate_limits" in result.output
    assert "60" in result.output

  def test_info_single(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "name": "auth",
      "db_number": 0,
      "key_count": 10,
      "purpose": "Authentication tokens",
      "sample_keys": ["apikey:abc123", "session:def456"],
    }

    result = runner.invoke(cli, ["-e", "dev", "cache", "info", "auth"])
    assert result.exit_code == 0
    assert "AUTH" in result.output
    assert "apikey:abc123" in result.output

  def test_flush_with_confirm(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "name": "auth",
      "db_number": 0,
      "keys_before": 10,
    }

    result = runner.invoke(cli, ["-e", "dev", "cache", "flush", "auth"], input="y\n")
    assert result.exit_code == 0
    assert "10" in result.output

  def test_flush_with_yes_flag(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "name": "auth",
      "db_number": 0,
      "keys_before": 10,
    }

    result = runner.invoke(cli, ["-e", "dev", "cache", "flush", "auth", "--yes"])
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with("POST", "/admin/v1/cache/flush/auth")

  def test_flush_all(self, runner, mock_make_request):
    mock_make_request.return_value = {
      "databases": [
        {"name": "auth", "keys_before": 10, "flushed": True},
        {"name": "rate_limits", "keys_before": 50, "flushed": True},
      ],
      "total_keys_flushed": 60,
    }

    result = runner.invoke(cli, ["-e", "dev", "cache", "flush", "all", "--yes"])
    assert result.exit_code == 0
    mock_make_request.assert_called_once_with("POST", "/admin/v1/cache/flush-all")
    assert "60" in result.output

  def test_delete_keys_wildcard_rejected(self, runner, mock_make_request):
    result = runner.invoke(
      cli, ["-e", "dev", "cache", "delete-keys", "auth", "--pattern", "*", "--yes"]
    )
    assert result.exit_code == 1
    assert "not allowed" in result.output


# ---------------------------------------------------------------------------
# migrations
# ---------------------------------------------------------------------------
class TestMigrationsCommands:
  def test_up_dev(self, runner):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Running upgrade... head\n"
    mock_result.stderr = ""

    with patch(
      "robosystems.admin.cli.subprocess.run", return_value=mock_result
    ) as mock_run:
      result = runner.invoke(cli, ["-e", "dev", "migrations", "up"])
      assert result.exit_code == 0
      mock_run.assert_called_once_with(
        ["uv", "run", "alembic", "upgrade", "head"],
        capture_output=True,
        text=True,
      )

  def test_down_dev(self, runner):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Rolling back...\n"
    mock_result.stderr = ""

    with patch(
      "robosystems.admin.cli.subprocess.run", return_value=mock_result
    ) as mock_run:
      result = runner.invoke(cli, ["-e", "dev", "migrations", "down"])
      assert result.exit_code == 0
      mock_run.assert_called_once_with(
        ["uv", "run", "alembic", "downgrade", "-1"],
        capture_output=True,
        text=True,
      )

  def test_current_dev(self, runner):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "abc123 (head)\n"
    mock_result.stderr = ""

    with patch(
      "robosystems.admin.cli.subprocess.run", return_value=mock_result
    ) as mock_run:
      result = runner.invoke(cli, ["-e", "dev", "migrations", "current"])
      assert result.exit_code == 0
      mock_run.assert_called_once_with(
        ["uv", "run", "alembic", "current"],
        capture_output=True,
        text=True,
      )

  def test_up_prod(self, runner):
    with patch("robosystems.admin.cli.SSMExecutor") as MockSSM:
      mock_executor = MagicMock()
      mock_executor.execute.return_value = ("Migration done", "", 0)
      MockSSM.return_value = mock_executor

      result = runner.invoke(cli, ["-e", "prod", "migrations", "up"])
      assert result.exit_code == 0
      MockSSM.assert_called_once_with("prod")
      mock_executor.execute.assert_called_once_with(
        "/usr/local/bin/run-migrations.sh --command 'upgrade head'"
      )


# ---------------------------------------------------------------------------
# SSMExecutor
# ---------------------------------------------------------------------------
class TestSSMExecutor:
  def test_get_bastion_instance(self):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "i-0abc123def456\n"

    with patch("subprocess.run", return_value=mock_result):
      executor = SSMExecutor(environment="prod")
      instance_id = executor._get_bastion_instance()
      assert instance_id == "i-0abc123def456"
      # Verify caching
      assert executor.instance_id == "i-0abc123def456"
      instance_id_2 = executor._get_bastion_instance()
      assert instance_id_2 == "i-0abc123def456"

  def test_get_bastion_instance_not_found(self):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "None\n"

    with patch("subprocess.run", return_value=mock_result):
      executor = SSMExecutor(environment="staging")
      with pytest.raises(RuntimeError, match="Bastion instance not found"):
        executor._get_bastion_instance()

  def test_ensure_instance_running_already_running(self):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "running\n"

    with patch("subprocess.run", return_value=mock_result) as mock_run:
      executor = SSMExecutor(environment="prod")
      executor._ensure_instance_running("i-0abc123")
      # Only one call: describe-instances to check state. No start-instances call.
      assert mock_run.call_count == 1

  def test_execute_success(self):
    describe_result = MagicMock()
    describe_result.returncode = 0
    describe_result.stdout = "i-0abc123\n"

    state_result = MagicMock()
    state_result.returncode = 0
    state_result.stdout = "running\n"

    send_result = MagicMock()
    send_result.returncode = 0
    send_result.stdout = "cmd-123456\n"

    wait_result = MagicMock()
    wait_result.returncode = 0

    invocation_result = MagicMock()
    invocation_result.returncode = 0
    invocation_result.stdout = json.dumps(
      {
        "StandardOutputContent": "migration output",
        "StandardErrorContent": "",
        "ResponseCode": 0,
      }
    )

    with patch("subprocess.run") as mock_run:
      mock_run.side_effect = [
        describe_result,  # _get_bastion_instance
        state_result,  # _ensure_instance_running (describe state)
        send_result,  # send-command
        wait_result,  # wait command-executed
        invocation_result,  # get-command-invocation
      ]

      executor = SSMExecutor(environment="prod")
      stdout, stderr, exit_code = executor.execute("echo hello")

      assert stdout == "migration output"
      assert stderr == ""
      assert exit_code == 0
      assert mock_run.call_count == 5
