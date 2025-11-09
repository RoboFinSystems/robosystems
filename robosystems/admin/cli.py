"""RoboSystems Admin CLI for remote administration via admin API.

This CLI provides remote access to admin operations without requiring bastion host access.
It fetches the admin API key from AWS Secrets Manager using AWS CLI and provides commands
for subscription management, customer management, credit management, graph management,
and user management.

Usage:
    uv run python -m robosystems.admin.cli [command] [options]

Examples:
    # List all subscriptions
    uv run python -m robosystems.admin.cli subscriptions list

    # Get subscription details
    uv run python -m robosystems.admin.cli subscriptions get <subscription-id>

    # Update org billing settings
    uv run python -m robosystems.admin.cli orgs update <org-id> --billing-email new@example.com

    # Show statistics
    uv run python -m robosystems.admin.cli stats
"""

import subprocess
import json
import os
import click
import requests
from typing import Optional, Dict, Any
from rich.console import Console
from rich.table import Table

from ..logger import get_logger
from .ssm_executor import SSMExecutor

logger = get_logger(__name__)
console = Console()


class AdminAPIClient:
  """Client for interacting with the RoboSystems admin API."""

  def __init__(
    self,
    environment: str = "prod",
    api_base_url: Optional[str] = None,
    aws_profile: str = "robosystems",
  ):
    """Initialize the admin API client.

    Args:
        environment: Environment name (dev/staging/prod)
        api_base_url: Base URL for the API (default: auto-detect from environment)
        aws_profile: AWS CLI profile name (default: robosystems)
    """
    self.environment = environment
    self.aws_profile = aws_profile

    if api_base_url:
      self.api_base_url = api_base_url
    elif environment == "dev":
      self.api_base_url = "http://localhost:8000"
    elif environment == "staging":
      self.api_base_url = "https://api.staging.robosystems.ai"
    else:
      self.api_base_url = "https://api.robosystems.ai"

    self.admin_key = self._get_admin_key()

  def _get_admin_key(self) -> str:
    """Get the admin API key from environment variable (dev) or AWS Secrets Manager.

    Returns:
        The admin API key

    Raises:
        ClickException: If key retrieval fails
    """
    admin_key = os.getenv("ADMIN_API_KEY")
    if admin_key:
      console.print(
        f"[green]✓[/green] Connected to {self.environment} environment admin API (using ADMIN_API_KEY from environment)"
      )
      return admin_key

    secret_id = f"robosystems/{self.environment}/admin"

    try:
      cmd = [
        "aws",
        "secretsmanager",
        "get-secret-value",
        "--secret-id",
        secret_id,
        "--profile",
        self.aws_profile,
        "--region",
        "us-east-1",
        "--query",
        "SecretString",
        "--output",
        "text",
      ]

      result = subprocess.run(cmd, capture_output=True, text=True, check=True)

      secret_data = json.loads(result.stdout)
      admin_key = secret_data.get("ADMIN_API_KEY")

      if not admin_key:
        raise click.ClickException(f"ADMIN_API_KEY not found in secret {secret_id}")

      console.print(
        f"[green]✓[/green] Connected to {self.environment} environment admin API (using AWS Secrets Manager)"
      )
      return admin_key

    except subprocess.CalledProcessError as e:
      error_msg = e.stderr.strip() if e.stderr else "Unknown error"
      raise click.ClickException(
        f"Failed to retrieve admin key from AWS Secrets Manager:\n{error_msg}\n\n"
        f"Ensure you have:\n"
        f"  1. AWS CLI configured with profile '{self.aws_profile}'\n"
        f"  2. Permissions to access secret '{secret_id}'\n"
        f"  3. Valid AWS credentials\n"
        f"\nAlternatively, for local development:\n"
        f"  Set ADMIN_API_KEY environment variable in your .env file"
      )
    except json.JSONDecodeError:
      raise click.ClickException(
        f'Invalid JSON in secret {secret_id}. Expected format: {{"ADMIN_API_KEY": "..."}}'
      )
    except Exception as e:
      raise click.ClickException(f"Error fetching admin key: {str(e)}")

  def _make_request(
    self,
    method: str,
    endpoint: str,
    data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """Make an authenticated request to the admin API.

    Args:
        method: HTTP method
        endpoint: API endpoint path
        data: Request body data
        params: Query parameters

    Returns:
        Response data

    Raises:
        ClickException: If the request fails
    """
    url = f"{self.api_base_url}{endpoint}"
    headers = {
      "Authorization": f"Bearer {self.admin_key}",
      "Content-Type": "application/json",
    }

    try:
      response = requests.request(
        method=method,
        url=url,
        headers=headers,
        json=data,
        params=params,
        timeout=30,
      )

      if response.status_code == 401:
        raise click.ClickException(
          "Authentication failed. Admin API key may be invalid or expired."
        )
      elif response.status_code == 403:
        raise click.ClickException("Permission denied. Insufficient admin privileges.")
      elif response.status_code == 404:
        raise click.ClickException("Resource not found.")
      elif response.status_code == 422:
        error_detail = response.json().get("detail", response.text)
        raise click.ClickException(f"Validation error: {error_detail}")
      elif not response.ok:
        raise click.ClickException(
          f"Request failed ({response.status_code}): {response.text}"
        )

      return response.json() if response.text else {}

    except requests.Timeout:
      raise click.ClickException(
        f"Request timed out. API may be unavailable at {self.api_base_url}"
      )
    except requests.ConnectionError:
      raise click.ClickException(
        f"Connection failed. Unable to reach API at {self.api_base_url}"
      )
    except requests.RequestException as e:
      raise click.ClickException(f"Network error: {str(e)}")


@click.group()
@click.option(
  "--environment",
  "-e",
  default="prod",
  type=click.Choice(["dev", "staging", "prod"]),
  help="Environment to connect to (dev=localhost:8000, staging/prod=remote)",
)
@click.option(
  "--api-url",
  help="Override API base URL (default: auto-detect from environment)",
)
@click.option(
  "--aws-profile",
  default="robosystems",
  help="AWS CLI profile name (only used for staging/prod)",
)
@click.pass_context
def cli(ctx, environment, api_url, aws_profile):
  """RoboSystems Admin CLI - Remote administration via admin API.

  This CLI provides access to subscription management, customer management,
  credit management, graph management, and user management.

  Environment selection:
    - dev: Uses localhost:8000 and ADMIN_API_KEY from .env.local
    - staging/prod: Uses remote URLs and AWS Secrets Manager for auth
  """
  ctx.obj = AdminAPIClient(
    environment=environment,
    api_base_url=api_url,
    aws_profile=aws_profile,
  )


@cli.group()
def subscriptions():
  """Manage graph subscriptions."""
  pass


@subscriptions.command("list")
@click.option("--status", help="Filter by status (ACTIVE, PAUSED, CANCELED)")
@click.option("--tier", help="Filter by tier (KUZU_STANDARD, KUZU_LARGE, KUZU_XLARGE)")
@click.option("--email", help="Filter by owner email")
@click.option("--include-canceled", is_flag=True, help="Include canceled subscriptions")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_subscriptions(client, status, tier, email, include_canceled, limit):
  """List all graph subscriptions."""
  params = {
    "limit": limit,
    "include_canceled": include_canceled,
  }
  if status:
    params["status"] = status
  if tier:
    params["tier"] = tier
  if email:
    params["owner_email"] = email

  subscriptions = client._make_request("GET", "/admin/v1/subscriptions", params=params)

  if not subscriptions:
    console.print("\n[yellow]No subscriptions found.[/yellow]")
    return

  table = Table(title="Subscriptions", show_header=True, header_style="bold cyan")
  table.add_column("ID", no_wrap=True)
  table.add_column("Resource", overflow="fold")
  table.add_column("Customer", overflow="fold")
  table.add_column("Status", overflow="fold")
  table.add_column("Plan", overflow="fold")
  table.add_column("Interval", overflow="fold")
  table.add_column("Price", justify="right")
  table.add_column("Created", overflow="fold")

  for sub in subscriptions:
    table.add_row(
      sub["id"],
      sub["resource_id"],
      sub.get("owner_email", "N/A"),
      sub["status"],
      sub["plan_name"],
      sub.get("billing_interval", "N/A"),
      f"${sub['base_price_cents'] / 100:.2f}"
      if sub.get("base_price_cents")
      else "Free",
      sub["created_at"][:10],
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(subscriptions):,} subscriptions")


@subscriptions.command("get")
@click.argument("subscription_id")
@click.pass_obj
def get_subscription(client, subscription_id):
  """Get details of a specific subscription."""
  sub = client._make_request("GET", f"/admin/v1/subscriptions/{subscription_id}")

  click.echo("\nSUBSCRIPTION DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nID: {sub['id']}")
  click.echo(f"Resource: {sub['resource_type']} / {sub['resource_id']}")
  click.echo(f"Org: {sub.get('org_name', 'N/A')} ({sub['org_id']})")
  click.echo(f"Owner: {sub.get('owner_name', 'N/A')} ({sub.get('owner_email', 'N/A')})")
  click.echo(f"Status: {sub['status']}")

  click.echo("\nBILLING")
  click.echo(f"  Plan: {sub['plan_name']}")
  click.echo(f"  Interval: {sub['billing_interval']}")
  click.echo(
    f"  Base Price: ${sub['base_price_cents'] / 100:.2f}"
    if sub.get("base_price_cents")
    else "  Base Price: Free"
  )
  click.echo(f"  Payment Method: {'Yes' if sub.get('has_payment_method') else 'No'}")
  click.echo(
    f"  Invoice Billing: {'Yes' if sub.get('invoice_billing_enabled') else 'No'}"
  )

  click.echo("\nDATES")
  if sub.get("started_at"):
    click.echo(f"  Started: {sub['started_at'][:10]}")
  if sub.get("current_period_start"):
    click.echo(
      f"  Current Period: {sub['current_period_start'][:10]} to {sub['current_period_end'][:10]}"
    )
  if sub.get("canceled_at"):
    click.echo(f"  Canceled: {sub['canceled_at'][:10]}")
  if sub.get("ends_at"):
    click.echo(f"  Ends: {sub['ends_at'][:10]}")

  if sub.get("stripe_subscription_id"):
    click.echo("\nSTRIPE")
    click.echo(f"  Subscription ID: {sub['stripe_subscription_id']}")


@subscriptions.command("create")
@click.option("--resource-id", required=True, help="Resource ID (graph ID)")
@click.option("--org-id", required=True, help="Organization ID")
@click.option(
  "--plan-name",
  required=True,
  help="Plan name (e.g., kuzu-standard, kuzu-large, kuzu-xlarge)",
)
@click.option(
  "--resource-type",
  default="graph",
  help="Resource type (default: graph)",
)
@click.option(
  "--billing-interval",
  default="monthly",
  type=click.Choice(["monthly", "annual"]),
  help="Billing interval",
)
@click.pass_obj
def create_subscription(
  client,
  resource_id,
  org_id,
  plan_name,
  resource_type,
  billing_interval,
):
  """Create a new subscription."""
  data = {
    "resource_type": resource_type,
    "resource_id": resource_id,
    "org_id": org_id,
    "plan_name": plan_name,
    "billing_interval": billing_interval,
  }

  sub = client._make_request("POST", "/admin/v1/subscriptions", data=data)

  click.echo(f"✅ Created subscription {sub['id']}")
  click.echo(f"   Resource: {sub['resource_type']} / {sub['resource_id']}")
  click.echo(f"   Org: {sub.get('org_name', org_id)}")
  click.echo(f"   Plan: {sub['plan_name']}")
  click.echo(f"   Status: {sub['status']}")


@subscriptions.command("update")
@click.argument("subscription_id")
@click.option(
  "--status", type=click.Choice(["ACTIVE", "PAUSED", "CANCELED"]), help="New status"
)
@click.option(
  "--plan-name",
  help="New plan name (e.g., kuzu-standard, kuzu-large, kuzu-xlarge)",
)
@click.option("--price", type=float, help="New base price in dollars")
@click.pass_obj
def update_subscription(
  client,
  subscription_id,
  status,
  plan_name,
  price,
):
  """Update an existing subscription."""
  data = {}

  if status:
    data["status"] = status
  if plan_name:
    data["plan_name"] = plan_name
  if price is not None:
    data["base_price_cents"] = int(price * 100)

  if not data:
    click.echo("❌ No updates specified")
    return

  sub = client._make_request(
    "PATCH", f"/admin/v1/subscriptions/{subscription_id}", data=data
  )

  click.echo(f"✅ Updated subscription {sub['id']}")
  click.echo(f"   Status: {sub['status']}")
  if plan_name:
    click.echo(f"   New Plan: {sub['plan_name']}")
  if price is not None:
    click.echo(f"   New Price: ${sub['base_price_cents'] / 100:.2f}")


@subscriptions.command("audit")
@click.argument("subscription_id")
@click.option("--event-type", help="Filter by event type")
@click.option("--limit", default=50, help="Maximum number of events")
@click.pass_obj
def subscription_audit(client, subscription_id, event_type, limit):
  """View audit log for a subscription."""
  params = {"limit": limit}
  if event_type:
    params["event_type"] = event_type

  events = client._make_request(
    "GET", f"/admin/v1/subscriptions/{subscription_id}/audit", params=params
  )

  if not events:
    console.print("\n[yellow]No audit events found.[/yellow]")
    return

  table = Table(title="Audit Log", show_header=True, header_style="bold cyan")
  table.add_column("Timestamp", overflow="fold")
  table.add_column("Event", overflow="fold")
  table.add_column("Actor", overflow="fold")
  table.add_column("Description", overflow="fold")

  for event in events:
    table.add_row(
      event["event_timestamp"][:19],
      event["event_type"],
      event.get("actor_type", "system"),
      event.get("description", "")[:50],
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(events):,} events")


@cli.group()
def invoices():
  """Manage customer invoices."""
  pass


@invoices.command("list")
@click.option("--status", help="Filter by status (DRAFT, OPEN, PAID, VOID)")
@click.option("--user-id", help="Filter by user ID")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_invoices(client, status, user_id, limit):
  """List all invoices."""
  params = {"limit": limit}
  if status:
    params["status"] = status
  if user_id:
    params["user_id"] = user_id

  invoices = client._make_request("GET", "/admin/v1/invoices", params=params)

  if not invoices:
    console.print("\n[yellow]No invoices found.[/yellow]")
    return

  table = Table(title="Invoices", show_header=True, header_style="bold cyan")
  table.add_column("Invoice #", no_wrap=True)
  table.add_column("Customer", overflow="fold")
  table.add_column("Status", overflow="fold")
  table.add_column("Total", justify="right")
  table.add_column("Due Date", overflow="fold")
  table.add_column("Payment Terms", overflow="fold")
  table.add_column("Created", overflow="fold")

  for invoice in invoices:
    table.add_row(
      invoice["invoice_number"],
      invoice.get("user_email", "N/A"),
      invoice["status"],
      f"${invoice['total_cents'] / 100:.2f}",
      invoice["due_date"][:10] if invoice.get("due_date") else "N/A",
      invoice["payment_terms"],
      invoice["created_at"][:10],
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(invoices):,} invoices")


@invoices.command("get")
@click.argument("invoice_id")
@click.pass_obj
def get_invoice(client, invoice_id):
  """Get details of a specific invoice."""
  invoice = client._make_request("GET", f"/admin/v1/invoices/{invoice_id}")

  click.echo("\nINVOICE DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nInvoice Number: {invoice['invoice_number']}")
  click.echo(f"ID: {invoice['id']}")
  click.echo(f"Status: {invoice['status']}")

  click.echo("\nCUSTOMER")
  click.echo(f"  Name: {invoice.get('user_name', 'N/A')}")
  click.echo(f"  Email: {invoice.get('user_email', 'N/A')}")
  click.echo(f"  User ID: {invoice['billing_customer_user_id']}")

  click.echo("\nAMOUNTS")
  click.echo(f"  Subtotal: ${invoice['subtotal_cents'] / 100:.2f}")
  if invoice["tax_cents"] > 0:
    click.echo(f"  Tax: ${invoice['tax_cents'] / 100:.2f}")
  if invoice["discount_cents"] > 0:
    click.echo(f"  Discount: -${invoice['discount_cents'] / 100:.2f}")
  click.echo(f"  Total: ${invoice['total_cents'] / 100:.2f}")

  click.echo("\nDATES")
  click.echo(
    f"  Period: {invoice['period_start'][:10]} to {invoice['period_end'][:10]}"
  )
  if invoice.get("due_date"):
    click.echo(f"  Due Date: {invoice['due_date'][:10]}")
  click.echo(f"  Payment Terms: {invoice['payment_terms']}")
  if invoice.get("paid_at"):
    click.echo(f"  Paid: {invoice['paid_at'][:10]}")

  if invoice.get("payment_method"):
    click.echo("\nPAYMENT")
    click.echo(f"  Method: {invoice['payment_method']}")
    if invoice.get("payment_reference"):
      click.echo(f"  Reference: {invoice['payment_reference']}")

  click.echo("\nLINE ITEMS")
  for item in invoice["line_items"]:
    click.echo(f"  - {item['description']}")
    click.echo(
      f"    Quantity: {item['quantity']} x ${item['unit_price_cents'] / 100:.2f} = ${item['amount_cents'] / 100:.2f}"
    )
    if item.get("subscription_id"):
      click.echo(f"    Subscription: {item['subscription_id']}")


@invoices.command("mark-paid")
@click.argument("invoice_id")
@click.option(
  "--payment-method", required=True, help="Payment method (e.g., bank_transfer, check)"
)
@click.option("--payment-reference", help="Payment reference or transaction ID")
@click.pass_obj
def mark_invoice_paid(client, invoice_id, payment_method, payment_reference):
  """Mark an invoice as paid."""
  params = {"payment_method": payment_method}
  if payment_reference:
    params["payment_reference"] = payment_reference

  invoice = client._make_request(
    "PATCH", f"/admin/v1/invoices/{invoice_id}/mark-paid", params=params
  )

  click.echo(f"✅ Marked invoice {invoice['invoice_number']} as paid")
  click.echo(f"   Customer: {invoice.get('user_email', 'N/A')}")
  click.echo(f"   Amount: ${invoice['total_cents'] / 100:.2f}")
  click.echo(f"   Payment Method: {invoice['payment_method']}")
  if payment_reference:
    click.echo(f"   Reference: {invoice['payment_reference']}")


@cli.command()
@click.pass_obj
def stats(client):
  """Show subscription and customer statistics."""
  all_subs = client._make_request(
    "GET", "/admin/v1/subscriptions", params={"limit": 1000}
  )

  if not all_subs:
    console.print("\n[yellow]No subscriptions found.[/yellow]")
    return

  stats_data = {
    "total": len(all_subs),
    "by_status": {},
    "by_tier": {},
    "by_billing": {},
    "revenue": 0,
  }

  for sub in all_subs:
    status = sub["status"]
    stats_data["by_status"][status] = stats_data["by_status"].get(status, 0) + 1

    plan = sub["plan_name"]
    stats_data["by_tier"][plan] = stats_data["by_tier"].get(plan, 0) + 1

    interval = sub.get("billing_interval", "monthly")
    stats_data["by_billing"][interval] = stats_data["by_billing"].get(interval, 0) + 1

    if sub["status"] == "ACTIVE" and sub.get("base_price_cents"):
      stats_data["revenue"] += sub["base_price_cents"]

  console.print()
  console.print("[bold cyan]SUBSCRIPTION STATISTICS[/bold cyan]")
  console.print("=" * 60)

  console.print(f"\n[bold]TOTAL:[/bold] {stats_data['total']:,} subscriptions")

  console.print("\n[bold]BY STATUS:[/bold]")
  for status, count in sorted(stats_data["by_status"].items()):
    console.print(f"  {status}: {count:,}")

  console.print("\n[bold]BY PLAN:[/bold]")
  for plan, count in sorted(stats_data["by_tier"].items()):
    console.print(f"  {plan}: {count:,}")

  console.print("\n[bold]BY BILLING INTERVAL:[/bold]")
  for interval, count in sorted(stats_data["by_billing"].items()):
    console.print(f"  {interval}: {count:,}")

  console.print(
    f"\n[bold]MONTHLY REVENUE:[/bold] [green]${stats_data['revenue'] / 100:,.2f}[/green]"
  )


@cli.group()
def credits():
  """Manage credit pools."""
  pass


@credits.command("list")
@click.option("--user-email", help="Filter by user email")
@click.option("--tier", help="Filter by tier")
@click.option("--low-balance", is_flag=True, help="Only show low balance pools")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_credits(client, user_email, tier, low_balance, limit):
  """List all graph credit pools."""
  params = {
    "limit": limit,
    "low_balance_only": low_balance,
  }
  if user_email:
    params["user_email"] = user_email
  if tier:
    params["tier"] = tier

  pools = client._make_request("GET", "/admin/v1/credits/graphs", params=params)

  if not pools:
    console.print("\n[yellow]No credit pools found.[/yellow]")
    return

  table = Table(title="Credit Pools", show_header=True, header_style="bold cyan")
  table.add_column("Graph ID", no_wrap=True)
  table.add_column("User ID", overflow="fold")
  table.add_column("Tier", overflow="fold")
  table.add_column("Balance", justify="right")
  table.add_column("Allocation", justify="right")
  table.add_column("Multiplier", justify="right")

  for pool in pools:
    table.add_row(
      pool["graph_id"],
      pool.get("user_id", "N/A"),
      pool["graph_tier"],
      f"{pool['current_balance']:,.2f}",
      f"{pool['monthly_allocation']:,.2f}",
      f"{pool['credit_multiplier']:.2f}x",
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(pools):,} credit pools")


@credits.command("get")
@click.argument("graph_id")
@click.pass_obj
def get_credits(client, graph_id):
  """Get details of a specific credit pool."""
  pool = client._make_request("GET", f"/admin/v1/credits/graphs/{graph_id}")

  click.echo("\nCREDIT POOL DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nGraph ID: {pool['graph_id']}")
  click.echo(f"User ID: {pool.get('user_id', 'N/A')}")
  click.echo(f"Tier: {pool['graph_tier']}")

  click.echo("\nCREDITS")
  click.echo(f"  Current Balance: {pool['current_balance']:,.2f}")
  click.echo(f"  Monthly Allocation: {pool['monthly_allocation']:,.2f}")
  click.echo(f"  Credit Multiplier: {pool['credit_multiplier']:.2f}x")

  if pool.get("storage_limit_override_gb"):
    click.echo(f"  Storage Limit Override: {pool['storage_limit_override_gb']:.2f} GB")


@credits.command("bonus")
@click.argument("graph_id")
@click.option("--amount", type=float, required=True, help="Amount of credits to add")
@click.option("--description", required=True, help="Reason for bonus credits")
@click.pass_obj
def add_bonus_credits(client, graph_id, amount, description):
  """Add bonus credits to a graph."""
  data = {
    "amount": amount,
    "description": description,
  }

  pool = client._make_request(
    "POST", f"/admin/v1/credits/graphs/{graph_id}/bonus", data=data
  )

  click.echo(f"✅ Added {amount:,.2f} bonus credits to graph {graph_id}")
  click.echo(f"   New balance: {pool['current_balance']:,.2f}")
  click.echo(f"   Description: {description}")


@credits.command("analytics")
@click.option("--tier", help="Filter by tier")
@click.pass_obj
def credits_analytics(client, tier):
  """Get system-wide credit analytics."""
  params = {}
  if tier:
    params["tier"] = tier

  analytics = client._make_request("GET", "/admin/v1/credits/analytics", params=params)

  console.print()
  console.print("[bold cyan]CREDIT ANALYTICS[/bold cyan]")
  console.print("=" * 60)

  console.print("\n[bold]OVERALL TOTALS:[/bold]")
  console.print(f"  Total Pools: {analytics['total_pools']:,}")
  console.print(
    f"  Total Monthly Allocation: {analytics['total_allocated_monthly']:,.2f}"
  )
  console.print(f"  Total Current Balance: {analytics['total_current_balance']:,.2f}")
  console.print(f"  Consumed This Month: {analytics['total_consumed_month']:,.2f}")

  graph_credits = analytics.get("graph_credits", {})
  if graph_credits:
    console.print("\n[bold]GRAPH CREDITS:[/bold]")
    console.print(f"  Pools: {graph_credits.get('total_pools', 0):,}")
    console.print(
      f"  Allocation: {graph_credits.get('total_allocated_monthly', 0):,.2f}"
    )
    console.print(f"  Balance: {graph_credits.get('total_current_balance', 0):,.2f}")
    console.print(f"  Consumed: {graph_credits.get('total_consumed_month', 0):,.2f}")

    if graph_credits.get("top_consumers"):
      console.print("\n  [bold]Top Consumers:[/bold]")
      for consumer in graph_credits["top_consumers"][:5]:
        console.print(
          f"    {consumer['graph_id'][:30]}: {consumer['consumed']:,.2f} credits ({consumer['tier']})"
        )

    if graph_credits.get("by_tier"):
      console.print("\n  [bold]By Tier:[/bold]")
      for tier_name, stats in sorted(graph_credits["by_tier"].items()):
        console.print(
          f"    {tier_name}: {stats['pool_count']:,} pools, {stats['total_current_balance']:,.2f} balance"
        )

  repo_credits = analytics.get("repository_credits", {})
  if repo_credits:
    console.print("\n[bold]REPOSITORY CREDITS:[/bold]")
    console.print(f"  Pools: {repo_credits.get('total_pools', 0):,}")
    console.print(
      f"  Allocation: {repo_credits.get('total_allocated_monthly', 0):,.2f}"
    )
    console.print(f"  Balance: {repo_credits.get('total_current_balance', 0):,.2f}")
    console.print(f"  Consumed: {repo_credits.get('total_consumed_month', 0):,.2f}")

    if repo_credits.get("by_type"):
      console.print("\n  [bold]By Type:[/bold]")
      for repo_type, stats in sorted(repo_credits["by_type"].items()):
        console.print(
          f"    {repo_type}: {stats['pool_count']:,} pools, {stats['total_current_balance']:,.2f} balance"
        )


@credits.command("health")
@click.pass_obj
def credits_health(client):
  """Check credit system health."""
  health = client._make_request("GET", "/admin/v1/credits/health")

  console.print()
  console.print("[bold cyan]CREDIT SYSTEM HEALTH[/bold cyan]")
  console.print("=" * 60)

  status_color = (
    "green"
    if health["status"] == "healthy"
    else "yellow"
    if health["status"] == "warning"
    else "red"
  )
  console.print(
    f"\n[bold]Status:[/bold] [{status_color}]{health['status'].upper()}[/{status_color}]"
  )
  console.print(f"[bold]Total Pools:[/bold] {health['total_pools']:,}")
  console.print(f"[bold]Pools with Issues:[/bold] {health['pools_with_issues']:,}")

  graph_health = health.get("graph_health", {})
  repo_health = health.get("repository_health", {})

  console.print(
    f"\n[bold]Graph Pools:[/bold] {graph_health.get('total_pools', 0):,} ({graph_health.get('pools_with_issues', 0):,} issues)"
  )
  console.print(
    f"[bold]Repository Pools:[/bold] {repo_health.get('total_pools', 0):,} ({repo_health.get('pools_with_issues', 0):,} issues)"
  )

  if graph_health.get("negative_balance_pools"):
    console.print(
      f"\n[bold red]GRAPH NEGATIVE BALANCE POOLS ({len(graph_health['negative_balance_pools'])}):[/bold red]"
    )
    for pool in graph_health["negative_balance_pools"][:10]:
      console.print(
        f"  {pool['graph_id']}: {pool['balance']:,.2f} credits ({pool['tier']})"
      )

  if repo_health.get("negative_balance_pools"):
    console.print(
      f"\n[bold red]REPOSITORY NEGATIVE BALANCE POOLS ({len(repo_health['negative_balance_pools'])}):[/bold red]"
    )
    for pool in repo_health["negative_balance_pools"][:10]:
      console.print(
        f"  {pool['user_repository_id']}: {pool['balance']:,.2f} credits ({pool['repository_type']})"
      )

  if graph_health.get("low_balance_pools"):
    console.print(
      f"\n[bold yellow]GRAPH LOW BALANCE POOLS ({len(graph_health['low_balance_pools'])}):[/bold yellow]"
    )
    for pool in graph_health["low_balance_pools"][:10]:
      console.print(
        f"  {pool['graph_id']}: {pool['balance']:,.2f} / {pool['allocation']:,.2f} ({pool['tier']})"
      )

  if repo_health.get("low_balance_pools"):
    console.print(
      f"\n[bold yellow]REPOSITORY LOW BALANCE POOLS ({len(repo_health['low_balance_pools'])}):[/bold yellow]"
    )
    for pool in repo_health["low_balance_pools"][:10]:
      console.print(
        f"  {pool['user_repository_id']}: {pool['balance']:,.2f} / {pool['allocation']:,.2f} ({pool['repository_type']})"
      )


@credits.group("repos")
def credits_repos():
  """Manage repository credit pools."""
  pass


@credits_repos.command("list")
@click.option("--user-email", help="Filter by user email")
@click.option(
  "--repository-type", help="Filter by repository type (sec, industry, economic)"
)
@click.option("--low-balance", is_flag=True, help="Only show low balance pools")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_repository_credits(client, user_email, repository_type, low_balance, limit):
  """List all repository credit pools."""
  params = {
    "limit": limit,
    "low_balance_only": low_balance,
  }
  if user_email:
    params["user_email"] = user_email
  if repository_type:
    params["repository_type"] = repository_type

  pools = client._make_request("GET", "/admin/v1/credits/repositories", params=params)

  if not pools:
    console.print("\n[yellow]No repository credit pools found.[/yellow]")
    return

  table = Table(
    title="Repository Credit Pools", show_header=True, header_style="bold cyan"
  )
  table.add_column("User Repo ID", no_wrap=True)
  table.add_column("User ID", overflow="fold")
  table.add_column("Repository", overflow="fold")
  table.add_column("Plan", overflow="fold")
  table.add_column("Balance", justify="right")
  table.add_column("Allocation", justify="right")
  table.add_column("Active", justify="center")

  for pool in pools:
    table.add_row(
      pool["user_repository_id"][:20] + "...",
      pool.get("user_id", "N/A")[:20] + "...",
      pool["repository_type"],
      pool["repository_plan"],
      f"{pool['current_balance']:,.2f}",
      f"{pool['monthly_allocation']:,.2f}",
      "✓" if pool["is_active"] else "✗",
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(pools):,} repository credit pools")


@credits_repos.command("get")
@click.argument("user_repository_id")
@click.pass_obj
def get_repository_credits(client, user_repository_id):
  """Get details of a specific repository credit pool."""
  pool = client._make_request(
    "GET", f"/admin/v1/credits/repositories/{user_repository_id}"
  )

  click.echo("\nREPOSITORY CREDIT POOL DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nUser Repository ID: {pool['user_repository_id']}")
  click.echo(f"User ID: {pool.get('user_id', 'N/A')}")
  click.echo(f"Repository Type: {pool['repository_type']}")
  click.echo(f"Repository Plan: {pool['repository_plan']}")
  click.echo(f"Active: {'Yes' if pool['is_active'] else 'No'}")

  click.echo("\nCREDITS")
  click.echo(f"  Current Balance: {pool['current_balance']:,.2f}")
  click.echo(f"  Monthly Allocation: {pool['monthly_allocation']:,.2f}")
  click.echo(f"  Consumed This Month: {pool['consumed_this_month']:,.2f}")
  click.echo(f"  Rollover Credits: {pool['rollover_credits']:,.2f}")
  click.echo(f"  Allows Rollover: {'Yes' if pool['allows_rollover'] else 'No'}")


@credits_repos.command("bonus")
@click.argument("user_repository_id")
@click.option("--amount", type=float, required=True, help="Amount of credits to add")
@click.option("--description", required=True, help="Reason for bonus credits")
@click.pass_obj
def add_repository_bonus_credits(client, user_repository_id, amount, description):
  """Add bonus credits to a repository credit pool."""
  data = {
    "amount": amount,
    "description": description,
  }

  pool = client._make_request(
    "POST", f"/admin/v1/credits/repositories/{user_repository_id}/bonus", data=data
  )

  click.echo(f"✅ Added {amount:,.2f} bonus credits to repository {user_repository_id}")
  click.echo(f"   New balance: {pool['current_balance']:,.2f}")
  click.echo(f"   Description: {description}")


@cli.group()
def graphs():
  """Manage graphs."""
  pass


@graphs.command("list")
@click.option("--user-email", help="Filter by owner email")
@click.option("--tier", help="Filter by tier")
@click.option("--backend", help="Filter by backend")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_graphs(client, user_email, tier, backend, limit):
  """List all graphs."""
  params = {"limit": limit}
  if user_email:
    params["user_email"] = user_email
  if tier:
    params["tier"] = tier
  if backend:
    params["backend"] = backend

  graphs_list = client._make_request("GET", "/admin/v1/graphs", params=params)

  if not graphs_list:
    console.print("\n[yellow]No graphs found.[/yellow]")
    return

  table = Table(title="Graphs", show_header=True, header_style="bold cyan")
  table.add_column("Graph ID", no_wrap=True)
  table.add_column("Name", overflow="fold")
  table.add_column("Tier", overflow="fold")
  table.add_column("Backend", overflow="fold")
  table.add_column("Status", overflow="fold")
  table.add_column("Storage", justify="right")

  for graph in graphs_list:
    storage = f"{graph['storage_gb']:.2f} GB" if graph.get("storage_gb") else "N/A"
    table.add_row(
      graph["graph_id"],
      graph["name"],
      graph["graph_tier"],
      graph["backend"],
      graph["status"],
      storage,
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(graphs_list):,} graphs")


@graphs.command("get")
@click.argument("graph_id")
@click.pass_obj
def get_graph(client, graph_id):
  """Get details of a specific graph."""
  graph = client._make_request("GET", f"/admin/v1/graphs/{graph_id}")

  click.echo("\nGRAPH DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nGraph ID: {graph['graph_id']}")
  click.echo(f"Name: {graph['name']}")
  click.echo(f"Description: {graph.get('description', 'N/A')}")
  click.echo(f"Owner: {graph['user_id']}")
  click.echo(f"Organization: {graph['org_id']}")

  click.echo("\nCONFIGURATION")
  click.echo(f"  Tier: {graph['graph_tier']}")
  click.echo(f"  Backend: {graph['backend']}")
  click.echo(f"  Status: {graph['status']}")

  click.echo("\nRESOURCES")
  if graph.get("storage_gb"):
    click.echo(f"  Storage: {graph['storage_gb']:.2f} GB")
  if graph.get("storage_limit_gb"):
    click.echo(f"  Storage Limit: {graph['storage_limit_gb']:.2f} GB")
  if graph.get("subgraph_count") is not None:
    click.echo(f"  Subgraphs: {graph['subgraph_count']}")
  if graph.get("subgraph_limit"):
    click.echo(f"  Subgraph Limit: {graph['subgraph_limit']}")


@graphs.command("analytics")
@click.option("--tier", help="Filter by tier")
@click.pass_obj
def graphs_analytics(client, tier):
  """Get cross-graph analytics."""
  params = {}
  if tier:
    params["tier"] = tier

  analytics = client._make_request("GET", "/admin/v1/graphs/analytics", params=params)

  console.print()
  console.print("[bold cyan]GRAPH ANALYTICS[/bold cyan]")
  console.print("=" * 60)

  console.print(f"\n[bold]TOTAL GRAPHS:[/bold] {analytics['total_graphs']:,}")

  console.print("\n[bold]BY TIER:[/bold]")
  for tier_name, count in sorted(analytics["by_tier"].items()):
    console.print(f"  {tier_name}: {count:,}")

  console.print("\n[bold]BY BACKEND:[/bold]")
  for backend, count in sorted(analytics["by_backend"].items()):
    console.print(f"  {backend}: {count:,}")

  console.print("\n[bold]BY STATUS:[/bold]")
  for status_val, count in sorted(analytics["by_status"].items()):
    console.print(f"  {status_val}: {count:,}")

  console.print(
    f"\n[bold]TOTAL STORAGE:[/bold] {analytics['total_storage_gb']:,.2f} GB"
  )

  if analytics.get("largest_graphs"):
    console.print("\n[bold]LARGEST GRAPHS:[/bold]")
    for graph in analytics["largest_graphs"][:10]:
      console.print(f"  {graph['graph_id']}: {graph['storage_gb']:,.2f} GB")


@cli.group()
def users():
  """Manage users."""
  pass


@users.command("list")
@click.option("--email", help="Filter by email (partial match)")
@click.option("--verified-only", is_flag=True, help="Only show verified users")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_users(client, email, verified_only, limit):
  """List all users."""
  params = {
    "limit": limit,
    "verified_only": verified_only,
  }
  if email:
    params["email"] = email

  users_list = client._make_request("GET", "/admin/v1/users", params=params)

  if not users_list:
    console.print("\n[yellow]No users found.[/yellow]")
    return

  table = Table(title="Users", show_header=True, header_style="bold cyan")
  table.add_column("User ID", no_wrap=True)
  table.add_column("Email", overflow="fold")
  table.add_column("Name", overflow="fold")
  table.add_column("Verified", overflow="fold")
  table.add_column("Org Role", overflow="fold")
  table.add_column("Created", overflow="fold")

  for user in users_list:
    verified = "Yes" if user["email_verified"] else "No"
    table.add_row(
      user["id"],
      user["email"],
      user.get("name", "N/A"),
      verified,
      user.get("org_role", "N/A"),
      user["created_at"][:10],
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(users_list):,} users")


@users.command("get")
@click.argument("user_id")
@click.pass_obj
def get_user(client, user_id):
  """Get details of a specific user."""
  user = client._make_request("GET", f"/admin/v1/users/{user_id}")

  click.echo("\nUSER DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nUser ID: {user['id']}")
  click.echo(f"Email: {user['email']}")
  click.echo(f"Name: {user.get('name', 'N/A')}")
  click.echo(f"Email Verified: {'Yes' if user['email_verified'] else 'No'}")

  click.echo("\nORGANIZATION")
  click.echo(f"  Org ID: {user['org_id']}")
  click.echo(f"  Role: {user['org_role']}")

  click.echo("\nDATES")
  click.echo(f"  Created: {user['created_at'][:10]}")
  if user.get("last_login_at"):
    click.echo(f"  Last Login: {user['last_login_at'][:10]}")


@users.command("graphs")
@click.argument("user_id")
@click.pass_obj
def user_graphs(client, user_id):
  """Get all graphs accessible by a user."""
  graphs_list = client._make_request("GET", f"/admin/v1/users/{user_id}/graphs")

  if not graphs_list:
    console.print("\n[yellow]User has no graph access.[/yellow]")
    return

  table = Table(title="User Graphs", show_header=True, header_style="bold cyan")
  table.add_column("Graph ID", no_wrap=True)
  table.add_column("Name", overflow="fold")
  table.add_column("Role", overflow="fold")
  table.add_column("Tier", overflow="fold")
  table.add_column("Storage", justify="right")

  for graph in graphs_list:
    storage = f"{graph['storage_gb']:.2f} GB" if graph.get("storage_gb") else "N/A"
    table.add_row(
      graph["graph_id"],
      graph["graph_name"],
      graph["role"],
      graph["graph_tier"],
      storage,
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(graphs_list):,} graphs")


@users.command("activity")
@click.argument("user_id")
@click.pass_obj
def user_activity(client, user_id):
  """Get user's recent activity summary."""
  activity = client._make_request("GET", f"/admin/v1/users/{user_id}/activity")

  console.print()
  console.print("[bold cyan]USER ACTIVITY[/bold cyan]")
  console.print("=" * 60)

  console.print(f"\nUser ID: {activity['user_id']}")

  console.print("\n[bold]USAGE (This Month):[/bold]")
  console.print(f"  Credit Usage: {activity['credit_usage_month']:,.2f}")
  console.print(f"  Storage Usage: {activity['storage_usage_gb']:,.2f} GB")

  console.print("\n[bold]ACCESS:[/bold]")
  console.print(f"  Graphs: {len(activity['graphs_accessed']):,}")
  console.print(f"  Repositories: {len(activity['repositories_accessed']):,}")

  if activity.get("recent_logins"):
    console.print("\n[bold]RECENT LOGINS:[/bold]")
    for login in activity["recent_logins"][:5]:
      console.print(f"  {login['timestamp'][:19]} ({login['type']})")


@cli.group()
def orgs():
  """Manage organizations."""
  pass


@orgs.command("list")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_orgs(client, limit):
  """List all organizations."""
  params = {"limit": limit}
  orgs_list = client._make_request("GET", "/admin/v1/orgs", params=params)

  if not orgs_list:
    console.print("\n[yellow]No organizations found.[/yellow]")
    return

  table = Table(title="Organizations", show_header=True, header_style="bold cyan")
  table.add_column("Org ID", no_wrap=True)
  table.add_column("Name", overflow="fold")
  table.add_column("Type", overflow="fold")
  table.add_column("Users", justify="right")
  table.add_column("Graphs", justify="right")
  table.add_column("Credits", justify="right")
  table.add_column("Created", overflow="fold")

  for org in orgs_list:
    table.add_row(
      org["org_id"],
      org["name"],
      org["org_type"],
      str(org["user_count"]),
      str(org["graph_count"]),
      f"{org['total_credits']:.2f}",
      org["created_at"][:10],
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(orgs_list):,} organizations")


@orgs.command("get")
@click.argument("org_id")
@click.pass_obj
def get_org(client, org_id):
  """Get details of a specific organization."""
  org = client._make_request("GET", f"/admin/v1/orgs/{org_id}")

  click.echo("\nORGANIZATION DETAILS")
  click.echo("=" * 60)

  click.echo(f"\nOrg ID: {org['org_id']}")
  click.echo(f"Name: {org['name']}")
  click.echo(f"Type: {org['org_type']}")

  click.echo("\nSTATS")
  click.echo(f"  Users: {org['user_count']}")
  click.echo(f"  Graphs: {org['graph_count']}")
  click.echo(f"  Total Credits: {org['total_credits']:.2f}")

  click.echo("\nBILLING")
  click.echo(f"  Payment Method: {'Yes' if org.get('has_payment_method') else 'No'}")
  click.echo(
    f"  Invoice Billing: {'Yes' if org.get('invoice_billing_enabled') else 'No'}"
  )
  click.echo(f"  Payment Terms: {org.get('payment_terms', 'N/A')}")
  if org.get("billing_email"):
    click.echo(f"  Billing Email: {org['billing_email']}")
  if org.get("stripe_customer_id"):
    click.echo(f"  Stripe Customer: {org['stripe_customer_id']}")

  click.echo("\nDATES")
  click.echo(f"  Created: {org['created_at'][:10]}")
  click.echo(f"  Updated: {org['updated_at'][:10]}")

  if org.get("users"):
    click.echo("\nUSERS")
    for user in org["users"]:
      click.echo(f"  - {user['email']} ({user['role']}) - {user['name']}")

  if org.get("graphs"):
    click.echo("\nGRAPHS")
    for graph in org["graphs"]:
      click.echo(f"  - {graph['graph_id']}: {graph['name']} ({graph['tier']})")


@orgs.command("update")
@click.argument("org_id")
@click.option(
  "--invoice-billing/--no-invoice-billing",
  default=None,
  help="Enable/disable invoice billing",
)
@click.option("--billing-email", help="Billing email address")
@click.option("--billing-contact-name", help="Billing contact name")
@click.option("--payment-terms", help="Payment terms (e.g., net_30, net_60)")
@click.pass_obj
def update_org(
  client,
  org_id,
  invoice_billing,
  billing_email,
  billing_contact_name,
  payment_terms,
):
  """Update organization billing settings."""
  params = {}

  if invoice_billing is not None:
    params["invoice_billing_enabled"] = invoice_billing
  if billing_email:
    params["billing_email"] = billing_email
  if billing_contact_name:
    params["billing_contact_name"] = billing_contact_name
  if payment_terms:
    params["payment_terms"] = payment_terms

  if not params:
    click.echo("❌ No updates specified")
    return

  org = client._make_request("PATCH", f"/admin/v1/orgs/{org_id}", params=params)

  click.echo(f"✅ Updated org {org['org_id']}")
  click.echo(f"   Name: {org.get('name', 'N/A')}")
  click.echo(
    f"   Invoice Billing: {'Yes' if org.get('invoice_billing_enabled') else 'No'}"
  )
  click.echo(f"   Payment Terms: {org.get('payment_terms', 'N/A')}")
  if billing_email:
    click.echo(f"   Billing Email: {org.get('billing_email', 'N/A')}")


@cli.group()
def migrations():
  """Database migration operations."""
  pass


@migrations.command("up")
@click.pass_obj
def migrations_up(client):
  """Run database migrations."""
  if client.environment == "dev":
    console.print("[blue]Running migrations locally...[/blue]")
    result = subprocess.run(
      ["uv", "run", "alembic", "upgrade", "head"],
      capture_output=True,
      text=True,
    )
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Migration failed")
    console.print("[green]✓ Migrations completed[/green]")
  else:
    executor = SSMExecutor(client.environment)
    stdout, _, _ = executor.execute(
      "/usr/local/bin/run-migrations.sh --command 'upgrade head'"
    )


@migrations.command("down")
@click.pass_obj
def migrations_down(client):
  """Rollback last migration."""
  if client.environment == "dev":
    console.print("[blue]Rolling back migration locally...[/blue]")
    result = subprocess.run(
      ["uv", "run", "alembic", "downgrade", "-1"],
      capture_output=True,
      text=True,
    )
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Rollback failed")
    console.print("[green]✓ Rollback completed[/green]")
  else:
    executor = SSMExecutor(client.environment)
    stdout, _, _ = executor.execute(
      "/usr/local/bin/run-migrations.sh --command 'downgrade -1'"
    )


@migrations.command("current")
@click.pass_obj
def migrations_current(client):
  """Show current migration version."""
  if client.environment == "dev":
    result = subprocess.run(
      ["uv", "run", "alembic", "current"],
      capture_output=True,
      text=True,
    )
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Failed to get current version")
  else:
    executor = SSMExecutor(client.environment)
    stdout, _, _ = executor.execute(
      "/usr/local/bin/run-migrations.sh --command current"
    )


@cli.group()
def sec():
  """SEC database operations."""
  pass


@sec.command("load")
@click.option("--ticker", required=True, help="Stock ticker symbol")
@click.option("--year", help="Year to load (optional)")
@click.pass_obj
def sec_load(client, ticker, year):
  """Load SEC data for a company."""
  if client.environment == "dev":
    console.print(f"[blue]Loading SEC data locally for {ticker}...[/blue]")
    year_arg = f" {year}" if year else ""
    command = f"just sec-load {ticker}{year_arg}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("SEC load failed")
  else:
    executor = SSMExecutor(client.environment)
    year_arg = f" --year {year}" if year else ""
    command = (
      f"/usr/local/bin/run-bastion-operation.sh sec-load --ticker {ticker}{year_arg}"
    )
    stdout, _, _ = executor.execute(command)


@sec.command("health")
@click.pass_obj
def sec_health(client):
  """Check SEC database health."""
  if client.environment == "dev":
    command = "just sec-health"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("SEC health check failed")
  else:
    executor = SSMExecutor(client.environment)
    stdout, _, _ = executor.execute(
      "/usr/local/bin/run-bastion-operation.sh sec-health"
    )


@sec.command("plan")
@click.option("--start-year", required=True, type=int, help="Start year")
@click.option("--end-year", required=True, type=int, help="End year")
@click.option("--max-companies", default=50, help="Maximum number of companies")
@click.pass_obj
def sec_plan(client, start_year, end_year, max_companies):
  """Create SEC orchestrator execution plan."""
  if client.environment == "dev":
    console.print("[blue]Creating SEC orchestrator plan...[/blue]")
    command = f"uv run python -m robosystems.scripts.sec_orchestrator plan --start-year {start_year} --end-year {end_year} --max-companies {max_companies}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Plan creation failed")
  else:
    executor = SSMExecutor(client.environment)
    command = f"/usr/local/bin/run-bastion-operation.sh sec-plan --start-year {start_year} --end-year {end_year} --max-companies {max_companies}"
    stdout, _, _ = executor.execute(command)


@sec.command("phase")
@click.option(
  "--phase",
  required=True,
  type=click.Choice(["download", "process", "consolidate", "ingest"]),
  help="Phase to execute",
)
@click.option("--resume", is_flag=True, help="Resume from previous state")
@click.pass_obj
def sec_phase(client, phase, resume):
  """Execute SEC orchestrator phase."""
  if client.environment == "dev":
    console.print(f"[blue]Executing SEC phase: {phase}...[/blue]")
    resume_arg = " --resume" if resume else ""
    command = f"uv run python -m robosystems.scripts.sec_orchestrator start-phase --phase {phase}{resume_arg}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Phase execution failed")
  else:
    executor = SSMExecutor(client.environment)
    resume_arg = " --resume" if resume else ""
    command = (
      f"/usr/local/bin/run-bastion-operation.sh sec-phase --phase {phase}{resume_arg}"
    )
    stdout, _, _ = executor.execute(command)


@sec.command("status")
@click.pass_obj
def sec_status(client):
  """Check SEC orchestrator status."""
  if client.environment == "dev":
    command = "uv run python -m robosystems.scripts.sec_orchestrator status"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    console.print(result.stdout)
    if result.returncode != 0:
      console.print(f"[red]Error:[/red] {result.stderr}")
      raise click.ClickException("Status check failed")
  else:
    executor = SSMExecutor(client.environment)
    stdout, _, _ = executor.execute(
      "/usr/local/bin/run-bastion-operation.sh sec-status"
    )


if __name__ == "__main__":
  cli()
