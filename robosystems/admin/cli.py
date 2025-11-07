"""RoboSystems Admin CLI for remote administration via admin API.

This CLI provides remote access to admin operations without requiring bastion host access.
It fetches the admin API key from AWS Secrets Manager using AWS CLI and provides commands
for subscription management, customer management, and repository access control.

Usage:
    uv run python -m robosystems.scripts.admin_cli [command] [options]

Examples:
    # List all subscriptions
    uv run python -m robosystems.scripts.admin_cli subscriptions list

    # Get subscription details
    uv run python -m robosystems.scripts.admin_cli subscriptions get <subscription-id>

    # Update customer billing settings
    uv run python -m robosystems.scripts.admin_cli customers update <user-id> --billing-email new@example.com

    # Show statistics
    uv run python -m robosystems.scripts.admin_cli stats
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
  and repository access control.

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
      sub.get("customer_email", "N/A"),
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
  click.echo(
    f"Customer: {sub.get('customer_name', 'N/A')} ({sub.get('customer_email', 'N/A')})"
  )
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
@click.option("--user-id", required=True, help="User ID")
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
  user_id,
  plan_name,
  resource_type,
  billing_interval,
):
  """Create a new subscription."""
  data = {
    "resource_type": resource_type,
    "resource_id": resource_id,
    "user_id": user_id,
    "plan_name": plan_name,
    "billing_interval": billing_interval,
  }

  sub = client._make_request("POST", "/admin/v1/subscriptions", data=data)

  click.echo(f"✅ Created subscription {sub['id']}")
  click.echo(f"   Resource: {sub['resource_type']} / {sub['resource_id']}")
  click.echo(f"   Customer: {sub.get('customer_email', user_id)}")
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
def customers():
  """Manage customer billing settings."""
  pass


@customers.command("list")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_customers(client, limit):
  """List all customers with their billing settings."""
  params = {"limit": limit}
  customers_data = client._make_request("GET", "/admin/v1/customers", params=params)

  if not customers_data:
    console.print("\n[yellow]No customers found.[/yellow]")
    return

  table = Table(title="Customers", show_header=True, header_style="bold cyan")
  table.add_column("User ID", no_wrap=True)
  table.add_column("Name", overflow="fold")
  table.add_column("Email", overflow="fold")
  table.add_column("Payment Method", overflow="fold")
  table.add_column("Invoice Billing", overflow="fold")
  table.add_column("Payment Terms", overflow="fold")
  table.add_column("Billing Email", overflow="fold")

  for customer in customers_data:
    table.add_row(
      customer["user_id"],
      customer.get("user_name", "N/A"),
      customer.get("user_email", "N/A"),
      "Yes" if customer.get("has_payment_method") else "No",
      "Yes" if customer.get("invoice_billing_enabled") else "No",
      customer["payment_terms"],
      customer.get("billing_email", "N/A"),
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(customers_data):,} customers")


@customers.command("update")
@click.argument("user_id")
@click.option(
  "--invoice-billing/--no-invoice-billing",
  default=None,
  help="Enable/disable invoice billing",
)
@click.option("--billing-email", help="Billing email address")
@click.option("--billing-contact-name", help="Billing contact name")
@click.option("--payment-terms", help="Payment terms (e.g., net_30, net_60)")
@click.pass_obj
def update_customer(
  client,
  user_id,
  invoice_billing,
  billing_email,
  billing_contact_name,
  payment_terms,
):
  """Update customer billing settings."""
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

  customer = client._make_request(
    "PATCH", f"/admin/v1/customers/{user_id}", params=params
  )

  click.echo(f"✅ Updated customer {customer['user_id']}")
  click.echo(f"   Name: {customer.get('user_name', 'N/A')}")
  click.echo(
    f"   Invoice Billing: {'Yes' if customer.get('invoice_billing_enabled') else 'No'}"
  )
  click.echo(f"   Payment Terms: {customer['payment_terms']}")
  if billing_email:
    click.echo(f"   Billing Email: {customer.get('billing_email', 'N/A')}")


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


if __name__ == "__main__":
  cli()
