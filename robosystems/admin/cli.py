"""Command-line client for the admin API (`/admin/v1/*`).

Covers subscriptions, invoices, credits, graphs, users, orgs, migrations,
cache, instances, search, and worker operations. Commands are thin wrappers
over HTTP calls; the API key comes from `ADMIN_API_KEY` in dev, otherwise from
AWS Secrets Manager.

Requests go to localhost:8000 by default, which serves both local dev and an
SSM tunnel. Against staging or prod a tunnel is required — the ALB blocks
`/admin/v1/*` whenever the API is publicly reachable.

Examples:
    # Local dev (Docker, no tunnel)
    just admin dev stats

    # Staging/prod: start the tunnel in another terminal, then run commands
    ./bin/tools/tunnels.sh prod all
    just admin prod stats
    just admin prod subscriptions list
    just admin prod subscriptions get <subscription-id>
    just admin prod credits health
    just admin prod orgs update <org-id> --billing-email new@example.com

    # Skip the tunnel; only works while the API is in 'internal' mode
    just admin prod --direct stats
"""

import json
import os
import subprocess
from typing import Any

import click
import requests
from rich.console import Console

from ..logger import get_logger
from .commands.billing import credits, invoices, subscriptions
from .commands.graphs import graphs
from .commands.oauth import oauth
from .commands.ops import cache, instances, migrations
from .commands.scim import scim
from .commands.search import search
from .commands.users_orgs import orgs, users
from .commands.worker import worker

logger = get_logger(__name__)
console = Console()


class AdminAPIClient:
  """Client for interacting with the RoboSystems admin API."""

  def __init__(
    self,
    environment: str = "prod",
    api_base_url: str | None = None,
    aws_profile: str = "robosystems-sso",
    use_direct: bool = False,
  ):
    """Resolve the API base URL and fetch the admin key.

    `api_base_url` defaults to localhost:8000, which reaches the API through
    an SSM tunnel. `use_direct` swaps in the environment's public URL instead,
    which the ALB only permits while the API is in 'internal' mode.
    """
    self.environment = environment
    self.aws_profile = aws_profile
    self.use_direct = use_direct

    if api_base_url:
      self.api_base_url = api_base_url
    elif use_direct:
      if environment == "staging":
        self.api_base_url = "https://api.staging.robosystems.ai"
      elif environment == "prod":
        self.api_base_url = "https://api.robosystems.ai"
      else:
        self.api_base_url = "http://localhost:8000"
    else:
      self.api_base_url = "http://localhost:8000"

    if use_direct and environment != "dev":
      self._check_api_access_mode()

    self.admin_key = self._get_admin_key()

  def _check_api_access_mode(self) -> None:
    """Check if API is in public mode and warn user to use tunnel."""
    try:
      stack_name = f"RoboSystemsAPI{self.environment.capitalize()}"
      cmd = [
        "aws",
        "cloudformation",
        "describe-stacks",
        "--stack-name",
        stack_name,
        "--query",
        "Stacks[0].Parameters[?ParameterKey==`ApiAccessMode`].ParameterValue",
        "--output",
        "text",
        "--profile",
        self.aws_profile,
        "--region",
        "us-east-1",
      ]

      env = {k: v for k, v in os.environ.items() if k != "AWS_ENDPOINT_URL"}
      result = subprocess.run(cmd, capture_output=True, text=True, check=False, env=env)
      api_access_mode = result.stdout.strip() if result.returncode == 0 else ""

      if api_access_mode == "public":
        raise click.ClickException(
          f"Direct API access is blocked in '{api_access_mode}' mode.\n\n"
          f"The ALB blocks all /admin/v1/* endpoints when API is publicly accessible.\n\n"
          f"To access admin endpoints, use the SSM tunnel:\n"
          f"  1. Start tunnel:  ./bin/tools/tunnels.sh {self.environment} all\n"
          f"  2. Run command:   just admin {self.environment} <command>\n\n"
          f"Example: just admin {self.environment} stats"
        )
    except subprocess.SubprocessError:
      pass

  def _get_admin_key(self) -> str:
    """Get the admin API key from environment variable (dev) or AWS Secrets Manager."""
    if self.environment == "dev":
      admin_key = os.getenv("ADMIN_API_KEY")
      if admin_key:
        console.print(
          f"[green]✓[/green] Connected to {self.environment} admin API (using ADMIN_API_KEY from .env.local)"
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

      env = {k: v for k, v in os.environ.items() if k != "AWS_ENDPOINT_URL"}
      result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)

      secret_value = result.stdout.strip()
      try:
        secret_data = json.loads(secret_value)
        admin_key = secret_data.get("ADMIN_API_KEY", secret_value)
      except json.JSONDecodeError:
        admin_key = secret_value

      if not admin_key:
        raise click.ClickException(f"Empty secret value in {secret_id}")

      mode = "direct" if self.use_direct else "tunnel"
      console.print(
        f"[green]✓[/green] Connected to {self.environment} admin API via {mode} (using AWS Secrets Manager)"
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
    except Exception as e:
      raise click.ClickException(f"Error fetching admin key: {e!s}")

  def _make_request(
    self,
    method: str,
    endpoint: str,
    data: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Make an authenticated request to the admin API."""
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
      raise click.ClickException(f"Network error: {e!s}")


@click.group()
@click.option(
  "--environment",
  "-e",
  default="prod",
  type=click.Choice(["dev", "staging", "prod"]),
  help="Environment to connect to",
)
@click.option(
  "--api-url",
  help="Override API base URL (default: localhost:8000)",
)
@click.option(
  "--aws-profile",
  default="robosystems-sso",
  help="AWS CLI profile name (for fetching admin key from Secrets Manager)",
)
@click.option(
  "--direct",
  "-d",
  is_flag=True,
  help="Use public API URLs directly (only works if API is in 'internal' mode)",
)
@click.pass_context
def cli(ctx, environment, api_url, aws_profile, direct):
  """RoboSystems Admin CLI - Remote administration via admin API.

  This CLI provides access to subscription management, customer management,
  credit management, graph management, and user management.

  By default, connects to localhost:8000 which works with:
    - Local dev (Docker): just admin dev stats
    - SSM tunnel: ./bin/tools/tunnels.sh prod all && just admin prod stats

  Direct mode (--direct / -d):
    Connects to public API URLs. Only works if API is in 'internal' mode.
    In 'public' mode, admin endpoints are blocked at the ALB.
  """
  ctx.obj = AdminAPIClient(
    environment=environment,
    api_base_url=api_url,
    aws_profile=aws_profile,
    use_direct=direct,
  )


@cli.command()
@click.pass_obj
def stats(client):
  """Show subscription and customer statistics."""
  all_subs = client._make_request(
    "GET", "/admin/v1/subscriptions", params={"limit": 1000, "include_canceled": True}
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

    if sub["status"].lower() == "active" and sub.get("base_price_cents"):
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


# Register command groups from submodules
cli.add_command(subscriptions)
cli.add_command(invoices)
cli.add_command(credits)
cli.add_command(graphs)
cli.add_command(users)
cli.add_command(orgs)
cli.add_command(migrations)
cli.add_command(cache)
cli.add_command(instances)
cli.add_command(scim)
cli.add_command(oauth)
cli.add_command(search)
cli.add_command(worker)


if __name__ == "__main__":
  cli()
