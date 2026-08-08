"""User and organization administration commands."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
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


def _resolve_user_id(client, identifier):
  """Accept an email or a user ID and return the user ID."""
  if "@" not in identifier:
    return identifier

  matches = client._make_request(
    "GET", "/admin/v1/users", params={"email": identifier, "limit": 10}
  )
  exact = [u for u in matches or [] if u["email"].lower() == identifier.lower()]
  if not exact:
    console.print(f"\n[red]No user found with email {identifier}[/red]")
    raise SystemExit(1)
  return exact[0]["id"]


@users.command("deactivate")
@click.argument("identifier")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt")
@click.pass_obj
def deactivate_user(client, identifier, yes):
  """Deactivate a user by email or ID, cutting off all access.

  Invalidates every session and revokes every API key — the escalation above a
  password change, which rotates the credential but leaves keys working so
  routine rotation doesn't break integrations. Use this for a compromised or
  suspended account. Reactivating does not restore the revoked keys.
  """
  user_id = _resolve_user_id(client, identifier)

  if not yes:
    click.confirm(
      f"\nDeactivate {identifier}? This ends all sessions and revokes all API keys.",
      abort=True,
      default=False,
    )

  result = client._make_request("POST", f"/admin/v1/users/{user_id}/deactivate")

  console.print(f"\n[green]Deactivated {result['email']}[/green]")
  console.print(f"  API keys revoked: {result['api_keys_revoked']}")
  if result.get("api_keys_failed"):
    console.print(
      f"[red]  {result['api_keys_failed']} API key(s) could NOT be revoked — "
      f"the account is only partially locked down. Re-run this command.[/red]"
    )
  console.print("  All sessions invalidated.")
  if not result["changed"]:
    console.print("[yellow]  (was already inactive — re-ran the revocation)[/yellow]")


@users.command("activate")
@click.argument("identifier")
@click.pass_obj
def activate_user(client, identifier):
  """Reactivate a deactivated user by email or ID.

  Restores sign-in only. Revoked API keys are not reinstated — the user must
  issue new ones.
  """
  user_id = _resolve_user_id(client, identifier)
  result = client._make_request("POST", f"/admin/v1/users/{user_id}/activate")

  console.print(f"\n[green]Activated {result['email']}[/green]")
  if result["changed"]:
    console.print("  They can sign in again; API keys must be re-issued.")
  else:
    console.print("[yellow]  (was already active — nothing changed)[/yellow]")


@users.command("delete")
@click.argument("identifier")
@click.option(
  "--dry-run", is_flag=True, help="Show what deletion would do, without doing it"
)
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt")
@click.pass_obj
def delete_user(client, identifier, dry_run, yes):
  """Delete a user account by email or user ID, freeing the email address.

  Refuses while the user's org still has live graphs, subscriptions in force,
  or active repository access. Billing and audit history is retained.
  """
  user_id = _resolve_user_id(client, identifier)

  assessment = client._make_request(
    "DELETE", f"/admin/v1/users/{user_id}", params={"dry_run": True}
  )

  console.print()
  console.print(f"[bold]User:[/bold] {assessment['email']} ({assessment['user_id']})")

  removals = assessment.get("removals") or {}
  if any(removals.values()):
    console.print("\n[bold]Will remove:[/bold]")
    for name, count in removals.items():
      if count:
        console.print(f"  {name.replace('_', ' ')}: {count}")

  if assessment.get("orgs_deleted"):
    console.print(
      f"\n[bold]Organizations deleted:[/bold] {', '.join(assessment['orgs_deleted'])}"
    )
  if assessment.get("orgs_retained"):
    console.print(
      f"[bold]Organizations retained:[/bold] {', '.join(assessment['orgs_retained'])}"
    )

  if assessment.get("blockers"):
    console.print("\n[red]Cannot delete:[/red]")
    for blocker in assessment["blockers"]:
      console.print(f"  - {blocker['detail']}")
    raise SystemExit(1)

  if dry_run:
    console.print("\n[yellow]Dry run — nothing was deleted.[/yellow]")
    return

  if not yes:
    click.confirm(
      f"\nPermanently delete {assessment['email']}?", abort=True, default=False
    )

  result = client._make_request("DELETE", f"/admin/v1/users/{user_id}")
  console.print(f"\n[green]Deleted {result['email']}[/green]")
  console.print("The email address is now available for registration or invitation.")


@click.group()
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
  table.add_column("Limit", justify="right")
  table.add_column("Credits", justify="right")
  table.add_column("Created", overflow="fold")

  for org in orgs_list:
    max_graphs = org.get("max_graphs")
    table.add_row(
      org["org_id"],
      org["name"],
      org["org_type"],
      str(org["user_count"]),
      str(org["graph_count"]),
      "unlimited" if max_graphs == -1 else str(max_graphs),
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

  click.echo("\nLIMITS")
  max_graphs = org.get("max_graphs")
  if max_graphs == -1:
    click.echo("  Max Graphs: unlimited")
  else:
    click.echo(f"  Max Graphs: {max_graphs}")

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
@click.option(
  "--max-graphs",
  type=int,
  default=None,
  help="Maximum graphs the org may create (-1 for unlimited)",
)
@click.pass_obj
def update_org(
  client,
  org_id,
  invoice_billing,
  billing_email,
  billing_contact_name,
  payment_terms,
  max_graphs,
):
  """Update organization billing settings and limits."""
  params = {}

  if invoice_billing is not None:
    params["invoice_billing_enabled"] = invoice_billing
  if billing_email:
    params["billing_email"] = billing_email
  if billing_contact_name:
    params["billing_contact_name"] = billing_contact_name
  if payment_terms:
    params["payment_terms"] = payment_terms
  if max_graphs is not None:
    if max_graphs < -1:
      raise click.BadParameter("must be >= -1 (-1 means unlimited)")
    params["max_graphs"] = max_graphs

  if not params:
    click.echo("❌ No updates specified")
    return

  org = client._make_request("PATCH", f"/admin/v1/orgs/{org_id}", data=params)

  click.echo(f"✅ Updated org {org['org_id']}")
  click.echo(f"   Name: {org.get('name', 'N/A')}")
  click.echo(
    f"   Invoice Billing: {'Yes' if org.get('invoice_billing_enabled') else 'No'}"
  )
  click.echo(f"   Payment Terms: {org.get('payment_terms', 'N/A')}")
  if billing_email:
    click.echo(f"   Billing Email: {org.get('billing_email', 'N/A')}")
  if max_graphs is not None:
    updated_limit = org.get("max_graphs")
    click.echo(
      f"   Max Graphs: {'unlimited' if updated_limit == -1 else updated_limit}"
    )
