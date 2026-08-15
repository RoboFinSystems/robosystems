"""Billing commands: subscriptions, invoices, and credits."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def subscriptions():
  """Manage graph subscriptions."""
  pass


@subscriptions.command("list")
@click.option("--status", help="Filter by status (active, paused, canceled)")
@click.option("--email", help="Filter by owner email")
@click.option("--resource-type", help="Filter by resource type (graph, repository)")
@click.option("--include-canceled", is_flag=True, help="Include canceled subscriptions")
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_obj
def list_subscriptions(client, status, email, resource_type, include_canceled, limit):
  """List all subscriptions."""
  params = {
    "limit": limit,
    "include_canceled": include_canceled,
  }
  if status:
    params["status_filter"] = status.lower()
  if email:
    params["user_email"] = email
  if resource_type:
    params["resource_type"] = resource_type

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
      sub["resource_id"] or "—",
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
  click.echo(f"Resource: {sub['resource_type']} / {sub['resource_id'] or '—'}")
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
  help="Plan name (e.g., ladybug-standard, ladybug-large, ladybug-xlarge)",
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
  "--status", type=click.Choice(["active", "paused", "canceled"]), help="New status"
)
@click.option(
  "--plan-name",
  help="New plan name (e.g., ladybug-standard, ladybug-large, ladybug-xlarge)",
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


@click.group()
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
  table.add_column("Created", overflow="fold")

  for invoice in invoices:
    table.add_row(
      invoice["invoice_number"],
      invoice.get("user_email", "N/A"),
      invoice["status"],
      f"${invoice['total_cents'] / 100:.2f}",
      invoice["due_date"][:10] if invoice.get("due_date") else "N/A",
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

  # The endpoint declares payment_method / payment_reference as query
  # parameters, not a JSON body.
  invoice = client._make_request(
    "PATCH", f"/admin/v1/invoices/{invoice_id}/mark-paid", params=params
  )

  click.echo(f"✅ Marked invoice {invoice['invoice_number']} as paid")
  click.echo(f"   Customer: {invoice.get('user_email', 'N/A')}")
  click.echo(f"   Amount: ${invoice['total_cents'] / 100:.2f}")
  click.echo(f"   Payment Method: {invoice['payment_method']}")
  if payment_reference:
    click.echo(f"   Reference: {invoice['payment_reference']}")


@click.group()
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


@credits.command("reset")
@click.argument("graph_id")
@click.option("--reason", default=None, help="Reason recorded on the ledger rows")
@click.confirmation_option(
  prompt="Forfeit the remaining balance and refill to the monthly allocation?"
)
@click.pass_obj
def reset_credit_pool(client, graph_id, reason):
  """Reset a graph's credit pool to its monthly allocation.

  The remaining balance is forfeited (recorded as an EXPIRATION ledger
  row) and the pool refills to the monthly allocation. The scheduled
  monthly reset still runs normally when the month turns.
  """
  data = {"reason": reason} if reason else {}

  pool = client._make_request(
    "POST", f"/admin/v1/credits/graphs/{graph_id}/reset", data=data
  )

  click.echo(f"✅ Reset credit pool for graph {graph_id}")
  click.echo(f"   New balance: {pool['current_balance']:,.2f}")
  click.echo(f"   Monthly allocation: {pool['monthly_allocation']:,.2f}")
  if reason:
    click.echo(f"   Reason: {reason}")


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
      pool["user_repository_id"],
      pool.get("user_id", "N/A"),
      pool["repository_type"],
      pool["repository_plan"],
      f"{pool['current_balance']:,.2f}",
      f"{pool['monthly_allocation']:,.2f}",
      "Yes" if pool["is_active"] else "No",
    )

  console.print()
  console.print(table)
  console.print(f"\n[bold]Total:[/bold] {len(pools):,} repository credit pools")


@credits_repos.command("get")
@click.argument("user_repository_id")
@click.pass_obj
def get_repository_credits(client, user_repository_id):
  """Get credit pool details for a repository."""
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
