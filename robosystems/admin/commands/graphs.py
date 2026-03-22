"""Graph administration commands."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
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


@graphs.command("deprovision")
@click.argument("graph_id")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
@click.option("--skip-backup", is_flag=True, help="Skip creating a final backup")
@click.pass_obj
def deprovision_graph(client, graph_id, force, skip_backup):
  """Deprovision a graph: tear down infrastructure and mark as deprovisioned."""
  if not force:
    click.confirm(
      f"This will deprovision graph {graph_id} and delete its database. Continue?",
      abort=True,
    )

  params = {}
  if skip_backup:
    params["skip_backup"] = "true"

  result = client._make_request(
    "POST", f"/admin/v1/graphs/{graph_id}/deprovision", params=params
  )

  click.echo(f"\n{result['message']}")
  click.echo(f"  Previous Status: {result['previous_status']}")
  click.echo(f"  Current Status: {result['status']}")
  click.echo(f"  Database Deleted: {'Yes' if result['database_deleted'] else 'No'}")
  click.echo(f"  Backup Created: {'Yes' if result.get('backup_created') else 'No'}")
  if result.get("subgraphs_deleted", 0) > 0:
    click.echo(f"  Subgraphs Deleted: {result['subgraphs_deleted']}")
  click.echo(f"  Records Cleaned: {'Yes' if result.get('records_cleaned') else 'No'}")
  if result.get("warnings"):
    click.echo("\n  Warnings:")
    for warning in result["warnings"]:
      click.echo(f"    - {warning}")


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
