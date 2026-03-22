"""Operational commands: migrations, cache, and instance management."""

import os
import subprocess
from decimal import Decimal
from typing import Any

import click
from rich.console import Console
from rich.table import Table

from ..ssm_executor import SSMExecutor

console = Console()


@click.group()
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


@click.group()
def cache():
  """Valkey cache operations."""
  pass


@cache.command("info")
@click.argument("database", required=False)
@click.pass_obj
def cache_info(client, database):
  """Show cache database info. Optionally specify a database name."""
  if database:
    data = client._make_request("GET", f"/admin/v1/cache/info/{database}")

    console.print()
    console.print(f"[bold cyan]CACHE DATABASE: {data['name'].upper()}[/bold cyan]")
    console.print("=" * 60)
    console.print(f"\n  DB Number: {data['db_number']}")
    console.print(f"  Key Count: {data['key_count']:,}")
    console.print(f"  Purpose: {data['purpose']}")

    if data.get("sample_keys"):
      console.print(f"\n[bold]Sample Keys ({len(data['sample_keys'])}):[/bold]")
      for key in data["sample_keys"]:
        console.print(f"  {key}")
  else:
    data = client._make_request("GET", "/admin/v1/cache/info")

    table = Table(title="Valkey Databases", show_header=True, header_style="bold cyan")
    table.add_column("DB #", justify="right")
    table.add_column("Name", overflow="fold")
    table.add_column("Keys", justify="right")
    table.add_column("Purpose", overflow="fold")

    for db in data["databases"]:
      table.add_row(
        str(db["db_number"]),
        db["name"],
        f"{db['key_count']:,}" if db["key_count"] >= 0 else "N/A",
        db["purpose"],
      )

    console.print()
    console.print(table)
    console.print(f"\n[bold]Total Keys:[/bold] {data['total_keys']:,}")


@cache.command("flush")
@click.argument("database")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_obj
def cache_flush(client, database, yes):
  """Flush a cache database. Use 'all' to flush everything."""
  if database == "all":
    if not yes:
      click.confirm("This will flush ALL Valkey databases. Continue?", abort=True)

    data = client._make_request("POST", "/admin/v1/cache/flush-all")

    table = Table(title="Flush Results", show_header=True, header_style="bold cyan")
    table.add_column("Name", overflow="fold")
    table.add_column("Keys Flushed", justify="right")
    table.add_column("Status", overflow="fold")

    for db in data["databases"]:
      status = "OK" if db["flushed"] else "FAILED"
      table.add_row(
        db["name"],
        f"{db['keys_before']:,}" if db["keys_before"] >= 0 else "N/A",
        status,
      )

    console.print()
    console.print(table)
    console.print(f"\n[bold]Total Keys Flushed:[/bold] {data['total_keys_flushed']:,}")
  else:
    if not yes:
      click.confirm(f"Flush all keys from '{database}'?", abort=True)

    data = client._make_request("POST", f"/admin/v1/cache/flush/{database}")
    console.print(
      f"\nFlushed [bold]{data['name']}[/bold] (DB {data['db_number']}): "
      f"{data['keys_before']:,} keys removed"
    )


@cache.command("keys")
@click.argument("database")
@click.option("--pattern", "-p", default="*", help="Key pattern to match")
@click.option("--count", "-c", default=100, help="Maximum keys to return")
@click.pass_obj
def cache_keys(client, database, pattern, count):
  """List keys in a cache database."""
  params = {"pattern": pattern, "count": count}
  data = client._make_request("GET", f"/admin/v1/cache/keys/{database}", params=params)

  console.print()
  console.print(
    f"[bold cyan]Keys in {data['name']} (pattern: {data['pattern']})[/bold cyan]"
  )

  if data["keys"]:
    for key in data["keys"]:
      console.print(f"  {key}")
    console.print(f"\n[bold]Count:[/bold] {data['count']:,}")
  else:
    console.print("  [yellow]No keys found.[/yellow]")


@cache.command("delete-keys")
@click.argument("database")
@click.option("--pattern", "-p", required=True, help="Key pattern to delete")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_obj
def cache_delete_keys(client, database, pattern, yes):
  """Delete keys matching a pattern from a cache database."""
  if pattern == "*":
    console.print("[red]Wildcard '*' is not allowed. Use 'flush' instead.[/red]")
    raise SystemExit(1)

  if not yes:
    click.confirm(f"Delete keys matching '{pattern}' from '{database}'?", abort=True)

  data = client._make_request(
    "DELETE", f"/admin/v1/cache/keys/{database}", params={"pattern": pattern}
  )
  console.print(
    f"\nDeleted {data['keys_deleted']:,} keys from [bold]{data['name']}[/bold] "
    f"(pattern: {data['pattern']})"
  )


# --- Instance management ---

GRAPH_TIERS = ["ladybug-standard", "ladybug-large", "ladybug-xlarge", "ladybug-shared"]
ALL_ASG_GROUPS = [*GRAPH_TIERS, "shared-replicas"]

TIER_TO_GHA_PREFIX = {
  "ladybug-standard": "STANDARD",
  "ladybug-large": "LARGE",
  "ladybug-xlarge": "XLARGE",
  "ladybug-shared": "SHARED",
}


class InstancesHelper:
  """Helper for AWS infrastructure operations on graph instances."""

  def __init__(self, environment: str, aws_profile: str):
    self.environment = environment
    self.aws_profile = aws_profile
    self.region = "us-east-1"

    import boto3

    os.environ.pop("AWS_ENDPOINT_URL", None)

    session = boto3.Session(profile_name=aws_profile, region_name=self.region)
    dynamodb = session.resource("dynamodb")
    self.autoscaling = session.client("autoscaling")

    self.instance_table = dynamodb.Table(
      f"robosystems-graph-{environment}-instance-registry"
    )
    self.graph_table = dynamodb.Table(f"robosystems-graph-{environment}-graph-registry")

  def _asg_name(self, group: str) -> str:
    if group == "shared-replicas":
      return f"robosystems-shared-replicas-{self.environment}-asg"
    return f"robosystems-{group}-writers-{self.environment}-asg"

  def _gha_var_name(self, group: str, param: str) -> str:
    env_upper = self.environment.upper()
    if group == "shared-replicas":
      if param == "DESIRED":
        return f"SHARED_REPLICAS_DESIRED_CAPACITY_{env_upper}"
      return f"SHARED_REPLICAS_{param}_INSTANCES_{env_upper}"
    prefix = TIER_TO_GHA_PREFIX[group]
    return f"LBUG_{prefix}_{param}_INSTANCES_{env_upper}"

  def get_all_instances(self) -> list[dict]:
    items: list[dict] = []
    response = self.instance_table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
      response = self.instance_table.scan(
        ExclusiveStartKey=response["LastEvaluatedKey"]
      )
      items.extend(response.get("Items", []))
    return items

  def get_all_graphs(self) -> list[dict]:
    items: list[dict] = []
    response = self.graph_table.scan()
    items.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
      response = self.graph_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
      items.extend(response.get("Items", []))
    return items

  def get_instance(self, instance_id: str) -> dict | None:
    response = self.instance_table.get_item(Key={"instance_id": instance_id})
    return response.get("Item")

  def get_graphs_for_instance(self, instance_id: str) -> list[dict]:
    all_graphs = self.get_all_graphs()
    return [
      g
      for g in all_graphs
      if g.get("instance_id") == instance_id and g.get("status") != "deleted"
    ]

  def get_asg_info(self, tier: str) -> dict | None:
    asg_name = self._asg_name(tier)
    try:
      response = self.autoscaling.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
      )
      groups = response.get("AutoScalingGroups", [])
      return groups[0] if groups else None
    except Exception:
      return None

  def get_all_asg_info(self) -> dict[str, dict | None]:
    result = {}
    for group in ALL_ASG_GROUPS:
      result[group] = self.get_asg_info(group)
    return result

  def scale_asg(
    self,
    tier: str,
    desired: int,
    min_size: int | None = None,
    max_size: int | None = None,
  ) -> None:
    asg_name = self._asg_name(tier)
    self.autoscaling.set_desired_capacity(
      AutoScalingGroupName=asg_name,
      DesiredCapacity=desired,
    )
    if min_size is not None or max_size is not None:
      kwargs: dict[str, Any] = {"AutoScalingGroupName": asg_name}
      if min_size is not None:
        kwargs["MinSize"] = min_size
      if max_size is not None:
        kwargs["MaxSize"] = max_size
      self.autoscaling.update_auto_scaling_group(**kwargs)

  def sync_gha_variable(self, tier: str, param: str, value: int) -> bool:
    var_name = self._gha_var_name(tier, param)
    try:
      subprocess.run(
        ["gh", "variable", "set", var_name, "--body", str(value)],
        capture_output=True,
        text=True,
        check=True,
      )
      return True
    except (subprocess.CalledProcessError, FileNotFoundError):
      return False

  def terminate_instance(self, instance_id: str) -> None:
    self.autoscaling.terminate_instance_in_auto_scaling_group(
      InstanceId=instance_id,
      ShouldDecrementDesiredCapacity=True,
    )


def _instance_matches_group(inst: dict, group: str) -> bool:
  """Check if a DynamoDB instance record belongs to the given ASG group."""
  if group == "shared-replicas":
    return inst.get("node_type") == "shared_replica"
  tier = inst.get("cluster_tier") or inst.get("tier", "")
  if inst.get("node_type") == "shared_replica":
    return False
  return tier == group


def _require_remote_env(environment: str) -> None:
  if environment == "dev":
    raise click.ClickException(
      "The 'instances' commands require staging or prod environment.\n"
      "There are no ASGs in local development."
    )


@click.group()
def instances():
  """Manage graph database instances and ASG capacity."""
  pass


@instances.command("list")
@click.pass_obj
def instances_list(client):
  """List all instances across tiers with capacity and ASG info."""
  _require_remote_env(client.environment)
  helper = InstancesHelper(client.environment, client.aws_profile)

  console.print("\n[dim]Fetching instances and ASG info...[/dim]")
  all_instances = helper.get_all_instances()
  all_asgs = helper.get_all_asg_info()

  by_group: dict[str, list[dict]] = {g: [] for g in ALL_ASG_GROUPS}
  ungrouped = []
  for inst in all_instances:
    node_type = inst.get("node_type", "")
    if node_type == "shared_replica":
      by_group["shared-replicas"].append(inst)
    else:
      tier = inst.get("cluster_tier") or inst.get("tier", "unknown")
      if tier in by_group:
        by_group[tier].append(inst)
      else:
        ungrouped.append(inst)

  for group in ALL_ASG_GROUPS:
    asg = all_asgs.get(group)
    group_instances = by_group[group]

    table = Table(
      title=f"{group}",
      show_header=True,
      header_style="bold cyan",
    )
    table.add_column("Instance ID", no_wrap=True)
    table.add_column("Status")
    table.add_column("DBs", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("Type")
    table.add_column("AZ")
    table.add_column("Last Health Check")

    for inst in sorted(group_instances, key=lambda x: x.get("instance_id", "")):
      db_count = inst.get("database_count", 0)
      max_dbs = inst.get("max_databases", 0)
      if isinstance(db_count, Decimal):
        db_count = int(db_count)
      if isinstance(max_dbs, Decimal):
        max_dbs = int(max_dbs)

      status = inst.get("status", "unknown")
      status_color = (
        "green"
        if status == "healthy"
        else "yellow"
        if status == "initializing"
        else "red"
      )

      last_hc = inst.get("last_health_check", "N/A")
      if last_hc and last_hc != "N/A" and len(last_hc) > 19:
        last_hc = last_hc[:19]

      table.add_row(
        inst.get("instance_id", "N/A"),
        f"[{status_color}]{status}[/{status_color}]",
        str(db_count),
        str(max_dbs),
        inst.get("instance_type", "N/A"),
        inst.get("availability_zone", "N/A"),
        last_hc,
      )

    if asg:
      desired = asg.get("DesiredCapacity", 0)
      min_s = asg.get("MinSize", 0)
      max_s = asg.get("MaxSize", 0)
      table.add_section()
      table.add_row(
        "[bold]ASG[/bold]",
        "",
        "",
        "",
        f"[dim]desired={desired} min={min_s} max={max_s}[/dim]",
        "",
        f"[dim]{len(group_instances)} instance(s)[/dim]",
      )
    else:
      table.add_section()
      table.add_row(
        "[bold]ASG[/bold]",
        "[dim]not found[/dim]",
        "",
        "",
        "",
        "",
        f"[dim]{len(group_instances)} instance(s)[/dim]",
      )

    console.print()
    console.print(table)

  if ungrouped:
    console.print(
      f"\n[yellow]Warning: {len(ungrouped)} instance(s) with unknown tier[/yellow]"
    )

  total = sum(len(v) for v in by_group.values()) + len(ungrouped)
  console.print(f"\n[bold]Total instances:[/bold] {total}")


@instances.command("info")
@click.argument("instance_id")
@click.pass_obj
def instances_info(client, instance_id):
  """Show detailed info for a specific instance."""
  _require_remote_env(client.environment)
  helper = InstancesHelper(client.environment, client.aws_profile)

  inst = helper.get_instance(instance_id)
  if not inst:
    raise click.ClickException(f"Instance {instance_id} not found in registry.")

  click.echo("\nINSTANCE DETAILS")
  click.echo("=" * 60)

  tier = inst.get("cluster_tier") or inst.get("tier", "N/A")
  status = inst.get("status", "unknown")
  db_count = inst.get("database_count", 0)
  max_dbs = inst.get("max_databases", 0)
  if isinstance(db_count, Decimal):
    db_count = int(db_count)
  if isinstance(max_dbs, Decimal):
    max_dbs = int(max_dbs)

  click.echo(f"\n  Instance ID:    {instance_id}")
  click.echo(f"  Tier:           {tier}")
  click.echo(f"  Status:         {status}")
  click.echo(f"  Instance Type:  {inst.get('instance_type', 'N/A')}")
  click.echo(f"  AZ:             {inst.get('availability_zone', 'N/A')}")
  click.echo(f"  Private IP:     {inst.get('private_ip', 'N/A')}")
  click.echo(f"  Node Type:      {inst.get('node_type', 'N/A')}")
  click.echo(f"  Backend:        {inst.get('backend_type', 'N/A')}")
  click.echo(f"  Databases:      {db_count} / {max_dbs}")
  click.echo(f"  Launch Time:    {inst.get('launch_time', 'N/A')}")
  click.echo(f"  Last Health:    {inst.get('last_health_check', 'N/A')}")
  click.echo(f"  Stack:          {inst.get('stack_name', 'N/A')}")

  node_type = inst.get("node_type", "")
  asg_group = "shared-replicas" if node_type == "shared_replica" else tier
  if asg_group in ALL_ASG_GROUPS:
    asg = helper.get_asg_info(asg_group)
    if asg:
      click.echo(f"\n  ASG:            {asg['AutoScalingGroupName']}")
      click.echo(f"  ASG Desired:    {asg['DesiredCapacity']}")
      click.echo(f"  ASG Min/Max:    {asg['MinSize']} / {asg['MaxSize']}")

  graphs = helper.get_graphs_for_instance(instance_id)
  if graphs:
    click.echo(f"\nALLOCATED GRAPHS ({len(graphs)})")
    click.echo("-" * 60)
    graph_table = Table(show_header=True, header_style="bold cyan")
    graph_table.add_column("Graph ID", no_wrap=True)
    graph_table.add_column("Status")
    graph_table.add_column("Created")
    graph_table.add_column("Last Accessed")

    for g in sorted(graphs, key=lambda x: x.get("graph_id", "")):
      created = g.get("created_at", "N/A")
      if created and created != "N/A" and len(created) > 10:
        created = created[:10]
      accessed = g.get("last_accessed", "N/A")
      if accessed and accessed != "N/A" and len(accessed) > 10:
        accessed = accessed[:10]
      graph_table.add_row(
        g.get("graph_id", "N/A"),
        g.get("status", "N/A"),
        created,
        accessed,
      )
    console.print(graph_table)
  else:
    click.echo("\nNo graphs allocated to this instance.")


@instances.command("scale")
@click.argument("tier", type=click.Choice(ALL_ASG_GROUPS))
@click.argument("desired", type=int)
@click.option("--min", "min_size", type=int, default=None, help="Also set ASG min size")
@click.option("--max", "max_size", type=int, default=None, help="Also set ASG max size")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def instances_scale(client, tier, desired, min_size, max_size, force):
  """Scale desired capacity for a tier's ASG.

  Updates the ASG immediately and syncs GHA variables so
  the next deploy doesn't reset capacity.
  """
  _require_remote_env(client.environment)
  helper = InstancesHelper(client.environment, client.aws_profile)

  asg = helper.get_asg_info(tier)
  if not asg:
    raise click.ClickException(
      f"ASG not found for tier {tier} in {client.environment}.\n"
      f"Expected: {helper._asg_name(tier)}"
    )

  current_desired = asg["DesiredCapacity"]
  current_min = asg["MinSize"]
  current_max = asg["MaxSize"]

  effective_max = max_size if max_size is not None else current_max
  effective_min = min_size if min_size is not None else current_min

  if desired > effective_max:
    raise click.ClickException(
      f"Desired capacity ({desired}) exceeds max size ({effective_max}).\n"
      f"Use --max {desired} to also raise the max."
    )

  if desired < effective_min:
    raise click.ClickException(
      f"Desired capacity ({desired}) is below min size ({effective_min}).\n"
      f"Use --min {desired} to also lower the min."
    )

  all_instances = helper.get_all_instances()
  tier_instances = [
    inst
    for inst in all_instances
    if _instance_matches_group(inst, tier) and inst.get("status") == "healthy"
  ]
  instances_with_graphs = [
    inst for inst in tier_instances if int(inst.get("database_count", 0)) > 0
  ]

  if desired < len(instances_with_graphs):
    raise click.ClickException(
      f"Cannot scale to {desired}: {len(instances_with_graphs)} instance(s) have active graphs.\n"
      f"Run 'instances cleanup' first to terminate empty instances."
    )

  console.print(f"\n[bold]{tier}[/bold] ({client.environment})")
  console.print(f"  Desired: {current_desired} -> [bold]{desired}[/bold]")
  if min_size is not None:
    console.print(f"  Min:     {current_min} -> [bold]{min_size}[/bold]")
  if max_size is not None:
    console.print(f"  Max:     {current_max} -> [bold]{max_size}[/bold]")

  if not force:
    click.confirm("\nApply these changes?", abort=True)

  helper.scale_asg(tier, desired, min_size=min_size, max_size=max_size)
  console.print("[green]ASG updated[/green]")

  gha_results = []

  if tier == "shared-replicas":
    ok = helper.sync_gha_variable(tier, "DESIRED", desired)
    var = helper._gha_var_name(tier, "DESIRED")
    gha_results.append((var, desired, ok))

  if max_size is not None:
    ok = helper.sync_gha_variable(tier, "MAX", max_size)
    var = helper._gha_var_name(tier, "MAX")
    gha_results.append((var, max_size, ok))

  if min_size is not None:
    ok = helper.sync_gha_variable(tier, "MIN", min_size)
    var = helper._gha_var_name(tier, "MIN")
    gha_results.append((var, min_size, ok))

  if gha_results:
    console.print("\n[bold]GHA variable sync:[/bold]")
    for var_name, value, ok in gha_results:
      status = "[green]synced[/green]" if ok else "[red]failed[/red]"
      console.print(f"  {var_name} = {value} {status}")
  else:
    console.print(
      "\n[dim]No GHA variables updated (only desired capacity changed).[/dim]\n"
      "[dim]Tip: Use --min/--max to persist changes across deploys.[/dim]"
    )

  console.print(f"\n[green]Scaled {tier} to desired={desired}[/green]")


@instances.command("cleanup")
@click.option("--force", is_flag=True, help="Skip confirmation prompt")
@click.pass_obj
def instances_cleanup(client, force):
  """Find and terminate instances with zero active graphs."""
  _require_remote_env(client.environment)
  helper = InstancesHelper(client.environment, client.aws_profile)

  console.print("\n[dim]Scanning instances and graphs...[/dim]")
  all_instances = helper.get_all_instances()
  all_graphs = helper.get_all_graphs()

  active_graph_instances: set[str] = set()
  for g in all_graphs:
    if g.get("status") not in ("deleted", "failed"):
      iid = g.get("instance_id")
      if iid:
        active_graph_instances.add(iid)

  empty_instances = []
  for inst in all_instances:
    iid = inst.get("instance_id", "")
    status = inst.get("status", "")
    if status != "healthy":
      continue
    if iid not in active_graph_instances:
      db_count = int(inst.get("database_count", 0))
      if db_count == 0:
        empty_instances.append(inst)

  if not empty_instances:
    console.print(
      "\n[green]No empty instances found. All instances have active graphs.[/green]"
    )
    return

  asg_info = helper.get_all_asg_info()
  group_empty_counts: dict[str, int] = {}
  for inst in empty_instances:
    node_type = inst.get("node_type", "")
    if node_type == "shared_replica":
      group = "shared-replicas"
    else:
      group = inst.get("cluster_tier") or inst.get("tier", "unknown")
    group_empty_counts[group] = group_empty_counts.get(group, 0) + 1

  table = Table(
    title=f"Empty Instances ({len(empty_instances)})",
    show_header=True,
    header_style="bold cyan",
  )
  table.add_column("Instance ID", no_wrap=True)
  table.add_column("Group")
  table.add_column("Type")
  table.add_column("AZ")
  table.add_column("Launch Time")

  terminatable = []
  for inst in sorted(
    empty_instances, key=lambda x: x.get("cluster_tier") or x.get("tier", "")
  ):
    node_type = inst.get("node_type", "")
    if node_type == "shared_replica":
      display_group = "shared-replicas"
    else:
      display_group = inst.get("cluster_tier") or inst.get("tier", "unknown")
    table.add_row(
      inst.get("instance_id", "N/A"),
      display_group,
      inst.get("instance_type", "N/A"),
      inst.get("availability_zone", "N/A"),
      inst.get("launch_time", "N/A")[:19] if inst.get("launch_time") else "N/A",
    )
    terminatable.append(inst)

  console.print()
  console.print(table)

  for group, count in group_empty_counts.items():
    asg = asg_info.get(group)
    if asg:
      min_s = asg["MinSize"]
      current = asg["DesiredCapacity"]
      if current - count < min_s:
        console.print(
          f"\n[yellow]Warning: Terminating all {count} empty {group} instance(s) "
          f"would bring desired ({current}) below min ({min_s}).[/yellow]"
        )

  if not terminatable:
    return

  if not force:
    click.confirm(
      f"\nTerminate {len(terminatable)} empty instance(s)? (desired capacity will decrement)",
      abort=True,
    )

  for inst in terminatable:
    iid = inst.get("instance_id", "")
    try:
      helper.terminate_instance(iid)
      console.print(f"  [green]Terminated {iid}[/green]")
    except Exception as e:
      console.print(f"  [red]Failed to terminate {iid}: {e}[/red]")

  console.print("\n[green]Cleanup complete.[/green]")
