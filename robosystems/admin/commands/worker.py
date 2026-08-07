"""Worker administration commands: DLQ management and queue inspection.

Unlike the rest of the admin CLI, these talk to Valkey directly rather than
through the admin API — worker queues live in Valkey DB 6 and have no admin
endpoints. They therefore work only where Valkey is reachable, i.e. local dev.
Inspect staging and prod through CloudWatch metrics or the Dagster UI instead.
"""

import json

import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def worker():
  """Background worker operations."""
  pass


@worker.group()
def dlq():
  """Dead letter queue management."""
  pass


@dlq.command("list")
@click.option("--limit", "-n", default=20, help="Max entries to show")
@click.pass_obj
def dlq_list(client, limit):
  """Show DLQ contents."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    total = queue.llen("worker:dlq")
    if total == 0:
      console.print("[green]DLQ is empty[/green]")
      return

    entries = queue.lrange("worker:dlq", 0, limit - 1)

    table = Table(
      title=f"Dead Letter Queue ({total} total)",
      show_header=True,
      header_style="bold red",
    )
    table.add_column("#", justify="right")
    table.add_column("Task ID")
    table.add_column("Type")
    table.add_column("Graph ID")
    table.add_column("Attempts", justify="right")

    for i, entry_json in enumerate(entries):
      try:
        entry = json.loads(entry_json)
        table.add_row(
          str(i + 1),
          entry.get("task_id", "?"),
          entry.get("task_type", "?"),
          entry.get("graph_id", "?"),
          str(entry.get("attempt", "?")),
        )
      except json.JSONDecodeError:
        table.add_row(str(i + 1), "?", "malformed", "", "")

    console.print()
    console.print(table)

    if total > limit:
      console.print(
        f"\n[dim]Showing {limit} of {total}. Use --limit to see more.[/dim]"
      )
  finally:
    queue.close()


@dlq.command("retry")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_obj
def dlq_retry(client, yes):
  """Requeue all DLQ tasks for another attempt."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    total = queue.llen("worker:dlq")
    if total == 0:
      console.print("[green]DLQ is empty, nothing to retry[/green]")
      return

    if not yes:
      click.confirm(f"Requeue {total} DLQ tasks?", abort=True)

    requeued = 0
    while True:
      task_json = queue.lpop("worker:dlq")
      if task_json is None:
        break

      try:
        task_data = json.loads(task_json)
        task_data["attempt"] = 1  # Reset attempt counter
        queue.rpush("worker:tasks", json.dumps(task_data))
        requeued += 1
      except json.JSONDecodeError:
        console.print("[yellow]Skipped malformed entry[/yellow]")

    console.print(f"[green]Requeued {requeued} tasks[/green]")
  finally:
    queue.close()


@dlq.command("clear")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_obj
def dlq_clear(client, yes):
  """Purge all DLQ entries."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    total = queue.llen("worker:dlq")
    if total == 0:
      console.print("[green]DLQ is empty[/green]")
      return

    if not yes:
      click.confirm(f"Delete {total} DLQ entries permanently?", abort=True)

    queue.delete("worker:dlq")
    console.print(f"[green]Cleared {total} DLQ entries[/green]")
  finally:
    queue.close()


@worker.command("status")
@click.pass_obj
def worker_status(client):
  """Show worker queue status."""
  from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client

  queue = create_redis_client(ValkeyDatabase.WORKER_QUEUE, decode_responses=True)
  try:
    queue_depth = queue.llen("worker:tasks")
    dlq_depth = queue.llen("worker:dlq")

    # Count inflight tasks across all workers
    inflight_keys = list(queue.scan_iter(match="worker:inflight:*", count=100))
    inflight_total = sum(queue.llen(k) for k in inflight_keys)

    console.print()
    console.print("[bold cyan]WORKER STATUS[/bold cyan]")
    console.print("=" * 40)
    console.print(f"  Queue depth:    {queue_depth}")
    console.print(
      f"  In-flight:      {inflight_total} (across {len(inflight_keys)} workers)"
    )
    console.print(f"  DLQ:            {dlq_depth}")
  finally:
    queue.close()
