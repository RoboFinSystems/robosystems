"""
Shared output utilities for query scripts.

Provides standardized output formatting for table, JSON, and CSV outputs
across all database query tools (graph_query, kuzu_query, duckdb_query).
"""

import csv
import io
import json
from typing import Any

from rich.console import Console
from rich.table import Table


def format_value(value: Any) -> str:
  if value is None:
    return "NULL"
  elif isinstance(value, (dict, list)):
    return json.dumps(value, default=str)
  else:
    return str(value)


def print_table(
  results: list[dict[str, Any]],
  title: str | None = None,
  row_count_label: str = "Total rows",
) -> None:
  console = Console()

  if not results:
    if title:
      console.print(f"\n[bold]{title}[/bold]")
    console.print(f"\n{row_count_label}: 0")
    return

  table = Table(title=title, show_header=True, header_style="bold cyan")

  column_names = list(results[0].keys())
  for col in column_names:
    table.add_column(col, overflow="fold")

  for row in results:
    formatted_row = [format_value(row.get(col)) for col in column_names]
    table.add_row(*formatted_row)

  console.print()
  console.print(table)
  console.print(f"\n{row_count_label}: {len(results):,}")


def print_json(results: list[dict[str, Any]]) -> None:
  print(json.dumps(results, indent=2, default=str))


def print_csv(results: list[dict[str, Any]]) -> None:
  if not results:
    print("")
    return

  output = io.StringIO()
  writer = csv.DictWriter(output, fieldnames=results[0].keys())

  writer.writeheader()
  for row in results:
    writer.writerow(row)

  print(output.getvalue())


def print_info_section(
  title: str, subtitle: str | None = None, width: int = 60
) -> None:
  console = Console()
  console.print(f"\n[bold cyan]{'=' * width}[/bold cyan]")
  console.print(f"[bold cyan]{title}[/bold cyan]")
  if subtitle:
    console.print(f"[cyan]{subtitle}[/cyan]")
  console.print(f"[bold cyan]{'=' * width}[/bold cyan]")


def print_info_field(label: str, value: Any, indent: int = 0) -> None:
  console = Console()
  prefix = " " * indent
  formatted_value = format_value(value)
  console.print(f"{prefix}[bold]{label}:[/bold] {formatted_value}")


def print_success(message: str) -> None:
  console = Console()
  console.print(f"[green]✓[/green] {message}")


def print_error(message: str) -> None:
  console = Console()
  console.print(f"[red]✗[/red] {message}")


def print_warning(message: str) -> None:
  console = Console()
  console.print(f"[yellow]⚠[/yellow] {message}")
