#!/usr/bin/env python3
"""
SEC Graph Analytics Demo

Demonstrates statement classification and element normalization
using icebug graph analytics on XBRL financial data.

Usage:
    uv run examples/analytics_demo/main.py sec                # Both stages (graph_id=sec)
    uv run examples/analytics_demo/main.py sec --stage 1      # Classification only
    uv run examples/analytics_demo/main.py sec --stage 2      # Normalization only
"""

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
  sys.path.insert(0, str(PROJECT_ROOT))

from rich.console import Console
from rich.table import Table

from robosystems.adapters.sec.analytics import (
  ArcExtractor,
  ElementNormalizer,
  StatementClassifier,
)
from robosystems.adapters.sec.analytics.classifiers import StatementType
from robosystems.adapters.sec.analytics.graphs import (
  build_cooccurrence_graph,
  build_element_graph,
)

console = Console()


def run_classification(extractor: ArcExtractor) -> None:
  """Stage 1: Classify elements into financial statements."""
  console.rule("[bold blue]Stage 1: Statement Classification")

  console.print("\n[dim]Extracting calculation arcs...[/dim]")
  calc_arcs = extractor.extract_calculation_arcs()
  console.print(f"  Found {len(calc_arcs):,} calculation arcs")

  console.print("[dim]Extracting presentation arcs...[/dim]")
  pres_arcs = extractor.extract_presentation_arcs()
  console.print(f"  Found {len(pres_arcs):,} presentation arcs")

  console.print("[dim]Building element graph...[/dim]")
  element_graph = build_element_graph(calc_arcs, pres_arcs)
  console.print(
    f"  Graph: {element_graph.num_nodes:,} nodes, {element_graph.num_edges:,} edges"
  )

  console.print("[dim]Running classification...[/dim]")
  classifier = StatementClassifier()
  result = classifier.classify(element_graph)

  # Summary table
  summary_table = Table(title="Classification Summary")
  summary_table.add_column("Statement", style="cyan")
  summary_table.add_column("Elements", justify="right", style="green")

  for stmt_name, count in sorted(result.summary().items()):
    summary_table.add_row(stmt_name, str(count))
  summary_table.add_row(
    "[bold]Total Classified[/bold]",
    f"[bold]{result.total_classified}[/bold]",
  )
  summary_table.add_row(
    "[dim]Unclassified[/dim]",
    f"[dim]{result.total_unclassified}[/dim]",
  )
  console.print(summary_table)

  # Detail tables per statement
  for stmt_type in StatementType:
    elements = result.get_statement_elements(stmt_type)
    if not elements:
      continue

    detail_table = Table(title=f"\n{stmt_type.value} Elements (top 15)")
    detail_table.add_column("Element", style="white")
    detail_table.add_column("Depth", justify="right", style="yellow")
    detail_table.add_column("Weight", justify="right", style="magenta")
    detail_table.add_column("Via Root", style="dim")

    for qname in elements[:15]:
      entries = result.classifications[qname]
      primary = min(entries, key=lambda c: c.depth)
      detail_table.add_row(
        qname,
        str(primary.depth),
        f"{primary.weight:.2f}",
        primary.via_root,
      )

    console.print(detail_table)

  # Multi-statement elements
  multi = {
    q: entries
    for q, entries in result.classifications.items()
    if len({e.statement for e in entries}) > 1
  }
  if multi:
    multi_table = Table(title="\nMulti-Statement Elements")
    multi_table.add_column("Element", style="white")
    multi_table.add_column("Statements", style="cyan")

    for qname, entries in sorted(multi.items())[:20]:
      stmts = sorted({e.statement.value for e in entries})
      multi_table.add_row(qname, ", ".join(stmts))

    console.print(multi_table)

  # Structural analysis
  console.print("\n[dim]Running structural analysis...[/dim]")
  structure = classifier.analyze_structure(element_graph)
  struct_table = Table(title="Graph Structure")
  struct_table.add_column("Metric", style="cyan")
  struct_table.add_column("Value", justify="right", style="green")
  for key, val in structure.items():
    struct_table.add_row(key.replace("_", " ").title(), str(val))
  console.print(struct_table)


def run_normalization(extractor: ArcExtractor) -> None:
  """Stage 2: Normalize elements via clustering."""
  console.rule("[bold blue]Stage 2: Element Normalization")

  console.print("\n[dim]Extracting calculation arcs...[/dim]")
  calc_arcs = extractor.extract_calculation_arcs()
  console.print(f"  Found {len(calc_arcs):,} calculation arcs")

  console.print("[dim]Building co-occurrence graph (single company)...[/dim]")
  element_graph = build_cooccurrence_graph([calc_arcs])
  console.print(
    f"  Graph: {element_graph.num_nodes:,} nodes, {element_graph.num_edges:,} edges"
  )

  if element_graph.num_nodes == 0:
    console.print("[yellow]No elements found. Skipping normalization.[/yellow]")
    return

  console.print("[dim]Running normalization...[/dim]")
  normalizer = ElementNormalizer()
  result = normalizer.normalize(element_graph)

  # Summary
  console.print(f"\n  Clusters found: {result.num_clusters}")
  console.print(f"  Elements mapped: {result.num_elements}")

  # Top clusters
  multi_member_clusters = [c for c in result.clusters if len(c.members) >= 2]
  if multi_member_clusters:
    cluster_table = Table(title="Element Clusters (multi-member)")
    cluster_table.add_column("ID", justify="right", style="dim")
    cluster_table.add_column("Canonical", style="cyan bold")
    cluster_table.add_column("Members", justify="right", style="green")
    cluster_table.add_column("Confidence", justify="right", style="magenta")
    cluster_table.add_column("Sample Members", style="white")

    for cluster in multi_member_clusters[:20]:
      others = [m for m in cluster.members if m != cluster.canonical]
      sample = ", ".join(others[:3])
      if len(others) > 3:
        sample += f" (+{len(others) - 3} more)"
      cluster_table.add_row(
        str(cluster.cluster_id),
        cluster.canonical,
        str(len(cluster.members)),
        f"{cluster.confidence:.3f}",
        sample,
      )

    console.print(cluster_table)

  # Link predictions
  console.print("\n[dim]Running link prediction (Jaccard)...[/dim]")
  predictions = normalizer.find_missing_links(element_graph, top_k=10)
  if predictions:
    pred_table = Table(title="Predicted Missing Equivalences")
    pred_table.add_column("Element 1", style="white")
    pred_table.add_column("Element 2", style="white")
    pred_table.add_column("Jaccard Score", justify="right", style="magenta")

    for e1, e2, score in predictions:
      pred_table.add_row(e1, e2, f"{score:.3f}")

    console.print(pred_table)
  else:
    console.print("  [dim]No strong predictions found.[/dim]")

  console.print(
    "\n[yellow]Note: Normalization works best with 5+ companies. "
    "Single-company results are degraded.[/yellow]"
  )


STAGING_DIR = PROJECT_ROOT / "data" / "staging"


def main() -> None:
  parser = argparse.ArgumentParser(description="SEC Graph Analytics Demo")
  parser.add_argument(
    "graph_id",
    help="Graph ID (resolves to ./data/staging/<graph_id>.duckdb)",
  )
  parser.add_argument(
    "--stage",
    choices=["1", "2", "both"],
    default="both",
    help="Which stage to run: 1=classification, 2=normalization, both (default: both)",
  )
  args = parser.parse_args()

  db_path = STAGING_DIR / f"{args.graph_id}.duckdb"
  if not db_path.exists():
    console.print(f"[red]Database not found: {db_path}[/red]")
    console.print("[dim]Load SEC data first: just sec-load NVDA 2025[/dim]")
    sys.exit(1)

  console.rule("[bold green]SEC Graph Analytics Demo")
  console.print(f"  Database: {db_path}")

  extractor = ArcExtractor(db_path)

  if args.stage in ("1", "both"):
    run_classification(extractor)

  if args.stage in ("2", "both"):
    run_normalization(extractor)

  console.rule("[bold green]Done")


if __name__ == "__main__":
  main()
