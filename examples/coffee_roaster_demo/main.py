#!/usr/bin/env python3
"""Driftline Coffee Roasters — RoboLedger Showcase Demo (Season 1, Episode 1).

A thin wrapper over ``examples._scenario.runner`` — the company *is* the
scenario. The Driftline arc (a profitable-but-cash-poor DTC + wholesale coffee
roaster whose cash drains into green-coffee inventory and one slow-paying
wholesale account) is authored as flow parameters in ``data.py``; run
``uv run python -m examples.coffee_roaster_demo.data`` for the offline preview.

Usage:
    uv run python -m examples.coffee_roaster_demo.main             # Create new graph + load
    uv run python -m examples.coffee_roaster_demo.main <graph_id>  # Load into existing graph
    uv run python -m examples.coffee_roaster_demo.main --dry-run   # Validate data only
    uv run python -m examples.coffee_roaster_demo.main --ai        # Use MappingOperator (requires Bedrock)

Requires: Docker stack running (just start)
"""

from __future__ import annotations

from pathlib import Path

from examples._scenario.runner import run_demo

from .agents import AGENTS
from .data import SCENARIO
from .disclosures import DISCLOSURE_NOTES
from .mappings import mappings_for
from .policies import DOCUMENTS

# Beat 4 — the unscripted reveal: profit up, cash down, traced to working capital.
REVEAL_PROMPTS = [
  "We look profitable — show me the income statement.",
  "But did cash go up or down over the period? Where did it go?",
  "Break down accounts receivable by customer and age it.",
  "How much cash is tied up in inventory vs. receivables?",
]


def main() -> None:
  run_demo(
    scenario=SCENARIO,
    agents=AGENTS,
    mappings_for=mappings_for,
    documents=DOCUMENTS,
    output_dir=Path(__file__).resolve().parent / "output",
    reveal_prompts=REVEAL_PROMPTS,
    disclosures=DISCLOSURE_NOTES,
  )


if __name__ == "__main__":
  main()
