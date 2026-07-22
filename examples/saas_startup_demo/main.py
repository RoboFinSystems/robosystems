#!/usr/bin/env python3
"""Cadence Labs — RoboLedger Showcase Demo (Season 1, Episode 2).

A thin wrapper over ``examples._scenario.runner`` — the company *is* the
scenario. Cadence Labs is a seed-funded B2B SaaS startup: high-margin
recurring revenue growing fast, but burning cash, with a large annual-prepay
deferred-revenue float masking the burn. The arc is authored as flow
parameters in ``data.py``; run
``uv run python -m examples.saas_startup_demo.data`` for the offline preview.

Usage:
    uv run python -m examples.saas_startup_demo.main             # Create new graph + load
    uv run python -m examples.saas_startup_demo.main <graph_id>  # Load into existing graph
    uv run python -m examples.saas_startup_demo.main --dry-run   # Validate data only
    uv run python -m examples.saas_startup_demo.main --ai        # Use MappingOperator (requires Bedrock)

Requires: Docker stack running (just start)
"""

from __future__ import annotations

from pathlib import Path

from examples._scenario.runner import run_demo

from .agents import AGENTS
from .data import SCENARIO
from .disclosures import DISCLOSURE_NOTES, TEXT_BLOCK_NOTES
from .mappings import mappings_for
from .memories import MEMORIES
from .metrics import CUSTOM_METRICS
from .policies import DOCUMENTS

# Beat 4 — the unscripted reveal: the deferred-revenue runway illusion.
REVEAL_PROMPTS = [
  "We raised a round and have cash in the bank — show me the income statement and cash position.",
  "Are we profitable? What is our monthly operating burn?",
  "How much of our cash is deferred revenue we still owe as service?",
  "Net out deferred revenue — at this burn, what's our real runway?",
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
    text_blocks=TEXT_BLOCK_NOTES,
    custom_metrics=CUSTOM_METRICS,
    memories=MEMORIES,
  )


if __name__ == "__main__":
  main()
