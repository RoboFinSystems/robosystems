#!/usr/bin/env python3
"""Driftline Coffee Roasters — RoboLedger showcase demo (Season 1, Episode 1).

Loads a whole synthetic company into a RoboLedger graph and leaves it ready for
a month-end close: chart of accounts, counterparty agents, 16 months of typed
REA events, CoA→rs-gaap mappings, fiscal calendar, schedules, disclosure notes,
policy documents, and a filed annual report.

Driftline is *profitable but cash-poor*. The P&L glows while cash drains into
green-coffee inventory bought ahead of demand and into one large wholesale
account (Summit Markets) that slips from net-30 to net-90. That squeeze is not
hand-written — the arc is authored as flow parameters in ``data.py`` and the
engine derives the balanced transactions, so the working-capital gap emerges
mechanically from recognition running ahead of cash.

This module is a thin wrapper over ``examples._scenario.runner``: the company
*is* the scenario, so a new episode is a new set of data modules, not a fork of
the runner. For the offline arc preview (no platform needed), run
``uv run python -m examples.coffee_roaster_demo.data``.

Prerequisites:
    just start        # Docker stack (API, PostgreSQL, Valkey, LadybugDB)
    just demo-user    # writes credentials to .local/config.json

Usage:
    just demo-coffee-roaster              # create a graph and load everything
    just demo-coffee-roaster <graph_id>   # load into an existing graph
    just demo-coffee-roaster --dry-run    # validate the synthetic data only
    just demo-coffee-roaster --ai         # map the CoA with the MappingOperator
                                          #   (requires AWS Bedrock)

Expect several minutes of progress output ending in a summary with the graph
id, the period queued for close, the filed report id, and the reveal prompts
below to try against an MCP client.
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

# Beat 4 — the analysis questions printed at the end of the run, to ask an MCP
# client against the loaded graph. Driftline's reveal: profit is up, cash is
# down, and the whole gap traces to working capital.
REVEAL_PROMPTS = [
  "We look profitable — show me the income statement.",
  "But did cash go up or down over the period? Where did it go?",
  "Break down accounts receivable by customer and age it.",
  "How much cash is tied up in inventory vs. receivables?",
]

# Operating-budget scenario — Driftline's working-capital arc as lever
# assertions: modest growth, roaster margins, the slow-paying wholesale account
# baked into DSO. Values follow the rs-driver catalog conventions: percent
# levers are decimals per month, days levers are day counts.
FORECAST_LEVERS = {
  "rs-driver:RevenueGrowthRate": 0.03,
  "rs-driver:CostOfRevenueRate": 0.62,
  "rs-driver:DaysSalesOutstanding": 45,
  "rs-driver:DaysPayableOutstanding": 30,
}


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
    forecast_levers=FORECAST_LEVERS,
  )


if __name__ == "__main__":
  main()
