#!/usr/bin/env python3
"""Cadence Labs — RoboLedger showcase demo (Season 1, Episode 2).

Loads a whole synthetic company into a RoboLedger graph and leaves it ready for
a month-end close: chart of accounts, counterparty agents, 16 months of typed
REA events, CoA→rs-gaap mappings, fiscal calendar, schedules, disclosure notes,
policy documents, and a filed annual report.

Cadence Labs is a seed-funded B2B SaaS startup burning cash. Recurring revenue
is high-margin and growing fast, but engineering and go-to-market spend swamps
gross profit every month. Customers sign annual contracts and pay up front, so
a large deferred-revenue float sits on the balance sheet and flatters the bank
balance — the runway looks longer than it is until you net the float out. The
arc is authored as flow parameters in ``data.py``.

This module is a thin wrapper over ``examples._scenario.runner``: the company
*is* the scenario, so a new episode is a new set of data modules, not a fork of
the runner. For the offline arc preview (no platform needed), run
``uv run python -m examples.saas_startup_demo.data``.

Prerequisites:
    just start        # Docker stack (API, PostgreSQL, Valkey, LadybugDB)
    just demo-user    # writes credentials to .local/config.json

Usage:
    just demo-saas-startup              # create a graph and load everything
    just demo-saas-startup <graph_id>   # load into an existing graph
    just demo-saas-startup --dry-run    # validate the synthetic data only
    just demo-saas-startup --ai         # map the CoA with the MappingOperator
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
# client against the loaded graph. Cadence's reveal: the bank balance implies a
# runway that disappears once deferred revenue is netted out.
REVEAL_PROMPTS = [
  "We raised a round and have cash in the bank — show me the income statement and cash position.",
  "Are we profitable? What is our monthly operating burn?",
  "How much of our cash is deferred revenue we still owe as service?",
  "Net out deferred revenue — at this burn, what's our real runway?",
]

# Operating-budget scenario — Cadence's growth/burn arc as lever assertions:
# SaaS growth continuing the historical ramp (~5%/month), the ~22% hosting +
# support cost-of-revenue rate, a month of hosting bills in payables. DSO is
# deliberately NOT asserted — an annual-prepay book has no receivables story,
# so the DSO rule stays inactive and the working-capital projection is the
# payables side only. That partial-lever path is the point: each episode
# asserts only the levers its business model actually turns. Values follow the
# rs-driver catalog conventions (percent levers as decimals per month, days
# levers as day counts).
FORECAST_LEVERS = {
  "rs-driver:RevenueGrowthRate": 0.05,
  "rs-driver:CostOfRevenueRate": 0.22,
  "rs-driver:DaysPayableOutstanding": 30,
}


def main(argv: list[str] | None = None) -> None:
  """Run the episode. ``argv`` defaults to ``sys.argv`` inside ``run_demo``.

  It is a parameter so another demo can provision this company as a
  dependency — ``examples/roboinvestor_demo`` needs a real issuer with a
  filed report to share — without that demo's own flags leaking in as
  scenario flags.
  """
  run_demo(
    scenario=SCENARIO,
    argv=argv,
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
