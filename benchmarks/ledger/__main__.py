"""Command line: `uv run python -m benchmarks.ledger <tools|run> ...`."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path
from typing import Any

import httpx

from .fixture import Company, inbox_lines, load_company, plant, schedule_entries
from .loop import Transcript, run_trial
from .mcp import ToolDef
from .providers import make_provider
from .rows import ROWS, tools_for
from .score import (
  Score,
  balance_diff,
  expected_delta,
  judge_line,
  ledger_state,
  outcome,
)
from .tasks import SYSTEM_PREAMBLE, TASKS, BankLine, Task, prompt

_RUNS = Path(__file__).with_name("runs")
_ROOT = Path(__file__).resolve().parents[2]


def _surface_report(row: str, tools: list[ToolDef]) -> str:
  chars = sum(t.chars for t in tools)
  return f"row {row}: {len(tools)} tools, {chars:,} chars (~{chars // 4:,} tokens)"


def _stack_version(company: Company) -> str:
  status = httpx.get(f"{company.mcp.base_url}/v1/status", timeout=10)
  return status.json()["details"]["version"]


def _git_sha() -> str:
  done = subprocess.run(
    ["git", "rev-parse", "--short", "HEAD"], cwd=_ROOT, capture_output=True, text=True
  )
  return done.stdout.strip()


def _score(
  company: Company,
  task: Task,
  lines: tuple[BankLine, ...],
  expected: Any,
  before: Any,
  baseline_codes: set[str],
  bill_ids: dict[str, str],
  line_ids: dict[str, str],
  model_finished: bool,
  close_overrides: list[str],
) -> Score:
  company.refresh_chart()
  after = ledger_state(company.mcp, company.period, company.period_end)
  verdicts = [
    judge_line(
      line,
      company.mcp.read("get-event-block", {"id": line_ids[line.key]}),
      company.code_by_id,
      baseline_codes,
      company.trait_by_code,
      bill_ids,
    )
    for line in lines
  ]
  closed_through = company.mcp.read("get-fiscal-calendar", {})["closed_through"]
  period_closed = closed_through >= company.period
  diff = balance_diff(before, after, expected, company.trait_by_code, baseline_codes)
  return Score(
    outcome(verdicts, task, period_closed, diff, model_finished, close_overrides),
    verdicts,
    period_closed,
    diff,
    close_overrides,
  )


def _close_overrides(transcript: Transcript) -> list[str]:
  """The gate-lifting flags the model passed on a close that went through."""
  return sorted(
    {
      flag
      for event in transcript.tool_events
      if event.name == "close-period" and not event.is_error
      for flag, value in event.args.items()
      if flag.startswith("allow_") and value is True
    }
  )


def _trial(
  args: argparse.Namespace, task: Task, row: str, index: int, out: Path
) -> dict:
  company = load_company(reset=not args.no_reset)
  tools = tools_for(row, company.mcp.list_tools())
  system = f"{SYSTEM_PREAMBLE}\n\n{company.mcp.instructions()}"
  user_text = prompt(task, company.period, company.day)
  baseline_codes = set(company.id_by_code)
  before = ledger_state(company.mcp, company.period, company.period_end)
  inbox, inbox_ids = inbox_lines(company) if task.inbox else ((), {})
  expected = expected_delta(task, inbox, schedule_entries(company))
  bill_ids, planted_ids = plant(company, task)
  lines = (*inbox, *task.lines)
  line_ids = {**inbox_ids, **planted_ids}

  if args.dry_run:
    transcript = Transcript(args.model, stop_reason="dry_run")
  else:
    transcript = run_trial(
      make_provider(args.model, args.effort),
      system,
      user_text,
      tools,
      company.mcp.call,
      args.max_turns,
      args.max_usd,
    )
  finished = transcript.stop_reason in ("end_turn", "dry_run")
  score = _score(
    company,
    task,
    lines,
    expected,
    before,
    baseline_codes,
    bill_ids,
    line_ids,
    finished,
    _close_overrides(transcript),
  )
  record = {
    "task": task.name,
    "on_script": task.on_script,
    "row": row,
    "model": args.model,
    "effort": args.effort or "model default",
    "trial": index,
    "graph_id": company.mcp.graph_id,
    "period": company.period,
    "stack_version": _stack_version(company),
    "harness_sha": _git_sha(),
    "surface": _surface_report(row, tools),
    "system": system,
    "prompt": user_text,
    "expected_delta_cents": dict(expected),
    "score": score.as_dict(),
    "transcript": transcript.as_dict(),
  }
  name = f"{task.name}-{row}-{args.model}-{index}.json"
  (out / name).write_text(json.dumps(record, indent=1, default=str))
  cost = "n/a" if transcript.cost_usd is None else f"${transcript.cost_usd:.3f}"
  print(
    f"{task.name:13} {row} {args.model:18} {score.outcome:10} "
    f"turns={transcript.turns:<3} calls={len(transcript.tool_events):<3} "
    f"errors={transcript.tool_errors:<2} {cost:>8} {transcript.wall_s:>6.0f}s "
    f"stop={transcript.stop_reason}",
    flush=True,
  )
  for verdict in score.items:
    if verdict.verdict != "correct":
      print(f"    {verdict.key}: {verdict.verdict} ({verdict.status}) {verdict.detail}")
  if score.close_overrides:
    print(f"    closed over a gate: {score.close_overrides}")
  if score.balance_diff_cents:
    print(f"    balance diff (cents): {score.balance_diff_cents}")
  return record


def _run(args: argparse.Namespace) -> None:
  names = list(TASKS) if args.task == "all" else args.task.split(",")
  unknown = [n for n in names if n not in TASKS]
  if unknown:
    raise SystemExit(f"unknown task(s) {unknown}; have {list(TASKS)}")
  out = _RUNS / time.strftime("%Y%m%d-%H%M%S")
  out.mkdir(parents=True)
  records = [
    _trial(args, TASKS[name], row, index, out)
    for row in args.row.split(",")
    for name in names
    for index in range(1, args.k + 1)
  ]
  costs = [r["transcript"]["cost_usd"] or 0 for r in records]
  print(f"\n{len(records)} trials, ${sum(costs):.2f}, records in {out}")


def _tools(args: argparse.Namespace) -> None:
  company = load_company(reset=False)
  surface = company.mcp.list_tools()
  _RUNS.mkdir(exist_ok=True)
  for row in ROWS:
    tools = tools_for(row, surface)
    print(_surface_report(row, tools))
    (_RUNS / f"tools-{row}.json").write_text(
      json.dumps([t.as_dict() for t in tools], indent=1)
    )
  print(f"what each row's model receives is in {_RUNS}/tools-<row>.json")


def main() -> None:
  parser = argparse.ArgumentParser(prog="benchmarks.ledger")
  commands = parser.add_subparsers(dest="command", required=True)
  commands.add_parser("tools", help="size each row's tool list and dump it")
  run = commands.add_parser("run", help="run trials and score them")
  run.add_argument("--model", default="claude-sonnet-5-5")
  run.add_argument("--effort", default=None, help="low|medium|high|xhigh|max")
  run.add_argument("--row", default="C", help=f"comma list of {list(ROWS)}")
  run.add_argument("--task", default="all", help="comma list of task names, or all")
  run.add_argument("--k", type=int, default=1)
  run.add_argument("--max-turns", type=int, default=60)
  run.add_argument("--max-usd", type=float, default=3.0, help="stop a trial past this")
  run.add_argument("--no-reset", action="store_true", help="reuse the loaded company")
  run.add_argument("--dry-run", action="store_true", help="plant and score, no model")
  args = parser.parse_args()
  {"tools": _tools, "run": _run}[args.command](args)


if __name__ == "__main__":
  main()
