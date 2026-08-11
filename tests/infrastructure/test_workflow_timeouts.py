"""Guards the recurring "job timeout shorter than its own wait budget" defect.

This class of bug has appeared three times in the graph refresh workflows, twice
introduced while collapsing code that carried the arithmetic:

  * `service-refresh.yml` set `timeout-minutes` to `max_wait_minutes` while its
    poll loop budgeted `max_wait_minutes + 15`.
  * `graph-asg-refresh.yml` used a fixed 45 against a worst case of 3 tiers x 30m
    plus a 20m completion wait.
  * `graph-maintenance.yml`'s cleanup used a fixed 15 against 20m per environment.

The symptom is nasty because it is silent and inverted: GitHub force-cancels the
runner *before* the job's own graceful timeout branch can fire, so a healthy but
slow run reports as a bare cancellation with no diagnostic, and the carefully
written "timed out waiting, use force-ignore-busy" message is unreachable.

The invariant: **a job whose wait budget scales with a workflow input cannot
express its timeout as a literal or as that same input.** It has to inflate the
input, and GitHub Actions expressions have no arithmetic — so the only correct
shape is a separate job that does the arithmetic in shell and exposes the result
as an output. This asserts that shape rather than trying to parse shell budgets.

A job whose internal budget is a *constant* is exempt: a literal timeout can be
checked by eye against a constant and cannot silently drift with an input.
"""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

# Inputs that carry a wait budget. A job that reads one of these — in its own
# shell, or by handing it to a composite action that waits — has an input-scaled
# budget and therefore needs a derived timeout.
WAIT_BUDGET_INPUTS = ("max_wait_minutes", "completion_wait_minutes")
WAIT_BUDGET_ACTION_INPUTS = ("max-wait-minutes", "completion-wait-minutes")

_DERIVED_TIMEOUT = re.compile(r"needs\.[\w-]+\.outputs\.\w*timeout\w*", re.I)


def _workflows() -> list[tuple[str, dict[str, Any]]]:
  out = []
  for path in sorted(WORKFLOW_DIR.glob("*.yml")):
    doc = yaml.safe_load(path.read_text())
    if isinstance(doc, dict) and isinstance(doc.get("jobs"), dict):
      out.append((path.name, doc))
  return out


def _is_budget_job(job: dict[str, Any]) -> bool:
  """A job that exposes a timeout as an output computes budgets, it does not wait.

  These necessarily read the wait input — that is their whole purpose — so
  without this exclusion every budget job would be flagged as needing a budget
  job of its own.
  """
  outputs = job.get("outputs") or {}
  return isinstance(outputs, dict) and any("timeout" in key.lower() for key in outputs)


def _job_consumes_a_wait_budget(job: dict[str, Any]) -> str | None:
  """Return the signal by which this job's budget scales with an input, or None."""
  if _is_budget_job(job):
    return None

  # Job-level env counts as much as step-level: a job that hoists the wait input
  # into env and reads it from a shell budget is the shape of the original bug.
  scopes: list[tuple[str, Any]] = [("the job's env", job.get("env") or {})]
  for step in job.get("steps") or []:
    if not isinstance(step, dict):
      continue
    scopes.append(("a step's run block", step.get("run") or ""))
    scopes.append(("a step's env", step.get("env") or {}))

    # Handing the budget to a composite action that does the waiting counts too —
    # that is exactly how graph-asg-refresh.yml hid its budget from the workflow.
    with_block = step.get("with") or {}
    if isinstance(with_block, dict):
      for name in WAIT_BUDGET_ACTION_INPUTS:
        if name in with_block:
          return f"it passes {name} to {step.get('uses', 'an action')}"

  for where, scope in scopes:
    text = (
      "\n".join(f"{k}={v}" for k, v in scope.items())
      if isinstance(scope, dict)
      else str(scope)
    )
    for name in WAIT_BUDGET_INPUTS:
      if f"inputs.{name}" in text:
        return f"{where} reads inputs.{name}"
  return None


def _budgeted_jobs() -> list[tuple[str, str, dict[str, Any], str]]:
  found = []
  for workflow, doc in _workflows():
    for job_name, job in doc["jobs"].items():
      if not isinstance(job, dict):
        continue
      signal = _job_consumes_a_wait_budget(job)
      if signal:
        found.append((workflow, job_name, job, signal))
  return found


def test_budgeted_jobs_are_discovered():
  """A silent discovery miss would make the guard below vacuously pass."""
  found = _budgeted_jobs()
  assert len(found) >= 2, f"expected at least 2 wait-budgeted jobs, found {found}"
  names = {(w, j) for w, j, _, _ in found}
  assert ("service-refresh.yml", "refresh-graph") in names
  assert ("graph-asg-refresh.yml", "refresh-asgs") in names


@pytest.mark.parametrize(
  ("workflow", "job_name", "job", "signal"),
  _budgeted_jobs(),
  ids=[f"{w}:{j}" for w, j, _, _ in _budgeted_jobs()],
)
def test_input_scaled_wait_budget_requires_a_derived_timeout(
  workflow: str, job_name: str, job: dict[str, Any], signal: str
):
  timeout = job.get("timeout-minutes")

  assert timeout is not None, (
    f"{workflow}:{job_name} has an input-scaled wait budget ({signal}) but no "
    "timeout-minutes at all, so it inherits GitHub's 360-minute default."
  )

  assert not isinstance(timeout, int), (
    f"{workflow}:{job_name} has an input-scaled wait budget ({signal}) but a "
    f"literal timeout-minutes: {timeout}. A literal cannot track an input — "
    "raising the wait would silently leave the job cancelled before its own "
    "timeout branch reports. Compute the timeout in a separate job's shell and "
    "read it via needs.<job>.outputs."
  )

  assert _DERIVED_TIMEOUT.search(str(timeout)), (
    f"{workflow}:{job_name} has an input-scaled wait budget ({signal}) but its "
    f"timeout-minutes does not read a budget job output: {timeout!r}. "
    "Expressions have no arithmetic, so referencing the wait input directly "
    "(e.g. fromJSON(inputs.max_wait_minutes)) yields the wait itself with no "
    "headroom — the exact shape of the bug this guards."
  )


@pytest.mark.parametrize(
  ("workflow", "job_name", "job"),
  [(w, j, job) for w, j, job, _ in _budgeted_jobs()],
  ids=[f"{w}:{j}" for w, j, _, _ in _budgeted_jobs()],
)
def test_budget_job_inflates_the_input(
  workflow: str, job_name: str, job: dict[str, Any]
):
  """The budget job must add headroom, not pass the wait through unchanged.

  A budget job that merely echoes `max_wait_minutes` would satisfy the shape
  check above while reproducing the original bug exactly.
  """
  doc = dict(_workflows())[workflow]
  match = _DERIVED_TIMEOUT.search(str(job.get("timeout-minutes")))
  assert match
  budget_job_name = match.group(0).split(".")[1]

  budget_job = doc["jobs"].get(budget_job_name)
  assert budget_job, (
    f"{workflow}:{job_name} derives its timeout from job '{budget_job_name}', "
    "which does not exist in this workflow."
  )
  assert budget_job_name in (job.get("needs") or []), (
    f"{workflow}:{job_name} reads {budget_job_name}'s output but does not list "
    "it in `needs`, so the output will be empty at evaluation time."
  )

  shell = "\n".join(
    step.get("run") or ""
    for step in budget_job.get("steps") or []
    if isinstance(step, dict)
  )

  # Comments are stripped before the check, and the check targets arithmetic
  # expansion specifically. The first version of this assertion searched the raw
  # shell for `+ <digits>` and was silently satisfied by the words "+10 slack" in
  # a comment — it passed against a budget job deliberately broken to pass the
  # input straight through. An assertion that cannot fail is worth nothing.
  code = re.sub(r"#.*$", "", shell, flags=re.M)
  assert re.search(r"\$\(\([^)]*\+[^)]*\)\)", code), (
    f"{workflow}:{budget_job_name} does not add headroom to the wait input in "
    "any arithmetic expansion. Passing it through unchanged reproduces the bug "
    "the budget job exists to prevent."
  )
