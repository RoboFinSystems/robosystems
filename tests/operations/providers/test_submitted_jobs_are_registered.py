"""Every Dagster job a connection provider can submit must exist in Definitions.

A provider submits by *name*. Nothing links that string to the job it refers to,
so a job that is renamed, retired, or never registered leaves the call site
compiling, importing and unit-testing green — and failing only at the scheduler,
for a customer, on a path a feature flag may keep dark for months.

This walks the provider modules for `job_name="..."` literals and asserts each
one resolves. It is deliberately a source scan rather than a call: submitting a
job needs a live Dagster instance, and the property under test is the name, not
the submission.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

PROVIDERS_DIR = Path(__file__).resolve().parents[3] / "robosystems/operations/providers"


def _submitted_job_names() -> dict[str, str]:
  """Every literal ``job_name=`` keyword argument under ``providers/``."""
  found: dict[str, str] = {}
  for path in sorted(PROVIDERS_DIR.glob("*.py")):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
      if not isinstance(node, ast.Call):
        continue
      for keyword in node.keywords:
        if keyword.arg != "job_name":
          continue
        if isinstance(keyword.value, ast.Constant) and isinstance(
          keyword.value.value, str
        ):
          found[keyword.value.value] = path.name
  return found


SUBMITTED = _submitted_job_names()


@pytest.mark.unit
def test_the_scan_finds_something_to_guard():
  """A parametrize over an empty set collects zero cases and reports success.

  That is the same green-for-the-wrong-reason failure this module exists to
  catch, one level up: if a ``job_name=`` argument stops being a literal, or the
  provider modules move, the guard below would silently stop guarding.
  """
  assert SUBMITTED, (
    f"no literal job_name= arguments found under {PROVIDERS_DIR.name}/ — either "
    "no provider submits a job any more, or the scan stopped matching."
  )


@pytest.mark.unit
@pytest.mark.parametrize("job_name", sorted(SUBMITTED))
def test_submitted_job_is_registered(job_name):
  from robosystems.dagster.definitions import all_jobs

  registered = {job.name for job in all_jobs}
  assert job_name in registered, (
    f"{SUBMITTED[job_name]} submits job_name={job_name!r}, which is not in "
    "Definitions. Either register it, or stop submitting it — a name that "
    "resolves to nothing fails at the scheduler, not here."
  )
