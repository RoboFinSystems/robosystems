"""Every subscription status change in the billing handlers reaches the grant.

Access to a repository reads the grant, not the billing row, so a branch that
moves `subscription.status` without reconciling the grant leaves access out of
step with what Stripe says (SS2, SS3, F18). A graph's access reads the row
itself; its teardown is the lifecycle sensors'. The scan fails when a new
branch changes status on a path that never reconciles, or when checkout starts
accepting a resource type that no lifecycle path handles.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

PACKAGE = Path(__file__).resolve().parents[1] / "robosystems"
BILLING = PACKAGE / "dagster" / "jobs" / "billing.py"
CHECKOUT = PACKAGE / "routers" / "billing" / "checkout.py"
RECONCILERS = {"_reconcile_repository_grant", "reconcile_repository_grant"}

# Status changes that happen before any grant exists, so there is nothing to
# reconcile. Each entry says why.
BEFORE_ANY_GRANT = {
  # Reopens a row with no resource_id for the provisioning claim.
  "_handle_checkout_completed",
  # Fails the row before provisioning starts (no owner, unknown type).
  "_fail_subscription",
}

# Where each resource type's access ends when its subscription does.
LIFECYCLE_PATHS = {
  "graph": (
    PACKAGE / "dagster" / "sensors" / "graph_lifecycle.py",
    'BillingSubscription.resource_type == "graph"',
  ),
  "repository": (
    PACKAGE / "operations" / "billing" / "repository_subscriptions.py",
    'subscription.resource_type != "repository"',
  ),
}


def _functions(tree: ast.Module) -> dict[str, ast.AST]:
  return {
    node.name: node
    for node in tree.body
    if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
  }


def _calls(func: ast.AST) -> set[str]:
  names = set()
  for node in ast.walk(func):
    if isinstance(node, ast.Call):
      target = node.func
      names.add(
        target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
      )
  return names


def _changes_status(func: ast.AST) -> bool:
  for node in ast.walk(func):
    if isinstance(node, ast.Assign):
      for target in node.targets:
        if (
          isinstance(target, ast.Attribute)
          and target.attr == "status"
          and isinstance(target.value, ast.Name)
          and "sub" in target.value.id
        ):
          return True
    if (
      isinstance(node, ast.Call)
      and isinstance(node.func, ast.Attribute)
      and node.func.attr == "cancel"
      and isinstance(node.func.value, ast.Name)
      and "sub" in node.func.value.id
    ):
      return True
  return False


def _reconciled(
  name: str, calls: dict[str, set[str]], seen: frozenset = frozenset()
) -> bool:
  """The function reconciles, or every function that calls it does."""
  if calls[name] & RECONCILERS:
    return True
  callers = [caller for caller, called in calls.items() if name in called]
  if not callers or name in seen:
    return False
  return all(_reconciled(caller, calls, seen | {name}) for caller in callers)


def test_every_status_change_reaches_a_grant_reconcile():
  functions = _functions(ast.parse(BILLING.read_text(), filename=str(BILLING)))
  calls = {name: _calls(func) for name, func in functions.items()}
  unreconciled = sorted(
    name
    for name, func in functions.items()
    if _changes_status(func)
    and name not in BEFORE_ANY_GRANT
    and not _reconciled(name, calls)
  )
  assert unreconciled == []


def test_the_allow_list_names_live_status_changes():
  functions = _functions(ast.parse(BILLING.read_text(), filename=str(BILLING)))
  stale = sorted(
    name
    for name in BEFORE_ANY_GRANT
    if name not in functions or not _changes_status(functions[name])
  )
  assert stale == []


def test_every_resource_type_checkout_accepts_has_a_lifecycle_path():
  accepted = set(re.findall(r'request\.resource_type == "(\w+)"', CHECKOUT.read_text()))
  assert accepted == set(LIFECYCLE_PATHS)
  for resource_type, (path, marker) in LIFECYCLE_PATHS.items():
    assert marker in path.read_text(), resource_type
