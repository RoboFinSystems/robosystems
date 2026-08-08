"""Structural coverage of TASK_TIMEOUTS against the worker task registry.

A task type with no ``TASK_TIMEOUTS`` entry silently inherits
``DEFAULT_TASK_TIMEOUT``. For a task whose own internal waits exceed that
budget the worker kills it mid-flight, and nothing in the code, the logs, or
the type checker says so — the only symptom is a task that always dies at the
same elapsed time.

These tests are the fix for that class of bug; the individual constants are
the symptom. They deliberately assert *both* directions of the correspondence,
because a task-type rename produces a dead key and a missing key at the same
time, and checking only for missing keys catches half of it.
"""

import pytest

from robosystems.worker.constants import DEFAULT_TASK_TIMEOUT, TASK_TIMEOUTS


@pytest.fixture(scope="module")
def registered_task_types() -> set[str]:
  """Task types registered by importing the worker package.

  Importing ``robosystems.worker`` runs the side-effect imports that trigger
  every ``@register_task`` decorator, which is the same thing the worker
  process does at startup.
  """
  import robosystems.worker  # noqa: F401  (imported for registration side effects)
  from robosystems.worker.tasks import TASK_REGISTRY

  return set(TASK_REGISTRY)


@pytest.mark.unit
def test_every_registered_task_has_an_explicit_timeout(
  registered_task_types: set[str],
) -> None:
  missing = registered_task_types - set(TASK_TIMEOUTS)
  assert not missing, (
    f"Task types registered with no TASK_TIMEOUTS entry: {sorted(missing)}. "
    f"They silently run on DEFAULT_TASK_TIMEOUT ({DEFAULT_TASK_TIMEOUT}s). Add an "
    "entry in robosystems/worker/constants.py sized to the task's own internal "
    "waits, not to how long it usually takes."
  )


@pytest.mark.unit
def test_every_timeout_entry_names_a_registered_task(
  registered_task_types: set[str],
) -> None:
  orphaned = set(TASK_TIMEOUTS) - registered_task_types
  assert not orphaned, (
    f"TASK_TIMEOUTS entries with no registered task type: {sorted(orphaned)}. "
    "Either the task type was renamed (in which case a registered task is now "
    "silently on the default — fix both sides) or the task is gone and the key "
    "should be deleted. A task loaded behind a feature flag must be imported by "
    "the registered_task_types fixture for this assertion to stay meaningful."
  )


@pytest.mark.unit
def test_timeouts_are_positive_ints() -> None:
  for task_type, timeout in TASK_TIMEOUTS.items():
    assert isinstance(timeout, int), f"{task_type} timeout must be an int"
    assert timeout > 0, f"{task_type} timeout must be positive"


@pytest.mark.unit
def test_tier_upgrade_budget_covers_its_internal_waits() -> None:
  """The tier migration awaits drain + reattach before it verifies anything.

  Pinned against the task's own constants so that raising either wait without
  raising the worker budget fails here rather than in production, where the
  symptom is a graph left with an unattached volume.
  """
  from robosystems.operations.graph.tasks.graph_tier_upgrade import (
    DRAIN_TIMEOUT_SECONDS,
    REATTACH_TIMEOUT_SECONDS,
  )

  internal_waits = DRAIN_TIMEOUT_SECONDS + REATTACH_TIMEOUT_SECONDS
  assert TASK_TIMEOUTS["graph_tier_upgrade"] > internal_waits, (
    f"graph_tier_upgrade budget ({TASK_TIMEOUTS['graph_tier_upgrade']}s) must exceed "
    f"its own drain + reattach waits ({internal_waits}s) with headroom for the "
    "snapshot, ASG capacity change, and health verification that follow them."
  )
