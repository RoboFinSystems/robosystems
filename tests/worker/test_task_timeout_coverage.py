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


def _worst_case_graph_call_seconds() -> float:
  """Worst-case wall clock for one Graph API call, from the client's own config.

  Every attempt can burn the full request timeout, and ``_execute_with_retry``
  makes ``max_retries + 1`` of them with exponential backoff between. Jitter
  adds up to 10% per delay and is ignored here — it only makes the real number
  larger, so a budget that clears this bound clears it with jitter too.
  """
  from robosystems.graph_api.client.config import GraphClientConfig

  config = GraphClientConfig()
  attempts = config.max_retries + 1
  backoff = sum(
    config.retry_delay * (config.retry_backoff**i) for i in range(attempts - 1)
  )
  return config.timeout * attempts + backoff


@pytest.mark.unit
@pytest.mark.parametrize(
  ("task_type", "graph_api_calls"),
  [
    # create_database, install_schema
    ("graph_creation", 2),
    # create_database, install_schema, fork_from_parent
    ("subgraph_creation", 3),
  ],
)
def test_creation_budgets_cover_their_graph_api_calls(
  task_type: str, graph_api_calls: int
) -> None:
  """A creation budget must exceed the retry budget of the calls it makes.

  Both of these sat at 60s — less than one call's worst case — so a slow run
  was killed mid-provision rather than allowed to finish. The failure mode is
  invisible from the code: the only symptom is a task that always dies at the
  same elapsed time, and (before the rollback was fixed) an allocation left
  behind on a one-graph-per-instance writer.

  The bound is deliberately the *floor*, not the budget: it ignores the
  extensions tenant provisioning, the taxonomy copy, and the fork's own data
  volume, all of which come on top.
  """
  floor = _worst_case_graph_call_seconds() * graph_api_calls
  assert TASK_TIMEOUTS[task_type] > floor, (
    f"{task_type} budget ({TASK_TIMEOUTS[task_type]}s) must exceed the worst-case "
    f"retry budget of its {graph_api_calls} Graph API calls ({floor:.0f}s), which is "
    "only the floor — the schema and data work it does on top is unbounded by this. "
    "Raise the budget in robosystems/worker/constants.py, or lower the client's "
    "timeout/max_retries in graph_api/client/config.py."
  )


@pytest.mark.unit
def test_creation_budgets_leave_room_for_their_own_rollback() -> None:
  """The rollback runs inside the same worker task, after the pipeline fails.

  ``GraphCreationService`` catches the timeout's ``CancelledError`` and runs
  teardown under ``CLEANUP_TIMEOUT_SECONDS``. Pinned so that shrinking the task
  budget toward the rollback budget — which would leave a rollback with no room
  to finish — fails here.
  """
  from robosystems.operations.graph.graph_creation_service import (
    CLEANUP_TIMEOUT_SECONDS,
  )

  assert TASK_TIMEOUTS["graph_creation"] > CLEANUP_TIMEOUT_SECONDS, (
    f"graph_creation budget ({TASK_TIMEOUTS['graph_creation']}s) must exceed the "
    f"rollback budget ({CLEANUP_TIMEOUT_SECONDS}s) it triggers on failure."
  )


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
