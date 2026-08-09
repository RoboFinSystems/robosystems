"""Tests for the nightly graph backup schedule.

The scope rules are the whole point of this schedule, so they are what these
pin: which graphs are in, which are deliberately out, and that a re-evaluation
within a day cannot take a second backup.
"""

from unittest.mock import MagicMock, patch

import pytest
from dagster import build_schedule_context

from robosystems.dagster.jobs.backup_schedule import (
  _graphs_to_back_up,
  _run_config_for,
  nightly_graph_backup_schedule,
)


def _session_returning(graph_ids: list[str]) -> MagicMock:
  session = MagicMock()
  session.query.return_value.filter.return_value.all.return_value = [
    (graph_id,) for graph_id in graph_ids
  ]
  return session


@pytest.mark.unit
class TestScope:
  def test_shared_repositories_are_excluded(self):
    """Re-ingestible from a public source, and large.

    Backing up a multi-hundred-GB corpus nightly is the expensive mistake this
    scope exists to avoid, and it already has a volume snapshot from the
    instance lifecycle.
    """
    session = _session_returning(["kg1a2b3c4d5e6f7a8b9c", "sec", "sec_historical"])

    with patch(
      "robosystems.dagster.jobs.backup_schedule.is_shared_repository_or_subgraph",
      side_effect=lambda g: g.startswith("sec"),
    ):
      assert _graphs_to_back_up(session) == ["kg1a2b3c4d5e6f7a8b9c"]

  def test_subgraphs_are_included(self):
    """Subgraphs are one of the two classes with no upstream at all.

    They are also invisible to the DynamoDB volume registry, whose `databases`
    list records only parent ids — which is why enumeration reads the platform
    graphs table instead.
    """
    session = _session_returning(
      ["kg1a2b3c4d5e6f7a8b9c", "kg1a2b3c4d5e6f7a8b9c_entities"]
    )

    with patch(
      "robosystems.dagster.jobs.backup_schedule.is_shared_repository_or_subgraph",
      return_value=False,
    ):
      assert _graphs_to_back_up(session) == [
        "kg1a2b3c4d5e6f7a8b9c",
        "kg1a2b3c4d5e6f7a8b9c_entities",
      ]

  def test_query_filters_on_type_and_status(self):
    """A graph still provisioning has nothing to back up."""
    session = _session_returning([])

    with patch(
      "robosystems.dagster.jobs.backup_schedule.is_shared_repository_or_subgraph",
      return_value=False,
    ):
      _graphs_to_back_up(session)

    assert session.query.return_value.filter.called


@pytest.mark.unit
class TestRunConfig:
  def test_marks_the_run_as_scheduled(self):
    """Keeps the run out of the customer's daily allowance.

    The entry tier permits two backups a day; charging one to the platform's
    own job would take half of it for something nobody asked for.
    """
    config = _run_config_for("kg1a2b3c4d5e6f7a8b9c", 7)["ops"]["create_backup"][
      "config"
    ]

    assert config["initiated_by"] == "scheduled"
    assert config["graph_id"] == "kg1a2b3c4d5e6f7a8b9c"
    assert config["retention_days"] == 7
    assert config["backup_format"] == "full_dump"


@pytest.mark.unit
class TestFanOut:
  def _evaluate(self, graph_ids, tier="ladybug-standard", retention=7):
    graph = MagicMock()
    graph.graph_tier = tier

    context = build_schedule_context()

    with (
      patch(
        "robosystems.dagster.jobs.backup_schedule.db_session_factory",
        return_value=MagicMock(),
      ),
      patch(
        "robosystems.dagster.jobs.backup_schedule._graphs_to_back_up",
        return_value=graph_ids,
      ),
      patch("robosystems.models.core.Graph.get_by_id", return_value=graph),
      patch(
        "robosystems.config.graph_tier.GraphTierConfig.get_backup_limits",
        return_value={"backup_retention_days": retention},
      ),
    ):
      return nightly_graph_backup_schedule(context)

  def test_one_run_per_graph(self):
    """Fanned out rather than looped inside one run.

    One graph failing must not take the rest of the fleet's backups with it.
    """
    requests = self._evaluate(["kg_a", "kg_b", "kg_c"])

    assert len(requests) == 3
    assert {r.tags["graph_id"] for r in requests} == {"kg_a", "kg_b", "kg_c"}

  def test_run_key_is_stable_within_a_day(self):
    """A re-evaluation must dedupe rather than take a second backup.

    Without this the schedule could eat into the tier's daily ceiling on its
    own, and the customer would find their allowance already spent.
    """
    first = self._evaluate(["kg_a"])
    second = self._evaluate(["kg_a"])

    assert first[0].run_key == second[0].run_key
    assert "kg_a" in first[0].run_key

  def test_retention_follows_the_graph_tier(self):
    requests = self._evaluate(["kg_a"], tier="ladybug-xlarge", retention=90)

    config = requests[0].run_config["ops"]["create_backup"]["config"]
    assert config["retention_days"] == 90

  def test_no_graphs_produces_no_runs(self):
    assert self._evaluate([]) == []


@pytest.mark.unit
class TestKillSwitch:
  def test_disabled_flag_stops_the_nightly_run(self):
    """The kill switch has to cover this path too.

    Both manual paths check it. A flag honoured on two of the three paths that
    create backups is not a kill switch — and this is the one nobody is
    watching when they flip it during an incident.
    """
    from robosystems.config import env as env_module

    with (
      patch.object(env_module, "BACKUP_CREATION_ENABLED", False),
      patch(
        "robosystems.dagster.jobs.backup_schedule._graphs_to_back_up",
        return_value=["kg_a", "kg_b"],
      ) as enumerate_graphs,
    ):
      assert nightly_graph_backup_schedule(build_schedule_context()) == []

    # Short-circuits before touching the database at all.
    enumerate_graphs.assert_not_called()
