"""A tier upgrade moves the graph's volume: the staleness sweep and the
materialize op hold off while the graph's subscription is ``upgrading``,
and the graph stays stale for the sweep after it."""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from dagster import DagsterInstance, build_op_context, build_sensor_context

from robosystems.dagster.jobs.extensions import (
  ExtensionsMaterializeConfig,
  materialize_extensions_to_graph,
)
from robosystems.dagster.sensors.materialization import (
  stale_graph_materialization_sensor,
)
from robosystems.models.core import Graph, Org, OrgType
from robosystems.models.core.billing import BillingSubscription


def _stale_graph(session, *, parent: Graph | None = None, index: int = 1) -> Graph:
  if parent is None:
    org = Org.create(
      name=f"Upgrade {uuid.uuid4().hex[:6]}", org_type=OrgType.PERSONAL, session=session
    )
    graph = Graph.create(
      graph_id=f"kg{uuid.uuid4().hex[:16]}",
      org_id=org.id,
      graph_name="Upgrade",
      graph_type="entity",
      session=session,
    )
  else:
    graph = Graph.create(
      graph_id=f"{parent.graph_id}_sub{index}",
      org_id=parent.org_id,
      graph_name="Sub",
      graph_type="entity",
      session=session,
      parent_graph_id=parent.graph_id,
      subgraph_index=index,
      subgraph_name=f"sub{index}",
      is_subgraph=True,
    )
  stale_at = datetime.now(UTC) - timedelta(days=1)
  graph.graph_stale = True
  graph.graph_stale_at = stale_at
  graph.graph_stale_since = stale_at
  graph.graph_stale_reason = "schedule_created"
  session.commit()
  return graph


def _subscription(session, graph: Graph, status: str) -> BillingSubscription:
  sub = BillingSubscription(
    org_id=graph.org_id,
    resource_type="graph",
    resource_id=graph.graph_id,
    plan_name="ladybug-standard",
    base_price_cents=9900,
    status=status,
    stripe_subscription_id=f"sub_test_{uuid.uuid4().hex[:10]}",
    subscription_metadata={},
  )
  session.add(sub)
  session.commit()
  return sub


def _selected(session) -> list[str]:
  with (
    patch(
      "robosystems.dagster.sensors.materialization.db_session_factory",
      return_value=session,
    ),
    patch(
      "robosystems.dagster.sensors.materialization._graphs_being_written",
      return_value=set(),
    ),
  ):
    runs = list(
      stale_graph_materialization_sensor(
        build_sensor_context(instance=DagsterInstance.ephemeral())
      )
    )
  selected = []
  for run in runs:
    run_config = getattr(run, "run_config", None)
    if run_config:
      selected.append(next(iter(run_config["ops"].values()))["config"]["graph_id"])
  return selected


def _run_op(session, graph_id: str) -> tuple[dict, AsyncMock]:
  materialize = AsyncMock()
  with (
    patch(
      "robosystems.operations.extensions.materialize.ExtensionsMaterializer.materialize",
      new=materialize,
    ),
    patch(
      "robosystems.middleware.graph.ingestion_limits.IngestionLimitChecker.check_instance_storage",
      new=AsyncMock(return_value={"allowed": True, "errors": []}),
    ),
    patch("robosystems.database.get_db_session", side_effect=lambda: iter([session])),
  ):
    out = materialize_extensions_to_graph(
      build_op_context(),
      MagicMock(),
      MagicMock(),
      ExtensionsMaterializeConfig(graph_id=graph_id),
    )
  return out, materialize


@pytest.mark.unit
class TestTheSweepHoldsOffDuringATierUpgrade:
  def test_an_upgrading_graph_is_not_selected_until_the_upgrade_ends(self, test_db):
    # The sensor closes the session it is handed, so rows are reached by id.
    graph_id = _stale_graph(test_db).graph_id
    sub_id = _subscription(test_db, test_db.get(Graph, graph_id), "upgrading").id

    assert graph_id not in _selected(test_db)

    test_db.get(BillingSubscription, sub_id).status = "active"
    test_db.commit()
    assert graph_id in _selected(test_db)

  def test_a_subgraph_waits_on_its_parents_upgrade(self, test_db):
    parent = _stale_graph(test_db)
    parent_id, child_id = parent.graph_id, _stale_graph(test_db, parent=parent).graph_id
    _subscription(test_db, test_db.get(Graph, parent_id), "upgrading")

    selected = _selected(test_db)

    assert parent_id not in selected
    assert child_id not in selected


@pytest.mark.unit
class TestTheOpDefersDuringATierUpgrade:
  def test_the_op_defers_and_leaves_the_graph_stale(self, test_db):
    graph = _stale_graph(test_db)
    _subscription(test_db, graph, "upgrading")

    out, materialize = _run_op(test_db, graph.graph_id)

    assert out == {"graph_id": graph.graph_id, "status": "deferred"}
    materialize.assert_not_called()
    test_db.expire_all()
    assert test_db.get(Graph, graph.graph_id).graph_stale is True

  def test_a_subgraph_defers_on_its_parents_upgrade(self, test_db):
    parent = _stale_graph(test_db)
    child = _stale_graph(test_db, parent=parent)
    _subscription(test_db, parent, "upgrading")

    out, materialize = _run_op(test_db, child.graph_id)

    assert out["status"] == "deferred"
    materialize.assert_not_called()
