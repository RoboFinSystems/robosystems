"""The compensating path for a graph that provisioned but could not be billed.

`GraphCreationTask` provisions first and creates the billing subscription second,
so a declined card arrives *after* an EC2 slot, a DynamoDB allocation, a
LadybugDB database, a `Graph` row and a credit pool already exist. Undoing all
of that is the only thing standing between a routine payment failure and a
stranded instance the owner cannot delete.

That path was dead for four months — it imported a class name that does not
exist and the `ImportError` was swallowed by the same handler that logs cleanup
failures. Nothing caught it because nothing referenced `GraphCreationTask` in
the whole suite.

These tests therefore drive the **real** `GraphDeprovisionService` against a
real `Graph` row with only the infrastructure mocked, and assert the row's
final state rather than that a mock was called. A mock named for the
compensator would have passed happily against the broken import; the row would
not.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.models.core import Graph, Org, OrgType, OrgUser, User
from robosystems.models.core.graph import GraphStatus
from robosystems.operations.graph.graph_creation_service import GraphCreationResult
from robosystems.operations.graph.tasks.graph_creation import GraphCreationTask

DEPROVISION_MODULE = "robosystems.operations.graph.deprovision_service"


@pytest.fixture
def test_user(db_session):
  uid = str(uuid.uuid4())[:8]
  user = User(
    id=f"test_user_{uid}",
    email=f"test+{uid}@example.com",
    name="Test User",
    password_hash="hash",
  )
  db_session.add(user)
  db_session.commit()
  db_session.refresh(user)
  return user


@pytest.fixture
def test_org(db_session, test_user):
  org = Org.create(name="Test Org", org_type=OrgType.PERSONAL, session=db_session)
  OrgUser.create(org_id=org.id, user_id=test_user.id, role="OWNER", session=db_session)
  return org


@pytest.fixture
def provisioned_graph(db_session, test_org):
  """A graph in the state creation leaves it: fully provisioned, unbilled."""
  uid = str(uuid.uuid4())[:8]
  return Graph.create(
    graph_id=f"kg_{uid}",
    graph_name="Test Graph",
    graph_type="entity",
    org_id=test_org.id,
    session=db_session,
    graph_tier=GraphTier.LADYBUG_STANDARD,
  )


def _task(user_id: str) -> GraphCreationTask:
  return GraphCreationTask(
    task_id="op_test",
    graph_id=None,
    user_id=user_id,
    params={"graph_name": "Test Graph", "tier": "ladybug-standard"},
    manager=AsyncMock(),
  )


@contextmanager
def _teardown_infra(db_session):
  """Let the real deprovision service run against mocked infrastructure.

  `platform_session` is redirected at the test session because the task opens
  its own; everything below it is the genuine teardown, so the assertions are
  about what teardown actually did.
  """
  with (
    patch(
      "robosystems.db.platform.platform_session",
      lambda: _yield(db_session),
    ),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      new_callable=AsyncMock,
    ),
    patch(
      "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
    ) as alloc_cls,
    patch(f"{DEPROVISION_MODULE}.get_deprovisioning_config"),
  ):
    alloc_cls.return_value = AsyncMock()
    yield


@contextmanager
def _yield(session):
  yield session


def _creation_result(graph_id: str) -> GraphCreationResult:
  return GraphCreationResult(
    graph_id=graph_id,
    org_id="org_test",
    instance_id="i-test",
    private_ip="10.0.0.1",
    graph_type="entity",
    tier="ladybug-standard",
    schema_type="extensions",
    schema_extensions=[],
  )


@pytest.mark.asyncio
async def test_billing_failure_tears_the_graph_down(
  db_session, test_user, provisioned_graph
):
  """A declined card must leave no graph behind, and must still surface."""
  task = _task(test_user.id)

  with (
    patch.object(
      GraphCreationTask,
      "_create_billing_subscription",
      side_effect=RuntimeError("card declined"),
    ),
    patch(
      "robosystems.operations.graph.graph_creation_service.GraphCreationService.create",
      new_callable=AsyncMock,
      return_value=_creation_result(provisioned_graph.graph_id),
    ),
    _teardown_infra(db_session),
  ):
    with pytest.raises(RuntimeError, match="card declined"):
      await task.execute()

  db_session.refresh(provisioned_graph)
  assert provisioned_graph.status == GraphStatus.DEPROVISIONED.value
  assert provisioned_graph.deleted_at is not None


@pytest.mark.asyncio
async def test_billing_error_survives_a_failing_teardown(
  db_session, test_user, provisioned_graph
):
  """Cleanup failure must not replace the error the caller needs to see.

  This is the behavior that hid the dead import: it is correct to swallow a
  cleanup failure, which is exactly why the swallowing cannot be the only thing
  standing behind this path.
  """
  task = _task(test_user.id)

  with (
    patch.object(
      GraphCreationTask,
      "_create_billing_subscription",
      side_effect=RuntimeError("card declined"),
    ),
    patch(
      "robosystems.operations.graph.graph_creation_service.GraphCreationService.create",
      new_callable=AsyncMock,
      return_value=_creation_result(provisioned_graph.graph_id),
    ),
    patch(
      f"{DEPROVISION_MODULE}.GraphDeprovisionService",
      side_effect=RuntimeError("teardown exploded"),
    ),
  ):
    with pytest.raises(RuntimeError, match="card declined"):
      await task.execute()


@pytest.mark.asyncio
async def test_compensator_is_constructed_with_an_environment(
  db_session, test_user, provisioned_graph
):
  """Pins the constructor contract the dead import got wrong twice over.

  `DeprovisionService` never existed, and `GraphDeprovisionService` takes a
  required `environment` — so correcting the name alone would still have raised
  `TypeError` into the same swallowing handler.
  """
  task = _task(test_user.id)

  with (
    patch.object(
      GraphCreationTask,
      "_create_billing_subscription",
      side_effect=RuntimeError("card declined"),
    ),
    patch(
      "robosystems.operations.graph.graph_creation_service.GraphCreationService.create",
      new_callable=AsyncMock,
      return_value=_creation_result(provisioned_graph.graph_id),
    ),
    patch(
      "robosystems.db.platform.platform_session",
      lambda: _yield(db_session),
    ),
    patch(f"{DEPROVISION_MODULE}.GraphDeprovisionService") as service_cls,
  ):
    service_cls.return_value = MagicMock(
      deprovision_graph=AsyncMock(return_value=MagicMock(status="success"))
    )

    with pytest.raises(RuntimeError, match="card declined"):
      await task.execute()

  assert service_cls.call_args.kwargs["environment"]
  call = service_cls.return_value.deprovision_graph.call_args.kwargs
  assert call["graph_id"] == provisioned_graph.graph_id
  assert call["create_backup"] is False
  assert call["skip_backup_check"] is True
