"""Rollback of a graph whose creation pipeline failed part-way through.

Which resources exist when `create()` fails depends on where it failed, and the
split is `_persist_metadata`, which **commits**. Before it, only the allocation
exists. After it, so do a `Graph` row, its `GraphUser` / `GraphSchema` / staging
rows, and possibly an extensions tenant schema — none of which the rollback used
to touch, which is how a failed creation left a row its owner could not delete.

The cancellation case is separate and worse: the worker runs the handler under
`asyncio.wait_for`, and an expired budget raises `CancelledError` *inside* the
pipeline. That is a `BaseException` in 3.13, so an `except Exception` rollback
is skipped entirely and the allocation is stranded on a writer that holds one
graph.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.graph_tier import GraphTier
from robosystems.middleware.graph.allocation_manager import (
  DatabaseLocation,
  DatabaseStatus,
)
from robosystems.models.core import Graph, Org, OrgType, OrgUser, User
from robosystems.models.core.graph import GraphStatus
from robosystems.operations.graph.graph_creation_service import (
  GraphCreationConfig,
  GraphCreationService,
)

SERVICE_MODULE = "robosystems.operations.graph.graph_creation_service"
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
def persisted_graph(db_session, test_org):
  """The row `_persist_metadata` commits before the later steps can fail."""
  uid = str(uuid.uuid4())[:8]
  return Graph.create(
    graph_id=f"kg_{uid}",
    graph_name="Test Graph",
    graph_type="entity",
    org_id=test_org.id,
    session=db_session,
    graph_tier=GraphTier.LADYBUG_STANDARD,
  )


@contextmanager
def _yield(session):
  yield session


def _location() -> DatabaseLocation:
  return DatabaseLocation(
    graph_id="kg_test",
    instance_id="i-test",
    private_ip="10.0.0.1",
    availability_zone="us-east-1a",
    created_at=datetime(2026, 1, 1, tzinfo=UTC),
    status=DatabaseStatus.ACTIVE,
  )


def _config() -> GraphCreationConfig:
  return GraphCreationConfig(
    user_id="u1",
    tier="ladybug-standard",
    graph_name="Test Graph",
    graph_type="generic",
  )


@contextmanager
def _teardown_infra(db_session):
  """Real deprovision service, mocked infrastructure, test session."""
  with (
    patch("robosystems.db.platform.platform_session", lambda: _yield(db_session)),
    patch(
      "robosystems.graph_api.client.factory.get_graph_client", new_callable=AsyncMock
    ),
    patch(
      "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
    ) as alloc_cls,
    patch(f"{DEPROVISION_MODULE}.get_deprovisioning_config"),
  ):
    alloc_cls.return_value = AsyncMock()
    yield


class TestCleanupAfterPersist:
  """A failure after `_persist_metadata` must not leave the row behind."""

  @pytest.mark.asyncio
  async def test_cleanup_deprovisions_a_persisted_row(
    self, db_session, persisted_graph
  ):
    service = GraphCreationService()

    with _teardown_infra(db_session):
      await service._cleanup(persisted_graph.graph_id, _location(), None)

    db_session.refresh(persisted_graph)
    assert persisted_graph.status == GraphStatus.DEPROVISIONED.value
    assert persisted_graph.deleted_at is not None

  @pytest.mark.asyncio
  async def test_cleanup_closes_the_client_before_teardown(
    self, db_session, persisted_graph
  ):
    """Teardown opens its own client, so the pipeline's must be released first."""
    service = GraphCreationService()
    client = AsyncMock()

    with _teardown_infra(db_session):
      await service._cleanup(persisted_graph.graph_id, _location(), client)

    client.close.assert_awaited_once()


class TestCleanupBeforePersist:
  """A failure before the row is committed still has an allocation to release."""

  @pytest.mark.asyncio
  async def test_cleanup_deallocates_when_no_row_exists(self, db_session):
    service = GraphCreationService()

    with (
      patch("robosystems.db.platform.platform_session", lambda: _yield(db_session)),
      patch(f"{SERVICE_MODULE}.LadybugAllocationManager") as alloc_cls,
    ):
      manager = AsyncMock()
      alloc_cls.return_value = manager

      await service._cleanup("kg_neverpersisted", _location(), None)

    manager.deallocate_database.assert_awaited_once_with("kg_neverpersisted")

  @pytest.mark.asyncio
  async def test_no_row_and_no_allocation_is_not_an_error(self, db_session):
    """`_validate_org` can fail before anything is allocated at all."""
    service = GraphCreationService()

    with (
      patch("robosystems.db.platform.platform_session", lambda: _yield(db_session)),
      patch(f"{SERVICE_MODULE}.LadybugAllocationManager") as alloc_cls,
    ):
      manager = AsyncMock()
      alloc_cls.return_value = manager

      await service._cleanup("kg_nothing", None, None)

    manager.deallocate_database.assert_not_awaited()


class TestCancellation:
  """The worker's timeout arrives as a BaseException, not an Exception."""

  @pytest.mark.asyncio
  async def test_cancellation_runs_cleanup_and_propagates(self):
    """The D2 regression: rollback must run, and the cancel must not be eaten.

    Re-raising `CancelledError` unchanged is what lets `wait_for` convert it to
    `TimeoutError`, which is what the consumer's timeout branch records the
    failed operation from. Translating it here would silently reclassify every
    timeout as a generic failure.
    """
    service = GraphCreationService()
    cleanup = AsyncMock()

    with (
      patch.object(service, "_validate_org", return_value="org_test"),
      patch.object(service, "_generate_graph_id", return_value="kg_cancelled"),
      patch.object(
        service, "_allocate", new_callable=AsyncMock, return_value=_location()
      ),
      patch.object(
        service,
        "_create_database",
        new_callable=AsyncMock,
        side_effect=asyncio.CancelledError(),
      ),
      patch.object(service, "_cleanup", cleanup),
    ):
      with pytest.raises(asyncio.CancelledError):
        await service.create(_config())

    cleanup.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_ordinary_failure_still_runs_cleanup(self):
    """Widening to BaseException must not lose the Exception case."""
    service = GraphCreationService()
    cleanup = AsyncMock()

    with (
      patch.object(service, "_validate_org", return_value="org_test"),
      patch.object(service, "_generate_graph_id", return_value="kg_failed"),
      patch.object(
        service, "_allocate", new_callable=AsyncMock, return_value=_location()
      ),
      patch.object(
        service,
        "_create_database",
        new_callable=AsyncMock,
        side_effect=RuntimeError("graph api down"),
      ),
      patch.object(service, "_cleanup", cleanup),
    ):
      with pytest.raises(RuntimeError, match="graph api down"):
        await service.create(_config())

    cleanup.assert_awaited_once()

  @pytest.mark.asyncio
  async def test_rollback_is_bounded(self):
    """An unbounded rollback would hold the worker for the whole teardown.

    `wait_for` cancels the handler exactly once, so after that cancellation is
    caught nothing interrupts the rollback a second time — the budget is the
    only thing that ends it.
    """
    service = GraphCreationService()

    async def _never_finishes(*_args, **_kwargs):
      await asyncio.sleep(3600)

    with (
      patch.object(service, "_cleanup", _never_finishes),
      patch(f"{SERVICE_MODULE}.CLEANUP_TIMEOUT_SECONDS", 0.01),
    ):
      # Returns rather than hanging or raising: the caller is already failing.
      await service._cleanup_within_budget("kg_slow", _location(), None)

  @pytest.mark.asyncio
  async def test_rollback_failure_does_not_replace_the_original_error(self):
    service = GraphCreationService()

    with (
      patch.object(service, "_validate_org", return_value="org_test"),
      patch.object(service, "_generate_graph_id", return_value="kg_failed"),
      patch.object(
        service, "_allocate", new_callable=AsyncMock, return_value=_location()
      ),
      patch.object(
        service,
        "_create_database",
        new_callable=AsyncMock,
        side_effect=RuntimeError("graph api down"),
      ),
      patch.object(
        service,
        "_cleanup",
        AsyncMock(side_effect=RuntimeError("rollback exploded")),
      ),
    ):
      with pytest.raises(RuntimeError, match="graph api down"):
        await service.create(_config())


class TestCleanupIsBestEffort:
  """A rollback that cannot reach its dependencies must not raise."""

  @pytest.mark.asyncio
  async def test_teardown_failure_is_swallowed(self, db_session, persisted_graph):
    service = GraphCreationService()

    with (
      patch("robosystems.db.platform.platform_session", lambda: _yield(db_session)),
      patch(
        f"{DEPROVISION_MODULE}.GraphDeprovisionService",
        MagicMock(side_effect=RuntimeError("teardown exploded")),
      ),
    ):
      await service._cleanup(persisted_graph.graph_id, _location(), None)


class TestTenantSchemaProvisioning:
  """An extensions-flagged graph must get its tenant schema at creation
  whether or not an entity is created up front. `create_entity=false` used to
  skip provisioning entirely, leaving a graph that passes the extension gate
  with no schema for its sessions to land in.
  """

  def _pipeline(self, service, provision, provision_entity):
    return (
      patch.object(service, "_validate_org", return_value="org_test"),
      patch.object(service, "_generate_graph_id", return_value="kg00000000000000ee"),
      patch.object(
        service, "_allocate", new_callable=AsyncMock, return_value=_location()
      ),
      patch.object(
        service,
        "_create_database",
        new_callable=AsyncMock,
        return_value=(AsyncMock(), None),
      ),
      patch.object(
        service,
        "_install_schema",
        new_callable=AsyncMock,
        return_value=("", {}),
      ),
      patch.object(service, "_persist_metadata"),
      patch.object(service, "_create_credits"),
      patch.object(service, "_provision_entity", provision_entity),
      patch("robosystems.db.extensions.provision_tenant_schema", provision),
    )

  @pytest.mark.asyncio
  async def test_extensions_graph_without_entity_still_gets_a_schema(self):
    service = GraphCreationService()
    provision = MagicMock()
    provision_entity = AsyncMock()
    config = GraphCreationConfig(
      user_id="u1",
      tier="ladybug-standard",
      graph_name="Empty ledger",
      graph_type="entity",
      schema_extensions=["roboledger"],
      create_entity=False,
    )
    with ExitStack() as stack:
      for p in self._pipeline(service, provision, provision_entity):
        stack.enter_context(p)
      await service.create(config)

    provision.assert_called_once_with("kg00000000000000ee")
    provision_entity.assert_not_awaited()

  @pytest.mark.asyncio
  async def test_entity_graph_provisions_then_creates_the_entity(self):
    service = GraphCreationService()
    provision = MagicMock()
    provision_entity = AsyncMock(return_value={"id": "entity_x"})
    config = GraphCreationConfig(
      user_id="u1",
      tier="ladybug-standard",
      graph_name="Ledger",
      graph_type="entity",
      schema_extensions=["roboledger"],
      entity_data={"name": "Acme"},
    )
    with ExitStack() as stack:
      for p in self._pipeline(service, provision, provision_entity):
        stack.enter_context(p)
      result = await service.create(config)

    provision.assert_called_once_with("kg00000000000000ee")
    provision_entity.assert_awaited_once()
    assert result.entity == {"id": "entity_x"}

  @pytest.mark.asyncio
  async def test_generic_graph_gets_no_tenant_schema(self):
    service = GraphCreationService()
    provision = MagicMock()
    with ExitStack() as stack:
      for p in self._pipeline(service, provision, AsyncMock()):
        stack.enter_context(p)
      await service.create(_config())

    provision.assert_not_called()
