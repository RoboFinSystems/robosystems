"""Tenant schema provisioning is a first-time act, and never for a torn-down graph.

``provision_tenant_schema`` is idempotent but its CHECK and trigger installs take
AccessExclusive locks on every tenant table, so a caller that runs on every sync
(the OLTP loader) goes through ``ensure_tenant_schema`` and only provisions a
schema that is missing. And teardown drops the schema before it deletes the
graph's connections, so provisioning refuses a graph the platform has already
deprovisioned rather than resurrecting its ledger.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

import robosystems.db.extensions as ext
from robosystems.db.extensions import (
  TenantDeprovisionedError,
  ensure_tenant_schema,
  provision_tenant_schema,
)
from robosystems.models.core.graph.graph import GraphStatus

pytestmark = pytest.mark.unit

GRAPH = "kg00000000000000aa"


class TestEnsureTenantSchema:
  def test_skips_provisioning_when_the_schema_exists(self):
    with (
      patch.object(ext, "tenant_schema_exists", return_value=True) as exists,
      patch.object(ext, "provision_tenant_schema") as provision,
    ):
      assert ensure_tenant_schema(GRAPH) is False
    exists.assert_called_once_with(GRAPH)
    provision.assert_not_called()

  def test_provisions_when_the_schema_is_missing(self):
    with (
      patch.object(ext, "tenant_schema_exists", return_value=False),
      patch.object(ext, "provision_tenant_schema") as provision,
    ):
      assert ensure_tenant_schema(GRAPH) is True
    provision.assert_called_once_with(GRAPH)


class TestProvisionRefusesDeprovisionedGraph:
  @staticmethod
  def _platform_with(graph):
    @contextmanager
    def _session():
      pdb = MagicMock()
      pdb.get.return_value = graph
      yield pdb

    return _session

  def test_raises_before_touching_the_extensions_db(self):
    graph = MagicMock()
    graph.status = GraphStatus.DEPROVISIONED.value
    engine = MagicMock()
    with (
      patch.object(ext, "_get_engine", return_value=engine),
      patch("robosystems.database.platform_session", self._platform_with(graph)),
    ):
      with pytest.raises(TenantDeprovisionedError) as excinfo:
        provision_tenant_schema(GRAPH)
    assert excinfo.value.graph_id == GRAPH
    engine.connect.assert_not_called()

  def test_raises_while_teardown_is_in_flight(self):
    """Teardown stamps `deleted_at` (committed) before it drops anything and
    flips the status only at the end; a sync in that window must not
    re-create the schema."""
    from datetime import UTC, datetime

    graph = MagicMock()
    graph.status = GraphStatus.ACTIVE.value
    graph.deleted_at = datetime.now(UTC)
    engine = MagicMock()
    with (
      patch.object(ext, "_get_engine", return_value=engine),
      patch("robosystems.database.platform_session", self._platform_with(graph)),
    ):
      with pytest.raises(TenantDeprovisionedError):
        provision_tenant_schema(GRAPH)
    engine.connect.assert_not_called()

  def test_missing_graph_row_still_provisions(self):
    """Scripts provision without a platform row (framework_validate); only a
    row that says *deprovisioned* refuses."""
    engine = MagicMock()
    with (
      patch.object(ext, "_get_engine", return_value=engine),
      patch("robosystems.database.platform_session", self._platform_with(None)),
      patch("robosystems.taxonomy.pins.resolve_pin", return_value="v1"),
      patch("robosystems.taxonomy.writer.copy_library_into_tenant"),
      patch.object(ext.ExtensionsBase.metadata, "create_all"),
      patch.object(ext, "_widen_library_checks"),
      patch.object(ext, "_install_library_immutability_triggers"),
    ):
      provision_tenant_schema(GRAPH)
    engine.connect.assert_called_once()


class TestEngineSessionGuards:
  def test_engine_sets_an_idle_in_transaction_timeout(self):
    """A leaked idle-in-transaction session holding FOR UPDATE blocked writers
    for as long as the connection lived; Postgres now closes it."""
    from unittest.mock import patch

    with (
      patch.object(ext, "create_engine") as create_engine,
      patch.object(ext, "get_extensions_database_url", return_value="postgresql://x"),
    ):
      ext._create_extensions_engine()
    kwargs = create_engine.call_args.kwargs
    assert (
      f"idle_in_transaction_session_timeout={ext.IDLE_IN_TRANSACTION_TIMEOUT_MS}"
      in kwargs["connect_args"]["options"]
    )
    assert "statement_timeout" not in kwargs["connect_args"]["options"]
