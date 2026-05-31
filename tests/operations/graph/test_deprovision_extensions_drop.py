# pyright: reportPrivateUsage=false
"""Regression tests for extensions OLTP schema drop on graph deprovision.

Locks in the fix that a deprovisioned tenant's extensions schema (and its
financial data) is actually dropped, and that the step is best-effort (a
failure is recorded but never blocks teardown).
"""

from unittest.mock import patch

import pytest

from robosystems.operations.graph.deprovision_service import (
  DeprovisionResult,
  GraphDeprovisionService,
)

GID = "kg" + "0" * 16
_DROP = "robosystems.db.extensions.drop_tenant_schema"


@pytest.mark.unit
class TestDeprovisionExtensionsSchemaDrop:
  def test_drop_records_result(self) -> None:
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_DROP, return_value=True) as m:
      svc._drop_extensions_schema(GID, result)
    m.assert_called_once_with(GID)
    assert result.extensions_schema_dropped is True
    assert result.errors == []

  def test_drop_skipped_returns_false(self) -> None:
    # drop_tenant_schema returns False for subgraphs / extensions-disabled.
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_DROP, return_value=False):
      svc._drop_extensions_schema(GID, result)
    assert result.extensions_schema_dropped is False
    assert result.errors == []

  def test_drop_failure_is_best_effort(self) -> None:
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_DROP, side_effect=RuntimeError("boom")):
      svc._drop_extensions_schema(GID, result)  # must not raise
    assert result.extensions_schema_dropped is False
    assert any("Extensions schema drop failed" in e for e in result.errors)
