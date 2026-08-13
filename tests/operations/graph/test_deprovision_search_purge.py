# pyright: reportPrivateUsage=false
"""Regression tests for the OpenSearch purge on graph deprovision.

Locks in the fix that a deprovisioned tenant's documents are removed from the
shared search index (an application-level graph_id filter is the only boundary),
that a search-disabled deployment is a clean no-op, and that an index failure is
best-effort — recorded, never blocking teardown.
"""

from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.graph.deprovision_service import (
  DeprovisionResult,
  GraphDeprovisionService,
)

GID = "kg" + "0" * 16
_CLIENT = "robosystems.operations.search.client.OpenSearchClient"
_FLAG = "robosystems.config.env.EnvConfig.SEMANTIC_SEARCH_ENABLED"


@pytest.mark.unit
class TestDeprovisionSearchPurge:
  def test_purge_deletes_and_records(self) -> None:
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_FLAG, True), patch(_CLIENT) as cls:
      client = MagicMock()
      cls.return_value = client
      svc._purge_search_index(GID, result)
    client.delete_by_graph_id.assert_called_once_with(GID)
    assert result.search_purged is True
    assert result.errors == []

  def test_purge_skipped_when_search_disabled(self) -> None:
    # No cluster call, no error, no partial-status downgrade.
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_FLAG, False), patch(_CLIENT) as cls:
      svc._purge_search_index(GID, result)
    cls.assert_not_called()
    assert result.search_purged is False
    assert result.errors == []

  def test_purge_failure_is_best_effort(self) -> None:
    svc = GraphDeprovisionService("test")
    result = DeprovisionResult(graph_id=GID, status="success")
    with patch(_FLAG, True), patch(_CLIENT) as cls:
      client = MagicMock()
      client.delete_by_graph_id.side_effect = RuntimeError("boom")
      cls.return_value = client
      svc._purge_search_index(GID, result)  # must not raise
    assert result.search_purged is False
    assert any("Search index purge failed" in e for e in result.errors)
