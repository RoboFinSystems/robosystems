"""Orchestration tests for the taxonomy library re-sync entrypoints.

These cover the pure control flow — the bypass GUC is set inside the session
before the DO UPDATE fan-out runs, and a per-schema failure does not abort the
whole sweep. The SQL itself and the immutability-trigger bypass are PL/pgSQL
behavior that requires a live Postgres and is exercised separately.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from robosystems.operations.taxonomy_block import resync

_MODULE = "robosystems.operations.taxonomy_block.resync"


def _session_cm(session: MagicMock):
  @contextmanager
  def _cm(graph_id: str):
    yield session

  return _cm


class TestResyncTenant:
  def test_sets_bypass_guc_before_fan_out(self) -> None:
    session = MagicMock()
    conn = MagicMock()
    session.connection.return_value = conn
    sentinel = MagicMock(total=42)

    with (
      patch(f"{_MODULE}.extensions_session", _session_cm(session)),
      patch(f"{_MODULE}.resync_library_into_tenant", return_value=sentinel) as m_resync,
    ):
      result = resync.resync_tenant("kg0123456789abcdef")

    assert result is sentinel
    # The bypass GUC must have been issued on the session.
    guc_calls = [
      str(call.args[0]) for call in session.execute.call_args_list if call.args
    ]
    assert any("robosystems.library_resync" in sql for sql in guc_calls)
    # The fan-out runs on the session's connection (same txn as the GUC).
    m_resync.assert_called_once_with(conn, "kg0123456789abcdef", None)

  def test_passes_pin_through(self) -> None:
    session = MagicMock()
    pin = {"rs-gaap": "v1"}

    with (
      patch(f"{_MODULE}.extensions_session", _session_cm(session)),
      patch(
        f"{_MODULE}.resync_library_into_tenant", return_value=MagicMock(total=0)
      ) as m_resync,
    ):
      resync.resync_tenant("kg0123456789abcdef", pin)

    assert m_resync.call_args.args[2] == pin


class TestResyncAllTenants:
  def test_one_schema_failure_does_not_abort_sweep(self) -> None:
    schemas = ["kg1111111111111111", "kg2222222222222222", "kg3333333333333333"]
    good = MagicMock(total=10)

    def _fake_resync_tenant(graph_id: str, pin=None):
      if graph_id == "kg2222222222222222":
        raise RuntimeError("boom")
      return good

    with (
      patch(f"{_MODULE}.list_tenant_schemas", return_value=schemas),
      patch(f"{_MODULE}.resync_tenant", side_effect=_fake_resync_tenant),
    ):
      results = resync.resync_all_tenants()

    # The failing schema is skipped; the other two still re-sync.
    assert set(results) == {"kg1111111111111111", "kg3333333333333333"}
    assert results["kg1111111111111111"] is good

  def test_empty_when_no_tenants(self) -> None:
    with patch(f"{_MODULE}.list_tenant_schemas", return_value=[]):
      assert resync.resync_all_tenants() == {}
