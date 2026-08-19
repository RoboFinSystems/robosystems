"""The pre-execution INFO log in execute_write must not leak the extensions DSN.

`execute_write` runs postgres_scanner statements whose leading characters carry
the libpq connstr — host, user, and password. #1213 redacted the *error* path;
this covers the pre-execution log, which previously relied only on a `[:100]`
truncation happening to cut ahead of the password.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.duckdb.manager import DuckDBTableManager
from robosystems.graph_api.models.tables import TableQueryRequest

MODULE = "robosystems.graph_api.core.duckdb.manager"


@pytest.mark.unit
def test_execute_write_info_log_redacts_the_dsn(monkeypatch):
  # The INFO log fires before the pool is used; make the pool blow up so the
  # test needs no real DuckDB — the line is already emitted by then.
  def _boom():
    raise RuntimeError("no pool in test")

  monkeypatch.setattr(f"{MODULE}.get_duckdb_pool", _boom)
  fake_logger = MagicMock()
  monkeypatch.setattr(f"{MODULE}.logger", fake_logger)

  secret_sql = (
    "ATTACH 'dbname=extensions user=postgres password=hunter2 "
    "host=rds.internal port=5432' AS ext (TYPE postgres); "
    "INSERT INTO staging SELECT * FROM postgres_scan('...', 'kg1', 'line_items')"
  )
  request = TableQueryRequest(graph_id="kg1a2b3c4d5e6f7890ab", sql=secret_sql)

  manager = DuckDBTableManager()
  with pytest.raises((RuntimeError, HTTPException)):
    manager.execute_write(request)

  info_calls = " ".join(
    str(c.args) + str(c.kwargs) for c in fake_logger.info.call_args_list
  )
  assert "Executing write" in info_calls, "pre-execution log line not emitted"
  assert "hunter2" not in info_calls
  assert "password=***" in info_calls
