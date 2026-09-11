"""Unit tests for ``sever_synced_chart`` — the graph-side stamp of a cutover."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.commands.connections import (
  SEVERABLE_SOURCES,
  sever_synced_chart,
)


def _compiled(statement) -> str:
  return str(
    statement.compile(
      dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
    )
  )


@pytest.mark.unit
class TestSeverSyncedChart:
  def test_only_quickbooks_is_severable(self) -> None:
    assert frozenset({"quickbooks"}) == SEVERABLE_SOURCES
    session = MagicMock()
    with pytest.raises(ValueError, match="cannot be severed"):
      sever_synced_chart(session, "conn_1", source="mercury")
    session.execute.assert_not_called()

  def test_stamps_only_this_connections_quickbooks_elements(self) -> None:
    session = MagicMock()
    session.execute.return_value.rowcount = 29

    stamped = sever_synced_chart(session, "conn_1")

    assert stamped == 29
    statement = session.execute.call_args.args[0]
    sql = _compiled(statement)
    assert sql.startswith("UPDATE elements SET")
    assert "source='native'" in sql
    assert "external_source=NULL" in sql
    assert "connection_id=NULL" in sql
    assert "external_id=NULL" in sql
    assert "elements.external_source = 'quickbooks'" in sql
    assert "elements.connection_id = 'conn_1'" in sql
    # The qname is deliberately kept — nothing else is touched.
    assert "qname" not in sql
    assert "taxonomy_id" not in sql

  def test_missing_rowcount_reads_as_zero(self) -> None:
    session = MagicMock()
    session.execute.return_value.rowcount = None
    assert sever_synced_chart(session, "conn_1") == 0
