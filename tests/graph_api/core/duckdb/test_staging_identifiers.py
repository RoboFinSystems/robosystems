"""Column names probed from an uploaded file are tenant-controlled and are
interpolated into staging DDL as identifiers. Two layers hold: the validator
refuses anything outside the identifier class before SQL is built, and every
interpolation site quotes through ``quote_identifier`` so a name that somehow
got past the validator still cannot terminate the identifier it sits in.

These tests drive real DuckDB. A test that only inspected the generated
string would pass against a broken escape.
"""

from __future__ import annotations

from contextlib import contextmanager

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi import HTTPException

from robosystems.graph_api.core.duckdb.manager import (
  DuckDBTableManager,
  quote_identifier,
  validate_column_names,
)
from robosystems.graph_api.models.tables import TableCreateRequest

# Against the pre-fix ``create_table`` this name stages a table with columns
# ``identifier, benign, injected`` and the subquery's result in the row. It
# references ``identifier`` so the binder is satisfied, and ends in ``--`` so
# the second interpolation of the name (every column appears twice in the
# template) is swallowed as a comment and the statement parses.
HOSTILE = 'identifier") AS "benign", (SELECT 42) AS "injected" --'


class _OneConnectionPool:
  """Stands in for ``get_duckdb_pool()``: hands out one real connection."""

  def __init__(self, conn: duckdb.DuckDBPyConnection):
    self.conn = conn

  @contextmanager
  def get_connection(self, graph_id: str):
    yield self.conn


def _write_parquet(path, columns: dict[str, list]) -> str:
  pq.write_table(pa.table(columns), str(path))
  return str(path)


def _request(table_name: str, files: list[str], **kw) -> TableCreateRequest:
  # ``model_construct`` skips the ``s3://`` field validator so a local file
  # can stand in for the uploaded object.
  return TableCreateRequest.model_construct(
    graph_id="kg0123456789abcdef",
    table_name=table_name,
    s3_pattern=files,
    file_id_map=kw.pop("file_id_map", None),
    null_columns=kw.pop("null_columns", None),
    deduplicate=kw.pop("deduplicate", True),
    timeout_seconds=1800,
  )


@pytest.fixture
def conn():
  c = duckdb.connect(":memory:")
  yield c
  c.close()


@pytest.fixture
def manager(conn, monkeypatch):
  monkeypatch.setattr(
    "robosystems.graph_api.core.duckdb.manager.get_duckdb_pool",
    lambda: _OneConnectionPool(conn),
  )
  return DuckDBTableManager()


@pytest.mark.unit
class TestValidateColumnNames:
  def test_accepts_the_identifier_class(self):
    validate_column_names(
      ["identifier", "from", "to", "_private", "camelCase", "UPPER", "a1_b2"],
      source="t",
    )

  @pytest.mark.parametrize(
    "name",
    [
      HOSTILE,
      'a"b',
      "has space",
      "has-hyphen",
      "has.dot",
      "1leading_digit",
      "",
      "semi;colon",
      "new\nline",
      "x" * 129,
    ],
  )
  def test_rejects_everything_outside_it(self, name):
    with pytest.raises(HTTPException) as e:
      validate_column_names(["identifier", name], source="file f.parquet")
    assert e.value.status_code == 400
    assert "file f.parquet" in e.value.detail
    assert repr(name)[:40] in e.value.detail

  def test_rejects_non_strings(self):
    with pytest.raises(HTTPException) as e:
      validate_column_names([None], source="t")  # type: ignore[list-item]
    assert e.value.status_code == 400


@pytest.mark.unit
class TestQuoteIdentifier:
  def test_wraps_and_doubles_embedded_quotes(self):
    assert quote_identifier("plain") == '"plain"'
    assert quote_identifier('a"b') == '"a""b"'
    assert quote_identifier(HOSTILE) == '"' + HOSTILE.replace('"', '""') + '"'

  def test_round_trips_through_duckdb(self, conn):
    conn.execute(f"CREATE TABLE t ({quote_identifier(HOSTILE)} INTEGER)")
    (name,) = [row[0] for row in conn.execute("DESCRIBE t").fetchall()]
    assert name == HOSTILE


@pytest.mark.unit
class TestCreateTableRefusesHostileColumns:
  def test_benign_file_stages(self, manager, conn, tmp_path):
    """Positive control: the harness really runs the staging SQL."""
    f = _write_parquet(
      tmp_path / "ok.parquet",
      {"identifier": ["a", "a", "b"], "amount": [1, 1, 2]},
    )
    resp = manager.create_table(_request("Thing", [f]))
    assert resp.status == "success"
    assert resp.row_count == 2
    assert conn.execute('SELECT count(*) FROM "Thing"').fetchone()[0] == 2

  def test_hostile_column_is_refused_before_any_sql_runs(self, manager, conn, tmp_path):
    f = _write_parquet(tmp_path / "bad.parquet", {"identifier": ["a"], HOSTILE: [7]})
    with pytest.raises(HTTPException) as e:
      manager.create_table(_request("Thing", [f]))
    assert e.value.status_code == 400
    assert "Invalid column name" in e.value.detail
    assert conn.execute("SHOW TABLES").fetchall() == []

  def test_hostile_column_is_refused_on_the_file_id_path(self, manager, conn, tmp_path):
    f = _write_parquet(tmp_path / "bad.parquet", {"identifier": ["a"], HOSTILE: [7]})
    with pytest.raises(HTTPException) as e:
      manager.create_table(_request("Thing", [f], file_id_map={f: "file_1"}))
    assert e.value.status_code == 400
    assert conn.execute("SHOW TABLES").fetchall() == []

  def test_the_400_is_not_rewrapped_as_a_500(self, manager, tmp_path):
    f = _write_parquet(tmp_path / "bad.parquet", {"identifier": ["a"], "a b": [1]})
    with pytest.raises(HTTPException) as e:
      manager.create_table(_request("Thing", [f]))
    assert e.value.status_code == 400


@pytest.mark.unit
class TestInsertIntoTableRefusesHostileColumns:
  @pytest.fixture
  def staged(self, manager, conn, tmp_path):
    f = _write_parquet(tmp_path / "seed.parquet", {"identifier": ["a"], "amount": [1]})
    manager.create_table(_request("Thing", [f]))
    return conn

  @pytest.mark.parametrize("deduplicate", [True, False])
  def test_hostile_new_column_is_refused(self, manager, staged, tmp_path, deduplicate):
    """Schema evolution would ALTER TABLE ... ADD COLUMN with the probed name."""
    f = _write_parquet(
      tmp_path / "more.parquet",
      {"identifier": ["b"], "amount": [2], HOSTILE: [3]},
    )
    with pytest.raises(HTTPException) as e:
      manager.insert_into_table(_request("Thing", [f], deduplicate=deduplicate))
    assert e.value.status_code == 400
    cols = [r[0] for r in staged.execute('DESCRIBE "Thing"').fetchall()]
    assert cols == ["identifier", "amount"]
    assert staged.execute('SELECT count(*) FROM "Thing"').fetchone()[0] == 1

  @pytest.mark.parametrize("deduplicate", [True, False])
  def test_benign_append_still_works(self, manager, staged, tmp_path, deduplicate):
    f = _write_parquet(
      tmp_path / "more.parquet",
      {"identifier": ["b"], "amount": [2], "added_later": ["x"]},
    )
    resp = manager.insert_into_table(_request("Thing", [f], deduplicate=deduplicate))
    assert resp.status == "success"
    assert resp.row_count == 1
    cols = [r[0] for r in staged.execute('DESCRIBE "Thing"').fetchall()]
    assert cols == ["identifier", "amount", "added_later"]


@pytest.mark.unit
class TestQuotingHoldsWithoutTheValidator:
  """Defense in depth: drive the SQL builders directly with hostile names,
  bypassing the validator, and check DuckDB produces exactly those column
  names — never an ``injected`` one."""

  def _parquet(self, tmp_path, extra: dict[str, list]) -> str:
    return _write_parquet(tmp_path / "h.parquet", {"identifier": ["a", "a"], **extra})

  def test_build_table_sql(self, conn, tmp_path):
    f = self._parquet(tmp_path, {HOSTILE: [1, 2]})
    sql = DuckDBTableManager()._build_table_sql(
      '"T"',
      has_identifier=True,
      has_from_to=False,
      use_list=False,
      column_names=["identifier", HOSTILE],
    )
    conn.execute(sql, [f])
    cols = [r[0] for r in conn.execute('DESCRIBE "T"').fetchall()]
    assert cols == ["identifier", HOSTILE]
    assert "injected" not in cols and "benign" not in cols
    assert conn.execute('SELECT count(*) FROM "T"').fetchone()[0] == 1

  def test_build_table_sql_with_null_column(self, conn, tmp_path):
    f = self._parquet(tmp_path, {HOSTILE: [1, 2]})
    sql = DuckDBTableManager()._build_table_sql(
      '"T"',
      has_identifier=True,
      has_from_to=False,
      use_list=False,
      column_names=["identifier", HOSTILE],
      null_columns={HOSTILE},
    )
    conn.execute(sql, [f])
    cols = [r[0] for r in conn.execute('DESCRIBE "T"').fetchall()]
    assert cols == ["identifier", HOSTILE]

  def test_build_table_sql_with_file_id(self, conn, tmp_path):
    f = self._parquet(tmp_path, {HOSTILE: [1, 2]})
    sql = DuckDBTableManager()._build_table_sql_with_file_id(
      '"T"',
      has_identifier=True,
      has_from_to=False,
      s3_files=[f],
      file_id_map={f: "file_1"},
      column_names=["identifier", HOSTILE],
    )
    conn.execute(sql)
    cols = [r[0] for r in conn.execute('DESCRIBE "T"').fetchall()]
    assert cols == ["identifier", HOSTILE, "file_id"]

  def test_relationship_shape(self, conn, tmp_path):
    f = _write_parquet(
      tmp_path / "r.parquet",
      {"from": ["a", "a"], "to": ["b", "b"], HOSTILE: [1, 2]},
    )
    sql = DuckDBTableManager()._build_table_sql(
      '"R"',
      has_identifier=False,
      has_from_to=True,
      use_list=False,
      column_names=["from", "to", HOSTILE],
    )
    conn.execute(sql, [f])
    cols = [r[0] for r in conn.execute('DESCRIBE "R"').fetchall()]
    assert cols == ["src", "dst", HOSTILE]
