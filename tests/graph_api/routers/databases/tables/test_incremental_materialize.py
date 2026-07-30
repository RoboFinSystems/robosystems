"""Incremental materialization: keyset-export + anti-join parity tests.

Exercises the REAL helpers (`_export_incremental_keyset`, `_incr_keyset_path`)
against real ladybug + duckdb engines, proving that an incremental COPY into a
populated graph yields the SAME graph as a full load — the correctness contract
the incremental mode rests on:

  - node dup PRIMARY KEY COPY hard-fails  -> node anti-join is mandatory
  - rel dup (src,dst) COPY silently duplicates -> rel anti-join is mandatory

so the anti-join must produce a graph identical to a from-empty full load, add
no duplicate edges, and be idempotent (a re-run with no new staging adds 0 rows).

COPY is done via parquet files here (dedup/anti-join semantics are transport-
independent — the production handler streams the same rows over Arrow instead).
"""

from types import SimpleNamespace

import duckdb
import ladybug as lbug
import pytest

from robosystems.graph_api.routers.databases.tables.materialize import (
  _export_incremental_keyset,
  _incr_keyset_path,
)


def _ladybug_service(conn):
  """Minimal stand-in exposing db_manager.connection_pool.get_connection(...)
  so the real _export_incremental_keyset runs against a plain ladybug conn."""

  class _Ctx:
    def __enter__(self):
      return conn

    def __exit__(self, *exc):
      return False

  pool = SimpleNamespace(get_connection=lambda _graph_id: _Ctx())
  return SimpleNamespace(db_manager=SimpleNamespace(connection_pool=pool))


def _load(conn, duck, table_name, select_sql, out_path):
  """DuckDB SELECT -> parquet -> ladybug COPY. Returns rows COPYed.

  Counts with duckdb rather than ``pyarrow.parquet.read_metadata``: duckdb
  wrote the file, and pyarrow's local-file operations raise
  ``ArrowKeyError: Attempted to register factory for scheme 'file'`` in any
  process that has imported networkit (see the SEC knowledge package) —
  which, in a full-suite run, is this one.
  """
  esc = str(out_path).replace("'", "''")
  duck.execute(f"COPY ({select_sql}) TO '{esc}' (FORMAT parquet)")
  rows = duck.execute(f"SELECT count(*) FROM read_parquet('{esc}')").fetchone()[0]
  if rows:
    conn.execute(f"COPY {table_name} FROM '{out_path}'")
  return rows


def _antijoin_select(table_name, select_expr, snapshot_path, is_rel):
  """Mirror the handler's incremental anti-join SELECT (materialize.py)."""
  esc = str(snapshot_path).replace("'", "''")
  if is_rel:
    incr = (
      f"NOT EXISTS (SELECT 1 FROM read_parquet('{esc}') k "
      "WHERE k.src = t.src AND k.dst = t.dst)"
    )
  else:
    incr = (
      f"NOT EXISTS (SELECT 1 FROM read_parquet('{esc}') k "
      "WHERE k.identifier = t.identifier)"
    )
  return f"SELECT {select_expr} FROM {table_name} AS t WHERE ({incr})"


def _incremental_copy(gc, duck, table_name, select_expr, is_rel, tmp_path, tag):
  """Reproduce the server incremental path for one table: export keyset ->
  anti-join SELECT -> COPY only the delta. Returns rows COPYed."""
  snapshot_path = tmp_path / f"{tag}_keys.parquet"
  _export_incremental_keyset(
    _ladybug_service(gc), "sec", table_name, is_rel, snapshot_path
  )
  select_sql = _antijoin_select(table_name, select_expr, snapshot_path, is_rel)
  return _load(gc, duck, table_name, select_sql, tmp_path / f"{tag}_delta.parquet")


def _new_graph(path):
  g = lbug.Database(str(path), read_only=False)
  c = lbug.Connection(g)
  c.execute(
    "CREATE NODE TABLE N(identifier STRING, name STRING, PRIMARY KEY(identifier))"
  )
  c.execute("CREATE REL TABLE R(FROM N TO N)")
  return g, c


def _counts(c):
  n = c.execute("MATCH (n:N) RETURN count(*)").get_as_arrow().column(0)[0].as_py()
  r = (
    c.execute("MATCH (:N)-[e:R]->(:N) RETURN count(*)")
    .get_as_arrow()
    .column(0)[0]
    .as_py()
  )
  return n, r


def test_incr_keyset_path_is_transient_local(monkeypatch):
  """Snapshot lives under DUCKDB_STAGING_PATH/incr_keys — never a .duckdb/.lbug
  file (so it is never published to S3)."""
  from robosystems.config import env

  monkeypatch.setattr(env, "DUCKDB_STAGING_PATH", "/data/staging", raising=False)
  p = _incr_keyset_path("sec", "FACT_HAS_ELEMENT")
  assert p.as_posix() == "/data/staging/incr_keys/sec/FACT_HAS_ELEMENT.parquet"
  assert p.suffix == ".parquet"
  assert ".lbug" not in p.as_posix() and ".duckdb" not in p.as_posix()


@pytest.mark.parametrize("bad", ["../etc", "a/b", "sec/../sec", "."])
def test_incr_keyset_path_rejects_traversal(bad, monkeypatch):
  """graph_id / table_name are URL path params — a traversal attempt must raise,
  not build a path outside the staging dir."""
  from robosystems.config import env

  monkeypatch.setattr(env, "DUCKDB_STAGING_PATH", "/data/staging", raising=False)
  with pytest.raises(ValueError):
    _incr_keyset_path(bad, "Fact")
  with pytest.raises(ValueError):
    _incr_keyset_path("sec", bad)


@pytest.mark.integration
def test_incremental_equals_full_load(tmp_path):
  """Incremental COPY into a populated graph == a full load of the same staging,
  with no duplicate edges and full idempotency."""
  # Shared staging (the source of truth): superset of the pre-populated graph.
  duck = duckdb.connect(str(tmp_path / "staging.duckdb"))
  duck.execute("CREATE TABLE N(identifier VARCHAR, name VARCHAR)")
  duck.execute("INSERT INTO N VALUES ('a','A'),('b','B'),('c','C'),('d','D'),('e','E')")
  duck.execute("CREATE TABLE R(src VARCHAR, dst VARCHAR)")
  duck.execute("INSERT INTO R VALUES ('a','b'),('b','c'),('c','d')")

  # ---- Path A: full load into a fresh graph ------------------------------
  _, full = _new_graph(tmp_path / "full.lbug")
  _load(full, duck, "N", "SELECT identifier, name FROM N", tmp_path / "fa_n.parquet")
  _load(full, duck, "R", "SELECT src, dst FROM R", tmp_path / "fa_r.parquet")
  full_counts = _counts(full)

  # ---- Path B: pre-populate a graph, then incremental-COPY the same staging
  _, incr = _new_graph(tmp_path / "incr.lbug")
  duck.execute(
    "CREATE TABLE seed_n AS SELECT * FROM N WHERE identifier IN ('a','b','c')"
  )
  duck.execute("CREATE TABLE seed_r AS SELECT * FROM R WHERE src='a' AND dst='b'")
  _load(
    incr, duck, "N", "SELECT identifier, name FROM seed_n", tmp_path / "seed_n.parquet"
  )
  _load(incr, duck, "R", "SELECT src, dst FROM seed_r", tmp_path / "seed_r.parquet")

  added_n = _incremental_copy(
    incr, duck, "N", "identifier, name", False, tmp_path, "n1"
  )
  added_r = _incremental_copy(incr, duck, "R", '"src", "dst"', True, tmp_path, "r1")
  incr_counts = _counts(incr)

  # only new rows added this run
  assert added_n == 2  # d, e
  assert added_r == 2  # b->c, c->d
  # parity: incremental graph == full-load graph
  assert incr_counts == full_counts == (5, 3)
  # no duplicate edges (a->b was seeded AND in staging, must appear once)
  dup = incr.execute(
    "MATCH (a:N)-[e:R]->(b:N) RETURN a.identifier, b.identifier, count(*) AS c"
  ).get_as_arrow()
  assert max(dup.column("c").to_pylist()) == 1

  # ---- idempotency: re-run with no new staging adds nothing, does not crash
  again_n = _incremental_copy(
    incr, duck, "N", "identifier, name", False, tmp_path, "n2"
  )
  again_r = _incremental_copy(incr, duck, "R", '"src", "dst"', True, tmp_path, "r2")
  assert again_n == 0 and again_r == 0
  assert _counts(incr) == (5, 3)


@pytest.mark.integration
def test_incremental_empty_graph_degrades_to_full_load(tmp_path):
  """First run against an empty graph: the keyset export is empty, the anti-join
  passes everything, and every staged row loads."""
  duck = duckdb.connect(str(tmp_path / "staging.duckdb"))
  duck.execute("CREATE TABLE N(identifier VARCHAR, name VARCHAR)")
  duck.execute("INSERT INTO N VALUES ('a','A'),('b','B')")

  _, c = _new_graph(tmp_path / "empty.lbug")  # N exists but is empty
  added = _incremental_copy(c, duck, "N", "identifier, name", False, tmp_path, "n")
  assert added == 2
  assert (
    c.execute("MATCH (n:N) RETURN count(*)").get_as_arrow().column(0)[0].as_py() == 2
  )
