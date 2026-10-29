"""The engine's ORDER BY is correct on the shapes LadybugDB #1067 once broke.

Before 0.22, past a few thousand scanned rows the engine dropped and misordered
rows when a sort's last key followed a STRING key, at any LIMIT, and the Graph
API client appended a constant string key to every multi-key sort to avoid it.
The rewrite is gone; these run the plain queries against a real embedded engine
so a regression in the pinned release fails here, not in a report.
"""

from __future__ import annotations

import ladybug as lbug
import pytest

ROWS = 20_000
PROJECTION = "MATCH (o:Obs) RETURN o.name AS a, o.value AS b"


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
  db = lbug.Database(str(tmp_path_factory.mktemp("sort") / "sort.lbdb"))
  c = lbug.Connection(db)
  c.execute(
    "CREATE NODE TABLE Obs(id INT64, name STRING, value DOUBLE, PRIMARY KEY(id))"
  )
  c.execute(
    f"COPY Obs FROM (UNWIND range(0, {ROWS - 1}) AS o "
    "RETURN o, 'item' + CAST(o % 3 AS STRING), CAST((o * 7919) % 100003 AS DOUBLE))"
  )
  yield c
  c.close()
  db.close()


@pytest.fixture(scope="module")
def ascending(conn):
  return sorted(_rows(conn, PROJECTION))


def _rows(conn, cypher):
  return [tuple(r) for r in conn.execute(cypher).get_all()]


@pytest.mark.unit
def test_engine_sorts_string_then_number(conn, ascending):
  runs = [_rows(conn, f"{PROJECTION} ORDER BY a, b") for _ in range(5)]
  assert all(run == ascending for run in runs)


@pytest.mark.unit
@pytest.mark.parametrize("limit", ["", " LIMIT 10", " LIMIT 1000", " LIMIT 10000"])
def test_string_then_number(conn, ascending, limit):
  expected = ascending[: int(limit.split()[-1])] if limit else ascending
  assert _rows(conn, f"{PROJECTION} ORDER BY a, b{limit}") == expected


@pytest.mark.unit
def test_descending(conn, ascending):
  assert _rows(conn, f"{PROJECTION} ORDER BY a DESC, b DESC") == ascending[::-1]


@pytest.mark.unit
def test_distinct(conn, ascending):
  query = "MATCH (o:Obs) RETURN DISTINCT o.name AS a, o.value AS b ORDER BY a, b"
  assert _rows(conn, query) == ascending


@pytest.mark.unit
def test_aggregation(conn, ascending):
  query = (
    "MATCH (o:Obs) RETURN o.name AS a, o.value AS b, count(*) AS n "
    "ORDER BY a, b LIMIT 100"
  )
  assert _rows(conn, query) == [(a, b, 1) for a, b in ascending[:100]]


@pytest.mark.unit
def test_with_clause_sort_feeds_a_limit(conn, ascending):
  query = (
    "MATCH (o:Obs) WITH o.name AS a, o.value AS b ORDER BY a, b SKIP 5 LIMIT 20 "
    "RETURN a, b"
  )
  assert _rows(conn, query) == ascending[5:25]


@pytest.fixture(scope="module")
def nullable(tmp_path_factory):
  db = lbug.Database(str(tmp_path_factory.mktemp("nulls") / "nulls.lbdb"))
  c = lbug.Connection(db)
  c.execute("CREATE NODE TABLE T(id INT64, v INT64, s STRING, PRIMARY KEY(id))")
  c.execute(
    "COPY T FROM (UNWIND range(0, 99999) AS o RETURN o, (o * 7919) % 100003, "
    "CASE WHEN o % 17 = 0 THEN NULL ELSE 's' + CAST(o % 5 AS STRING) END)"
  )
  yield c
  c.close()
  db.close()


@pytest.mark.unit
def test_sort_with_null_keys(nullable):
  rows = _rows(nullable, "MATCH (t:T) RETURN t.s, t.v")
  first = sorted((r for r in rows if r[0] is None), key=lambda r: r[1])[:10]
  query = "MATCH (t:T) RETURN t.s, t.v ORDER BY t.s DESC, t.v LIMIT 10"
  assert _rows(nullable, query) == first
