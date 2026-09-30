"""The sort tiebreaker makes the engine's ORDER BY correct (LadybugDB #1067).

Past a few thousand scanned rows the engine drops and misorders rows when a
sort's last key follows a STRING key, at any LIMIT. Every query the Graph API
client sends ends each ORDER BY with a constant string key; these run the
rewritten queries against a real embedded engine.
"""

from __future__ import annotations

import ladybug as lbug
import pytest

from robosystems.graph_api.client.cypher_text import add_sort_tiebreaker

ROWS = 20_000
PROJECTION = "MATCH (o:Obs) RETURN o.name AS a, o.value AS b"


@pytest.fixture(scope="module")
def conn(tmp_path_factory):
  db = lbug.Database(str(tmp_path_factory.mktemp("sort") / "sort.lbug"))
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


def _sorted(conn, cypher):
  return _rows(conn, add_sort_tiebreaker(cypher))


@pytest.mark.unit
@pytest.mark.xfail(
  strict=True,
  reason="LadybugDB #1067: once the engine sorts this correctly, the "
  "tiebreaker in cypher_text.py is dead weight; remove it on purpose",
)
def test_engine_sorts_string_then_number(conn, ascending):
  runs = [_rows(conn, f"{PROJECTION} ORDER BY a, b") for _ in range(5)]
  assert all(run == ascending for run in runs)


@pytest.mark.unit
@pytest.mark.parametrize("limit", ["", " LIMIT 10", " LIMIT 1000", " LIMIT 10000"])
def test_string_then_number(conn, ascending, limit):
  expected = ascending[: int(limit.split()[-1])] if limit else ascending
  assert _sorted(conn, f"{PROJECTION} ORDER BY a, b{limit}") == expected


@pytest.mark.unit
def test_descending(conn, ascending):
  assert _sorted(conn, f"{PROJECTION} ORDER BY a DESC, b DESC") == ascending[::-1]


@pytest.mark.unit
def test_distinct(conn, ascending):
  query = "MATCH (o:Obs) RETURN DISTINCT o.name AS a, o.value AS b ORDER BY a, b"
  assert _sorted(conn, query) == ascending


@pytest.mark.unit
def test_aggregation(conn, ascending):
  query = (
    "MATCH (o:Obs) RETURN o.name AS a, o.value AS b, count(*) AS n "
    "ORDER BY a, b LIMIT 100"
  )
  assert _sorted(conn, query) == [(a, b, 1) for a, b in ascending[:100]]


@pytest.mark.unit
def test_with_clause_sort_feeds_a_limit(conn, ascending):
  query = (
    "MATCH (o:Obs) WITH o.name AS a, o.value AS b ORDER BY a, b SKIP 5 LIMIT 20 "
    "RETURN a, b"
  )
  assert _sorted(conn, query) == ascending[5:25]
