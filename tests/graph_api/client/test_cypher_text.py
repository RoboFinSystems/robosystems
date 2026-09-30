"""Text rewrites applied before a Cypher query reaches the Graph API."""

import time

import pytest

from robosystems.graph_api.client.cypher_text import add_sort_tiebreaker, mask_literals


@pytest.mark.unit
class TestAddSortTiebreaker:
  @pytest.mark.parametrize(
    ("query", "expected"),
    [
      ("MATCH (n) RETURN n", "MATCH (n) RETURN n"),
      (
        "MATCH (n) RETURN n.a, n.b ORDER BY n.a, n.b",
        "MATCH (n) RETURN n.a, n.b ORDER BY n.a, n.b, ''",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a DESC, n.b LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.a DESC, n.b, '' LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a, n.b SKIP 5 LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.a, n.b, '' SKIP 5 LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n order by n.a, n.b;",
        "MATCH (n) RETURN n order by n.a, n.b, '';",
      ),
      (
        "MATCH (n) WITH n ORDER BY n.a, n.b LIMIT 3 MATCH (n)-->(m) RETURN m "
        "ORDER BY m.b, m.c",
        "MATCH (n) WITH n ORDER BY n.a, n.b, '' LIMIT 3 MATCH (n)-->(m) RETURN m "
        "ORDER BY m.b, m.c, ''",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a, n.b\n",
        "MATCH (n) RETURN n ORDER BY n.a, n.b, ''\n",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a, n.b // why\n",
        "MATCH (n) RETURN n ORDER BY n.a, n.b, '' // why\n",
      ),
      (
        "MATCH (n) RETURN n ORDER BY coalesce(n.a, 'LIMIT x'), size([1, 2]) LIMIT 5",
        "MATCH (n) RETURN n ORDER BY coalesce(n.a, 'LIMIT x'), size([1, 2]), '' "
        "LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY CASE WHEN n.a > 1 THEN 0 ELSE 1 END, n.b",
        "MATCH (n) RETURN n ORDER BY CASE WHEN n.a > 1 THEN 0 ELSE 1 END, n.b, ''",
      ),
      (
        "MATCH (n) RETURN n.name AS `Company`, n.x AS x ORDER BY x, `Company` LIMIT 5",
        "MATCH (n) RETURN n.name AS `Company`, n.x AS x ORDER BY x, `Company`, '' "
        "LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.x, n.`name`",
        "MATCH (n) RETURN n ORDER BY n.x, n.`name`, ''",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.x, n.form = '10-K' LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.x, n.form = '10-K', '' LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.name ENDS WITH 'Inc', n.name STARTS WITH 'A'",
        "MATCH (n) RETURN n ORDER BY n.name ENDS WITH 'Inc', n.name STARTS WITH 'A', ''",
      ),
      (
        "MATCH (n) WITH n.a AS skip, n.b AS b RETURN skip, b ORDER BY skip, b",
        "MATCH (n) WITH n.a AS skip, n.b AS b RETURN skip, b ORDER BY skip, b, ''",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.limit, n.skip",
        "MATCH (n) RETURN n ORDER BY n.limit, n.skip, ''",
      ),
      # One key never meets the bug, and a second would cost it the top-k path.
      (
        "MATCH (n) RETURN n ORDER BY n.a DESC LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.a DESC LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY coalesce(n.a, n.b) LIMIT 5",
        "MATCH (n) RETURN n ORDER BY coalesce(n.a, n.b) LIMIT 5",
      ),
    ],
  )
  def test_rewrites(self, query, expected):
    assert add_sort_tiebreaker(query) == expected

  def test_ignores_order_by_inside_literals_and_comments(self):
    query = (
      "MATCH (n) WHERE n.note = 'ORDER BY x, y' // ORDER BY y, z\n"
      "RETURN n /* ORDER BY z, w */"
    )
    assert add_sort_tiebreaker(query) == query

  def test_nested_sorts_each_get_a_tiebreaker(self):
    query = (
      "MATCH (n) RETURN n ORDER BY COUNT { MATCH (n)-->(m) RETURN m ORDER BY m.a, m.b }"
      ", n.b"
    )
    assert add_sort_tiebreaker(query) == (
      "MATCH (n) RETURN n ORDER BY COUNT { MATCH (n)-->(m) RETURN m "
      "ORDER BY m.a, m.b, '' }, n.b, ''"
    )

  def test_sort_in_a_subquery_stops_at_its_brace(self):
    query = "CALL { MATCH (n) RETURN n ORDER BY n.a, n.b } RETURN n"
    assert add_sort_tiebreaker(query) == (
      "CALL { MATCH (n) RETURN n ORDER BY n.a, n.b, '' } RETURN n"
    )


@pytest.mark.unit
class TestMaskLiterals:
  def test_keeps_offsets_and_fills_literals(self):
    query = "RETURN 'a\\'b', \"c\", `d e` // f\n/* g */ LIMIT 1"
    masked = mask_literals(query)
    assert len(masked) == len(query)
    assert masked.split() == ["RETURN", "######,", "###,", "#####", "LIMIT", "1"]

  def test_unterminated_literal_fills_to_the_end(self):
    assert mask_literals("RETURN 'abc LIMIT 5") == "RETURN ############"


@pytest.mark.unit
@pytest.mark.parametrize(
  "query",
  [
    "RETURN " + "'\\" * 25000,
    "RETURN " + "\"\\'" * 16000,
    "RETURN " + "/*" * 25000,
    "RETURN " + "`" * 50000,
    "RETURN 1 " + "ORDER BY x " * 4500,
    "RETURN 1 " + "ORDER BY x, y " * 3500,
    "RETURN 1 " + "ORDER BY (" * 5000,
    "RETURN 1 " + "ORDER BY 'a', (" * 3000 + " " * 5000,
  ],
)
def test_pathological_input_is_linear(query):
  start = time.perf_counter()
  add_sort_tiebreaker(query)
  assert time.perf_counter() - start < 0.5
