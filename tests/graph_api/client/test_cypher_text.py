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
        "MATCH (n) RETURN n ORDER BY n.a DESC LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.a DESC, '' LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a SKIP 5 LIMIT 5",
        "MATCH (n) RETURN n ORDER BY n.a, '' SKIP 5 LIMIT 5",
      ),
      (
        "MATCH (n) RETURN n order by n.a;",
        "MATCH (n) RETURN n order by n.a, '';",
      ),
      (
        "MATCH (n) WITH n ORDER BY n.a LIMIT 3 MATCH (n)-->(m) RETURN m ORDER BY m.b",
        "MATCH (n) WITH n ORDER BY n.a, '' LIMIT 3 MATCH (n)-->(m) RETURN m "
        "ORDER BY m.b, ''",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.a\n",
        "MATCH (n) RETURN n ORDER BY n.a, ''\n",
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
    ],
  )
  def test_rewrites(self, query, expected):
    assert add_sort_tiebreaker(query) == expected

  def test_ignores_order_by_inside_literals_and_comments(self):
    query = (
      "MATCH (n) WHERE n.note = 'ORDER BY x' // ORDER BY y\nRETURN n /* ORDER BY z */"
    )
    assert add_sort_tiebreaker(query) == query

  def test_property_named_like_a_clause_does_not_end_the_keys(self):
    assert add_sort_tiebreaker("MATCH (n) RETURN n ORDER BY n.limit, n.skip") == (
      "MATCH (n) RETURN n ORDER BY n.limit, n.skip, ''"
    )

  def test_nested_sorts_each_get_a_tiebreaker(self):
    query = (
      "MATCH (n) RETURN n ORDER BY COUNT { MATCH (n)-->(m) RETURN m ORDER BY m.a }, n.b"
    )
    assert add_sort_tiebreaker(query) == (
      "MATCH (n) RETURN n ORDER BY COUNT { MATCH (n)-->(m) RETURN m "
      "ORDER BY m.a, '' }, n.b, ''"
    )

  def test_trailing_sort_in_a_subquery_stops_at_its_brace(self):
    query = "CALL { MATCH (n) RETURN n ORDER BY n.a } RETURN n"
    assert add_sort_tiebreaker(query) == (
      "CALL { MATCH (n) RETURN n ORDER BY n.a, '' } RETURN n"
    )


@pytest.mark.unit
class TestMaskLiterals:
  def test_keeps_offsets_and_blanks_literals(self):
    query = "RETURN 'a\\'b', \"c\", `d e` // f\n/* g */ LIMIT 1"
    masked = mask_literals(query)
    assert len(masked) == len(query)
    assert masked.split() == ["RETURN", ",", ",", "LIMIT", "1"]

  def test_unterminated_literal_blanks_to_the_end(self):
    assert mask_literals("RETURN 'abc LIMIT 5").strip() == "RETURN"

  @pytest.mark.parametrize("fragment", ["'\\", "\"\\'", "/*", "`"])
  def test_pathological_input_is_linear(self, fragment):
    query = "RETURN " + fragment * 20000
    start = time.perf_counter()
    mask_literals(query)
    add_sort_tiebreaker(query)
    assert time.perf_counter() - start < 0.5
