"""A write clause glued to a numeric literal is still a write.

The engine's lexer ends a numeric literal where a letter begins, so ``1SET``
is the tokens ``1`` and ``SET``; the classifier must split the same way, or a
viewer's read-only query writes.
"""

import pytest

from robosystems.security.cypher_analyzer import (
  is_admin_operation,
  is_bulk_operation,
  is_write_operation,
)


@pytest.mark.unit
@pytest.mark.parametrize(
  "query",
  [
    "MATCH (n:Dummy) WHERE n.id = 1SET n.v = 999",
    "MATCH (n:Dummy) WHERE n.id = 1CREATE (:Dummy {id: 3, v: 30})",
    "MATCH (n:Dummy) WHERE n.id = 2DELETE n",
    "UNWIND [5] AS x WITH x WHERE x = 5MERGE (:Dummy {id: 5, v: 50})",
    "MATCH (n:Dummy) RETURN n LIMIT 1CREATE (:Dummy {id: 9})",
    "MATCH (n:Dummy) WHERE n.v > 0.5SET n.v = 7",
    "MATCH (n:Dummy) WHERE n.id = 3DETACH DELETE n",
  ],
)
def test_write_keyword_after_numeric_literal_is_a_write(query):
  assert is_write_operation(query)


@pytest.mark.unit
@pytest.mark.parametrize(
  "query",
  [
    "MATCH (n:Dummy) WHERE n.x1set = 1 RETURN n.created_at",
    "MATCH (n:Dummy) WHERE n.v = 1 RETURN n.delete_count",
    "MATCH (n:Dummy) RETURN n.merge_key, n.set_at",
  ],
)
def test_identifier_containing_a_keyword_is_still_a_read(query):
  assert not is_write_operation(query)


@pytest.mark.unit
def test_bulk_and_admin_keywords_after_numeric_literals_are_caught():
  assert is_bulk_operation("RETURN 1COPY Dummy FROM '/tmp/x.csv'")
  assert is_admin_operation("RETURN 1ATTACH '/tmp/x' AS y")
