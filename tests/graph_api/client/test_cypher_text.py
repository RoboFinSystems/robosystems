"""Text rewrites applied before a Cypher query reaches the Graph API."""

import time

import pytest

from robosystems.graph_api.client.cypher_text import mask_literals


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
  ],
)
def test_pathological_input_is_linear(query):
  start = time.perf_counter()
  mask_literals(query)
  assert time.perf_counter() - start < 0.5
