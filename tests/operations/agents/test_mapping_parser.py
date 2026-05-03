"""Parser tests for ``MappingAgent._parse_response``.

The auto-mapper only creates CoA→FAC arcs for elements whose batch
parses cleanly. Bedrock occasionally returns shapes the original
``json.loads`` couldn't handle — concatenated objects, multiple arrays,
or fenced code blocks — and silently drops the whole batch. The
balance-sheet trait batches were the canonical victim. These cases pin
the parser's tolerance to those shapes so the regression doesn't
recur.
"""

from __future__ import annotations

from robosystems.operations.agents.implementations.mapping.agent import MappingAgent


def _elements(*ids: str) -> list[dict]:
  return [{"id": eid} for eid in ids]


def _agent() -> MappingAgent:
  # The parser is a pure method on the class — instantiating without
  # __init__ args is fine because we never touch agent state.
  return MappingAgent.__new__(MappingAgent)


def test_clean_array_parses_each_object():
  content = (
    '[{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.95, "reasoning": "x"},'
    '{"element_id": "b", "target_id": null, "target_qname": null, '
    '"confidence": 0.5, "reasoning": "low"}]'
  )
  out = _agent()._parse_response(content, _elements("a", "b"))
  assert [m["element_id"] for m in out] == ["a", "b"]
  assert out[0]["target_id"] == "fac1"
  assert out[1]["target_id"] is None


def test_concatenated_objects_are_recovered():
  """Claude sometimes emits one object per line rather than wrapping
  in a single array. raw_decode walks the buffer and recovers all of
  them — the batch isn't dropped just because the wrapper is missing."""
  content = (
    '{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.9, "reasoning": "x"}\n'
    '{"element_id": "b", "target_id": "fac2", "target_qname": "fac:B", '
    '"confidence": 0.85, "reasoning": "y"}\n'
    '{"element_id": "c", "target_id": null, "target_qname": null, '
    '"confidence": 0.5, "reasoning": "z"}'
  )
  out = _agent()._parse_response(content, _elements("a", "b", "c"))
  assert [m["element_id"] for m in out] == ["a", "b", "c"]
  assert out[0]["target_id"] == "fac1"
  assert out[1]["target_id"] == "fac2"
  assert out[2]["target_id"] is None


def test_array_followed_by_extra_object_does_not_drop_batch():
  """The exact failure mode the worker logs flagged on the asset and
  liability batches: a JSON array followed by trailing extra data."""
  content = (
    '[{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.9, "reasoning": "x"}]\n'
    '{"element_id": "b", "target_id": "fac2", "target_qname": "fac:B", '
    '"confidence": 0.85, "reasoning": "y"}'
  )
  out = _agent()._parse_response(content, _elements("a", "b"))
  assert [m["element_id"] for m in out] == ["a", "b"]


def test_markdown_fence_is_stripped():
  content = (
    "```json\n"
    '[{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.9, "reasoning": "x"}]\n'
    "```"
  )
  out = _agent()._parse_response(content, _elements("a"))
  assert out[0]["target_id"] == "fac1"


def test_prose_between_objects_is_skipped():
  """Whitespace and stray text between top-level JSON values is
  tolerated — we walk forward to the next ``{`` or ``[`` and resume."""
  content = (
    '{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.9, "reasoning": "x"}\n\n'
    "Here is the next mapping:\n"
    '{"element_id": "b", "target_id": "fac2", "target_qname": "fac:B", '
    '"confidence": 0.85, "reasoning": "y"}'
  )
  out = _agent()._parse_response(content, _elements("a", "b"))
  assert [m["element_id"] for m in out] == ["a", "b"]


def test_completely_unparseable_response_returns_skip_records():
  out = _agent()._parse_response("not json at all", _elements("a", "b"))
  # One skip record per input element so the orchestrator can count
  # them rather than losing track of the batch.
  assert len(out) == 2
  assert all(m["target_id"] is None for m in out)
  assert all(m["confidence"] == 0 for m in out)


def test_single_object_outside_array_is_wrapped():
  """Some batches ship a single mapping; the prompt asks for an array
  but the parser shouldn't punish a single-element response."""
  content = (
    '{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": 0.95, "reasoning": "x"}'
  )
  out = _agent()._parse_response(content, _elements("a"))
  assert len(out) == 1
  assert out[0]["element_id"] == "a"


def test_confidence_coerced_to_float():
  content = (
    '[{"element_id": "a", "target_id": "fac1", "target_qname": "fac:A", '
    '"confidence": "0.95", "reasoning": "x"}]'
  )
  out = _agent()._parse_response(content, _elements("a"))
  assert out[0]["confidence"] == 0.95
  assert isinstance(out[0]["confidence"], float)
