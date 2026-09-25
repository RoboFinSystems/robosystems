"""A mapping is counted once its write lands, not when the model proposed it.

``CreateMappingAssociationTool`` never raises — a rejected element, a duplicate
arc, or a database fault come back as ``{"error": ...}`` — so the pass used to
report every failed persist as mapped, and "Mapped N" disagreed with the books.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.operators.implementations.mapping.operator import (
  MappingOperator,
)

pytestmark = pytest.mark.unit

_MOD = "robosystems.operations.operators.implementations.mapping.operator"


class _Tool:
  def __init__(self, result):
    self.result = result
    self.calls: list[dict] = []

  async def execute(self, args):
    self.calls.append(args)
    return self.result(args) if callable(self.result) else self.result


class _Tools:
  def __init__(self, by_name):
    self.by_name = by_name

  def get_tool_instance(self, cls):
    return self.by_name[cls.__name__]


class _Ctx:
  graph_id = "kg_test"
  user_id = "user_test"

  def __init__(self, tools):
    self.extra = {"mapping_id": "map_1"}
    self.tools = tools
    self.progress = AsyncMock()
    self.progress.is_cancelled = AsyncMock(return_value=False)


def _elements(n):
  return [
    {"id": f"el_{i}", "name": f"Account {i}", "code": str(i), "trait": "Assets"}
    for i in range(n)
  ]


async def _run(create_result, mappings):
  create = _Tool(create_result)
  tools = _Tools(
    {
      "GetUnmappedElementsTool": _Tool(
        {"elements": _elements(len(mappings)), "unmapped_count": len(mappings)}
      ),
      "SuggestMappingTool": _Tool({"candidates": [{"id": "rs_cash"}]}),
      "CreateMappingAssociationTool": create,
      "GetMappingSummaryTool": _Tool({"error": "not measured"}),
    }
  )
  op = MappingOperator.__new__(MappingOperator)
  with patch.object(MappingOperator, "_map_batch", AsyncMock(return_value=mappings)):
    result = await op._run_single_pass(_Ctx(tools))
  return result, create


@pytest.mark.asyncio
async def test_persisted_mappings_are_counted():
  mappings = [
    {"element_id": "el_0", "target_id": "rs_cash", "confidence": 0.95},
    {"element_id": "el_1", "target_id": "rs_cash", "confidence": 0.75},
  ]
  result, create = await _run({"association_id": "assoc_1"}, mappings)

  assert len(create.calls) == 2
  assert result.metadata["mapped"] == 1
  assert result.metadata["flagged"] == 1
  assert result.metadata["skipped"] == 0


@pytest.mark.asyncio
async def test_a_rejected_write_is_skipped_not_mapped():
  """The tool returned an error dict (element not found, duplicate, DB fault):
  the element is not mapped, whatever the model's confidence said."""
  mappings = [
    {"element_id": "el_0", "target_id": "rs_cash", "confidence": 0.95},
    {"element_id": "el_1", "target_id": "rs_cash", "confidence": 0.95},
  ]

  def _create(args):
    if args["from_element_id"] == "el_1":
      return {"error": "Element not found: rs_cash"}
    return {"association_id": "assoc_1"}

  result, _create_tool = await _run(_create, mappings)

  assert result.metadata["mapped"] == 1
  assert result.metadata["skipped"] == 1
  assert "Mapped 1" in result.content
  assert "skipped 1" in result.content


@pytest.mark.asyncio
async def test_a_closed_history_refusal_is_counted_apart_from_skipped():
  """The ledger refused the arc because the account has landed history in a
  closed month. Not the operator's call to make — the receipt says so."""
  mappings = [
    {"element_id": "el_0", "target_id": "rs_cash", "confidence": 0.95},
    {"element_id": "el_1", "target_id": "rs_cash", "confidence": 0.95},
  ]

  # The refusal exactly as the registrar tool, the one production uses,
  # renders ProtectedFactsError for this operation.
  from robosystems.middleware.extensions import OperationRegistrar
  from robosystems.middleware.mcp.tools.registrar import (
    _ensure_specs_registered,
    translate_error,
  )
  from robosystems.operations.taxonomy_block.immutability import ProtectedFactsError

  _ensure_specs_registered("roboledger")
  spec = next(
    spec
    for _reg, spec in OperationRegistrar.specs_for_extension("roboledger")
    if spec.name == "create-mapping-association"
  )
  refusal = translate_error(
    ProtectedFactsError(
      filed_report_count=0,
      closed_period_count=1,
      closed_period_names=["2026-06"],
    ),
    spec.error_map,
  )

  def _create(args):
    if args["from_element_id"] == "el_1":
      return refusal
    return {"association_id": "assoc_1"}

  result, create_tool = await _run(_create, mappings)

  assert result.metadata["mapped"] == 1
  assert result.metadata["skipped"] == 0
  assert result.metadata["refused_closed_history"] == 1
  assert "1 refused" in result.content
  assert "reopen" in result.content
  assert all(c["suggested_by"] == "mapping-agent" for c in create_tool.calls)


@pytest.mark.asyncio
async def test_a_write_that_raises_is_skipped_not_mapped():
  mappings = [{"element_id": "el_0", "target_id": "rs_cash", "confidence": 0.95}]

  def _create(_args):
    raise RuntimeError("database is away")

  result, _ = await _run(_create, mappings)

  assert result.metadata["mapped"] == 0
  assert result.metadata["skipped"] == 1
