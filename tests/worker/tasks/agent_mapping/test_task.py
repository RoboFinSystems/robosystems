"""Tests for the MappingAgent task handler."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.worker.tasks.agent_mapping.task import (
  CONFIDENCE_AUTO_APPROVE,
  CONFIDENCE_MIN_MAP,
  AgentMappingTask,
)


@pytest.fixture
def mock_manager():
  manager = AsyncMock()
  manager.emit_progress = AsyncMock()
  manager.get_operation_status = AsyncMock(return_value=None)
  return manager


@pytest.fixture
def mock_ai_response():
  """Create a mock AIResponse."""
  response = MagicMock()
  response.content = json.dumps(
    [
      {
        "element_id": "elem_cash",
        "target_id": "elem_gaap_cash",
        "target_qname": "us-gaap:CashAndCashEquivalents",
        "confidence": 0.95,
        "reasoning": "Direct match",
      },
      {
        "element_id": "elem_ar",
        "target_id": "elem_gaap_ar",
        "target_qname": "us-gaap:AccountsReceivableNet",
        "confidence": 0.85,
        "reasoning": "Strong match",
      },
      {
        "element_id": "elem_misc",
        "target_id": None,
        "target_qname": None,
        "confidence": 0.40,
        "reasoning": "Too ambiguous",
      },
    ]
  )
  response.input_tokens = 500
  response.output_tokens = 200
  response.model = "anthropic.claude-3-5-sonnet-20241022-v2:0"
  return response


def _make_task(mock_manager, params=None) -> AgentMappingTask:
  return AgentMappingTask(
    task_id="task_01TEST",
    graph_id="kg0123456789abcdef",
    user_id="user_01TEST",
    params=params or {"mapping_id": "struct_mapping_01"},
    manager=mock_manager,
  )


UNMAPPED_RESULT = {
  "unmapped_count": 3,
  "total_coa_elements": 10,
  "elements": [
    {
      "id": "elem_cash",
      "name": "Cash",
      "classification": "asset",
      "balance_type": "debit",
    },
    {
      "id": "elem_ar",
      "name": "Accounts Receivable",
      "classification": "asset",
      "balance_type": "debit",
    },
    {
      "id": "elem_misc",
      "name": "Miscellaneous",
      "classification": "expense",
      "balance_type": "debit",
    },
  ],
}

SUGGEST_RESULT_ASSET = {
  "source_element": {"id": "elem_cash", "name": "Cash"},
  "candidates": [
    {
      "id": "elem_gaap_cash",
      "qname": "us-gaap:CashAndCashEquivalents",
      "name": "Cash and Cash Equivalents",
      "classification": "asset",
      "depth": 1,
      "is_abstract": False,
    },
    {
      "id": "elem_gaap_ar",
      "qname": "us-gaap:AccountsReceivableNet",
      "name": "Accounts Receivable, Net",
      "classification": "asset",
      "depth": 1,
      "is_abstract": False,
    },
  ],
}

SUGGEST_RESULT_EXPENSE = {
  "source_element": {"id": "elem_misc", "name": "Miscellaneous"},
  "candidates": [],
}

SUMMARY_RESULT = {
  "total_coa_elements": 10,
  "mapped_count": 7,
  "unmapped_count": 3,
  "coverage_percent": 70.0,
}


@pytest.mark.asyncio
@patch("robosystems.worker.tasks.agent_mapping.task.AIClient")
async def test_happy_path(MockAIClient, mock_manager, mock_ai_response):
  mock_ai = AsyncMock()
  MockAIClient.return_value = mock_ai
  mock_ai.create_message.return_value = mock_ai_response

  # Mock MCP tools
  mock_unmapped = AsyncMock()
  mock_unmapped.execute.return_value = UNMAPPED_RESULT

  mock_suggest = AsyncMock()
  mock_suggest.execute.side_effect = lambda args: (
    SUGGEST_RESULT_EXPENSE
    if args.get("classification") == "expense"
    else SUGGEST_RESULT_ASSET
  )

  mock_create = AsyncMock()
  mock_create.execute.return_value = {"created": True, "association_id": "assoc_01"}

  mock_summary = AsyncMock()
  mock_summary.execute.return_value = SUMMARY_RESULT

  task = _make_task(mock_manager)

  with (
    patch(
      "robosystems.middleware.mcp.tools.taxonomy_tools.GetUnmappedElementsTool",
      return_value=mock_unmapped,
    ),
    patch(
      "robosystems.middleware.mcp.tools.taxonomy_tools.SuggestMappingTool",
      return_value=mock_suggest,
    ),
    patch(
      "robosystems.middleware.mcp.tools.taxonomy_tools.CreateMappingAssociationTool",
      return_value=mock_create,
    ),
    patch(
      "robosystems.middleware.mcp.tools.taxonomy_tools.GetMappingSummaryTool",
      return_value=mock_summary,
    ),
    patch.object(task, "_consume_credits", new_callable=AsyncMock, return_value=0.5),
  ):
    result = await task.execute()

  assert result["mapped"] == 1  # cash (>= 0.90)
  assert result["flagged"] == 1  # AR (0.85, >= 0.70 but < 0.90)
  assert result["skipped"] >= 1  # misc (0.40 or no candidates)
  assert "coverage_percent" in result

  # MCP tools were called
  mock_unmapped.execute.assert_called_once()
  assert mock_suggest.execute.call_count >= 1
  assert mock_create.execute.call_count >= 1
  mock_summary.execute.assert_called_once()

  # Progress reported
  assert mock_manager.emit_progress.call_count >= 2


@pytest.mark.asyncio
@patch("robosystems.worker.tasks.agent_mapping.task.AIClient")
async def test_all_mapped(MockAIClient, mock_manager):
  mock_unmapped = AsyncMock()
  mock_unmapped.execute.return_value = {
    "unmapped_count": 0,
    "total_coa_elements": 10,
    "elements": [],
  }

  task = _make_task(mock_manager)

  with patch(
    "robosystems.middleware.mcp.tools.taxonomy_tools.GetUnmappedElementsTool",
    return_value=mock_unmapped,
  ):
    result = await task.execute()

  assert result["mapped"] == 0
  assert result["coverage_percent"] == 100.0
  # No AI calls made
  MockAIClient.return_value.create_message.assert_not_called()


@pytest.mark.asyncio
async def test_parse_response_valid(mock_manager):
  task = _make_task(mock_manager)
  content = json.dumps(
    [
      {
        "element_id": "e1",
        "target_id": "t1",
        "target_qname": "us-gaap:X",
        "confidence": 0.95,
      },
    ]
  )
  result = task._parse_response(content, [{"id": "e1"}])
  assert len(result) == 1
  assert result[0]["confidence"] == 0.95


@pytest.mark.asyncio
async def test_parse_response_markdown_fenced(mock_manager):
  task = _make_task(mock_manager)
  content = '```json\n[{"element_id": "e1", "target_id": "t1", "confidence": 0.9}]\n```'
  result = task._parse_response(content, [{"id": "e1"}])
  assert len(result) == 1
  assert result[0]["confidence"] == 0.9


@pytest.mark.asyncio
async def test_parse_response_invalid_json(mock_manager):
  task = _make_task(mock_manager)
  result = task._parse_response("not json at all", [{"id": "e1"}, {"id": "e2"}])
  assert len(result) == 2
  assert all(m["target_id"] is None for m in result)
  assert all(m["confidence"] == 0 for m in result)


def test_confidence_thresholds():
  assert CONFIDENCE_AUTO_APPROVE == 0.90
  assert CONFIDENCE_MIN_MAP == 0.70
