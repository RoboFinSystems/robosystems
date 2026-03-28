"""Tests for the MappingAgent task handler."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.worker.tasks.agent_mapping import (
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
def mock_api():
  api = AsyncMock()
  return api


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


UNMAPPED_ELEMENTS = [
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
]

GAAP_CANDIDATES = [
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
]


@pytest.mark.asyncio
@patch("robosystems.worker.tasks.agent_mapping.WorkerAPIClient")
@patch("robosystems.worker.tasks.agent_mapping.AIClient")
async def test_happy_path(MockAIClient, MockAPIClient, mock_manager, mock_ai_response):
  mock_api = AsyncMock()
  MockAPIClient.return_value = mock_api

  # First call: unmapped elements
  mock_api.get.side_effect = [
    UNMAPPED_ELEMENTS,  # unmapped
    GAAP_CANDIDATES,  # asset candidates
    [],  # expense candidates
    {"coverage_percent": 66.7},  # coverage
  ]
  mock_api.post.return_value = {"created": True}

  mock_ai = AsyncMock()
  MockAIClient.return_value = mock_ai
  mock_ai.create_message.return_value = mock_ai_response

  task = _make_task(mock_manager)

  with patch.object(task, "_consume_credits", new_callable=AsyncMock, return_value=0.5):
    result = await task.execute()

  assert result["mapped"] == 1  # cash (>= 0.90)
  assert result["flagged"] == 1  # AR (0.85, >= 0.70 but < 0.90)
  assert result["skipped"] >= 1  # misc (0.40, < 0.70)
  assert "coverage_percent" in result

  # Progress reported
  assert mock_manager.emit_progress.call_count >= 2


@pytest.mark.asyncio
@patch("robosystems.worker.tasks.agent_mapping.WorkerAPIClient")
@patch("robosystems.worker.tasks.agent_mapping.AIClient")
async def test_all_mapped(MockAIClient, MockAPIClient, mock_manager):
  mock_api = AsyncMock()
  MockAPIClient.return_value = mock_api
  mock_api.get.return_value = []  # no unmapped elements

  task = _make_task(mock_manager)
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
