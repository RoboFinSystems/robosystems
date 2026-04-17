"""Tests for the build-fact-grid roboledger operation.

Covers the analytical view operation registered in
`routers/extensions/roboledger/views.py`. This is the only
read-shaped operation in the dispatcher — it queries the LadybugDB
graph (XBRL hypercube schema) and returns a multi-dimensional pivot
table wrapped in an `OperationEnvelope`.

The route lives in its own module (separate from `operations.py`) so
it can be mounted independently of `ROBOLEDGER_ENABLED` — SEC-only
deployments need fact-grid without enabling roboledger tenants. The
`TestFactGridFlagDecoupling` class below pins that behavior.

Mocks the underlying graph query (`query_fact_grid`) and the
`FactGridBuilder` so the tests stay focused on the operation route's
contract: arg threading, validation, envelope wrapping, and error
translation. The Cypher and pandas pivot logic have their own unit
coverage in the ops layer.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.reports import (
  FinancialStatementAnalysisRequest,
)
from robosystems.routers.extensions.roboledger.views import (
  build_fact_grid_op,
  financial_statement_analysis_op,
)

MODULE = "robosystems.routers.extensions.roboledger.views"
GRAPH_ID = "kg01234567890abcdef"


def _make_create_view_request(
  elements=None,
  canonical_concepts=None,
  periods=None,
  period_type="annual",
  entity=None,
  entities=None,
  include_summary=False,
):
  """Build a duck-typed CreateViewRequest matching the route signature."""
  mock_request = MagicMock()
  mock_request.elements = elements if elements is not None else ["us-gaap:Assets"]
  mock_request.canonical_concepts = (
    canonical_concepts if canonical_concepts is not None else []
  )
  mock_request.periods = periods or []
  mock_request.period_type = period_type
  mock_request.entity = entity
  mock_request.entities = entities or []
  mock_request.form = None
  mock_request.fiscal_year = None
  mock_request.fiscal_period = None
  mock_request.include_summary = include_summary
  mock_request.view_config = MagicMock()
  return mock_request


def _make_user(user_id: str = "usr_test"):
  user = MagicMock()
  user.id = user_id
  return user


def _make_mock_grid(fact_count: int = 5):
  grid = MagicMock()
  grid.metadata.fact_count = fact_count
  grid.facts_df = None
  return grid


class _FakeCache:
  """Minimal idempotency cache stub that stores nothing."""

  async def get(self, *args, **kwargs):
    return None

  async def put(self, *args, **kwargs):
    return None


@pytest.mark.asyncio
class TestBuildFactGridOperation:
  """Wire-shape and dispatcher contract for build-fact-grid."""

  @pytest.mark.unit
  async def test_happy_path_wraps_in_envelope(self):
    """The operation runs query_fact_grid + FactGridBuilder and wraps result."""
    mock_grid = _make_mock_grid(fact_count=42)
    mock_builder = MagicMock()
    mock_builder.build.return_value = mock_grid
    mock_builder.generate_pivot_table.return_value = {
      "index": [["Assets"]],
      "columns": [["2026-01-31"]],
      "data": [[1000.0]],
      "metadata": {"row_count": 1, "column_count": 1},
    }

    body = _make_create_view_request(elements=["us-gaap:Assets"], period_type="instant")

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=MagicMock(),
      ) as mock_query,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "build-fact-grid"
    assert envelope.operation_id.startswith("op_")
    assert envelope.status == "completed"
    assert envelope.result is not None
    # ViewResponse wraps {metadata, presentations}
    assert "metadata" in envelope.result
    assert "presentations" in envelope.result
    assert envelope.result["metadata"]["facts_processed"] == 42
    assert "pivot_table" in envelope.result["presentations"]
    mock_query.assert_called_once()
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["graph_id"] == GRAPH_ID
    assert call_kwargs["elements"] == ["us-gaap:Assets"]
    assert call_kwargs["period_type"] == "instant"

  @pytest.mark.unit
  async def test_entity_filter_passed_through(self):
    """Single-entity ticker filter threads through to query_fact_grid."""
    mock_builder = MagicMock()
    mock_builder.build.return_value = _make_mock_grid()
    mock_builder.generate_pivot_table.return_value = {
      "index": [],
      "columns": [],
      "data": [],
      "metadata": {},
    }

    body = _make_create_view_request(entity="NVDA")

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=MagicMock(),
      ) as mock_query,
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder),
    ):
      await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert mock_query.call_args.kwargs["entity"] == "NVDA"

  @pytest.mark.unit
  async def test_missing_elements_raises_400(self):
    """Body with neither elements nor canonical_concepts is rejected."""
    body = _make_create_view_request(elements=[], canonical_concepts=[])

    with pytest.raises(HTTPException) as exc_info:
      await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert exc_info.value.status_code == 400
    assert "elements" in exc_info.value.detail

  @pytest.mark.unit
  async def test_missing_period_scope_raises_400(self):
    """Body without periods, period_type, or fiscal_year is rejected."""
    body = _make_create_view_request(period_type=None)
    body.fiscal_year = None

    with pytest.raises(HTTPException) as exc_info:
      await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert exc_info.value.status_code == 400
    assert "period" in exc_info.value.detail.lower()

  @pytest.mark.unit
  async def test_include_summary_adds_per_element_stats(self):
    """include_summary=True adds element-keyed aggregates to presentations."""
    mock_grid = _make_mock_grid(fact_count=3)
    mock_grid.facts_df = pd.DataFrame(
      {
        "element_name": ["Revenue", "Revenue", "Cost of Revenue"],
        "value": [1000.0, 2000.0, 500.0],
      }
    )

    mock_builder = MagicMock()
    mock_builder.build.return_value = mock_grid
    mock_builder.generate_pivot_table.return_value = {
      "index": [],
      "columns": [],
      "data": [],
      "metadata": {},
    }

    body = _make_create_view_request(include_summary=True)

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=MagicMock(),
      ),
      patch(f"{MODULE}.FactGridBuilder", return_value=mock_builder),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    presentations = envelope.result["presentations"]  # type: ignore[index]
    assert "summary" in presentations
    assert presentations["summary"]["Revenue"]["count"] == 2
    assert presentations["summary"]["Revenue"]["total"] == 3000.0
    assert presentations["summary"]["Cost of Revenue"]["total"] == 500.0

  @pytest.mark.unit
  async def test_internal_error_audited_and_propagated(self):
    """Any error from the underlying query bubbles up through the dispatcher."""
    body = _make_create_view_request()

    with (
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        side_effect=RuntimeError("connection lost"),
      ),
      patch(f"{MODULE}.FactGridBuilder"),
    ):
      with pytest.raises(RuntimeError):
        await build_fact_grid_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )


@pytest.mark.asyncio
class TestFinancialStatementAnalysisOp:
  """Wire-shape contract for financial-statement-analysis."""

  @pytest.mark.unit
  async def test_shared_repo_ticker_autoresolves_report(self):
    """On SEC, missing report_id triggers resolve_sec_report; facts flow through."""
    body = FinancialStatementAnalysisRequest(
      statement_type="income_statement",
      ticker="NVDA",
      period_type="annual",
    )
    rows = [
      {
        "canonical_concept": "revenue",
        "qname": "us-gaap:Revenues",
        "name": "Revenues",
        "value": 100.0,
        "end_date": "2025-01-31",
        "period_type": "duration",
        "duration_type": "annual",
      }
    ]

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
        return_value={
          "identifier": "rpt_latest",
          "form": "10-K",
          "filing_date": "2025-06-30",
          "fiscal_year": 2025,
          "fiscal_period": "FY",
        },
      ) as mock_resolve,
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=rows,
      ) as mock_query,
    ):
      envelope = await financial_statement_analysis_op(
        body=body,
        graph_id="sec",
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.operation == "financial-statement-analysis"
    assert envelope.status == "completed"
    result = envelope.result
    assert result is not None
    assert result["report_id"] == "rpt_latest"
    assert result["ticker"] == "NVDA"
    assert result["fact_count"] == 1
    assert result["resolved_report"]["form"] == "10-K"
    mock_resolve.assert_awaited_once()
    mock_query.assert_awaited_once()

  @pytest.mark.unit
  async def test_tenant_graph_with_report_id_skips_resolver(self):
    """Tenant graphs must pass report_id; resolver must not be called."""
    body = FinancialStatementAnalysisRequest(
      statement_type="income_statement",
      report_id="rpt_tenant",
    )

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False),
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=[],
      ) as mock_query,
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
      ) as mock_resolve,
    ):
      envelope = await financial_statement_analysis_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.status == "completed"
    assert envelope.result["report_id"] == "rpt_tenant"  # type: ignore[index]
    mock_resolve.assert_not_called()
    mock_query.assert_awaited_once()

  @pytest.mark.unit
  async def test_shared_repo_missing_ticker_and_report_id_raises_400(self):
    body = FinancialStatementAnalysisRequest(statement_type="income_statement")
    with patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True):
      with pytest.raises(HTTPException) as exc_info:
        await financial_statement_analysis_op(
          body=body,
          graph_id="sec",
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc_info.value.status_code == 400
    assert "ticker" in exc_info.value.detail.lower()

  @pytest.mark.unit
  async def test_tenant_missing_report_id_raises_400(self):
    body = FinancialStatementAnalysisRequest(
      statement_type="income_statement", ticker="ABC"
    )
    with patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False):
      with pytest.raises(HTTPException) as exc_info:
        await financial_statement_analysis_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc_info.value.status_code == 400
    assert "report_id" in exc_info.value.detail.lower()

  @pytest.mark.unit
  async def test_invalid_statement_type_raises_400(self):
    body = FinancialStatementAnalysisRequest(
      statement_type="not_a_statement", report_id="rpt_x"
    )
    with patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False):
      with pytest.raises(HTTPException) as exc_info:
        await financial_statement_analysis_op(
          body=body,
          graph_id=GRAPH_ID,
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )
    assert exc_info.value.status_code == 400


class TestFactGridFlagDecoupling:
  """Regression: build-fact-grid must NOT depend on ROBOLEDGER_ENABLED.

  The legacy `/v1/graphs/{g}/views` endpoint mounted on
  `FACT_GRID_ENABLED` alone — SEC-only deployments (no roboledger
  tenants) used it for cross-entity public-company analysis. Earlier
  this branch accidentally bundled `build_fact_grid_op` into the same
  router as the ledger commands, so it was gated by
  `ROBOLEDGER_ENABLED AND FACT_GRID_ENABLED`. That broke SEC-only
  deployments.

  These tests pin the contract: the fact-grid router is a separate
  router file imported from `views.py`, mounted in `main.py` only on
  `FACT_GRID_ENABLED`, and shares the operations dispatcher contract
  (envelope, idempotency, audit) without requiring ledger tenants.
  """

  def test_views_router_is_separate_module(self) -> None:
    """The fact-grid route lives in views.py, not operations.py."""
    from robosystems.routers.extensions.roboledger import operations, views

    # Both modules expose a router
    assert hasattr(views, "router")
    assert hasattr(operations, "router")
    # views.router has the build-fact-grid route
    views_paths = {r.path for r in views.router.routes if hasattr(r, "path")}
    assert "/build-fact-grid" in views_paths
    # operations.router does NOT (it was moved out)
    ops_paths = {r.path for r in operations.router.routes if hasattr(r, "path")}
    assert "/build-fact-grid" not in ops_paths

  def test_main_py_mounts_views_on_fact_grid_flag_only(self) -> None:
    """main.py mounts the views router gated on FACT_GRID_ENABLED, not ROBOLEDGER_ENABLED.

    Reads main.py source as text to verify the mount block is gated
    on `env.FACT_GRID_ENABLED` independently of `env.ROBOLEDGER_ENABLED`.
    Derives the path from this test file's location rather than
    hardcoding it, so it works in CI runners and developer machines.
    """
    from pathlib import Path as _Path

    # this test file lives at <repo>/tests/routers/extensions/roboledger/test_views.py
    # → main.py is parents[4] up
    main_py = _Path(__file__).resolve().parents[4] / "main.py"
    main_src = main_py.read_text()
    # Find the views router import + mount section
    assert "roboledger_views_router" in main_src
    # The block must be gated on FACT_GRID_ENABLED, not on ROBOLEDGER_ENABLED
    views_block_start = main_src.find("if env.FACT_GRID_ENABLED:")
    views_import = main_src.find(
      "from robosystems.routers.extensions.roboledger.views import"
    )
    assert views_block_start >= 0
    assert views_import > views_block_start
    # And it must NOT be inside the ROBOLEDGER_ENABLED block — verify by
    # checking that the import line comes AFTER the ROBOLEDGER_ENABLED block
    # (which ends with the operations router include).
    ledger_block_start = main_src.find("if env.ROBOLEDGER_ENABLED:")
    assert ledger_block_start >= 0
    assert ledger_block_start < views_block_start, (
      "FACT_GRID_ENABLED block must come AFTER (and outside) the "
      "ROBOLEDGER_ENABLED block, otherwise SEC-only deployments lose fact-grid"
    )
