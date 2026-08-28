"""Tests for the build-fact-grid roboledger operation.

Covers the analytical view operation registered in
`routers/extensions/roboledger/views.py`. This is the only
read-shaped operation in the dispatcher — it queries the LadybugDB
graph (XBRL hypercube schema) and returns deduplicated facts plus the
aspects they span, wrapped in an `OperationEnvelope`.

The route lives in its own module (separate from `operations.py`) so
it can be mounted independently of `ROBOLEDGER_ENABLED` — SEC-only
deployments need fact-grid without enabling roboledger tenants. The
`TestFactGridFlagDecoupling` class below pins that behavior.

Mocks the underlying graph query (`query_fact_grid`) but runs the real
`FactGridBuilder` over fact records shaped exactly as the query returns
them — the response shape is the contract under test, and mocking the
builder is what previously let a column-name mismatch between query and
builder go unnoticed. The Cypher has its own coverage in the ops layer.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.models.api.extensions.reports import (
  FinancialStatementAnalysisRequest,
)
from robosystems.models.api.views import CreateViewRequest
from robosystems.models.api.views.view_config import DEFAULT_FACT_LIMIT
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
  limit=DEFAULT_FACT_LIMIT,
):
  """Build a real CreateViewRequest matching the route signature."""
  return CreateViewRequest(
    elements=elements if elements is not None else ["us-gaap:Assets"],
    canonical_concepts=canonical_concepts if canonical_concepts is not None else [],
    periods=periods or [],
    period_type=period_type,
    entity=entity,
    entities=entities or [],
    include_summary=include_summary,
    limit=limit,
  )


def _make_user(user_id: str = "usr_test"):
  user = MagicMock()
  user.id = user_id
  return user


def _make_facts(count: int = 5):
  """Fact records shaped exactly as `query_fact_grid` returns them."""
  return [
    {
      "element_id": "us-gaap:Assets",
      "element_name": "Assets",
      "period_end": f"202{i}-12-31",
      "value": 1000.0 * (i + 1),
      "unit": "USD",
    }
    for i in range(count)
  ]


class _FakeCache:
  """Minimal idempotency cache stub that stores nothing."""

  async def reserve(self, *args, **kwargs):
    return True

  async def release(self, *args, **kwargs):
    return None

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
    facts = _make_facts(count=3)
    body = _make_create_view_request(elements=["us-gaap:Assets"], period_type="instant")

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, False),
    ) as mock_query:
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
    assert envelope.result["metadata"]["facts_processed"] == 3
    assert len(envelope.result["facts"]) == 3
    assert envelope.result["facts"][0]["element_id"] == "us-gaap:Assets"
    assert envelope.result["facts"][0]["value"] == 1000.0
    assert envelope.result["summary"] is None
    mock_query.assert_called_once()
    call_kwargs = mock_query.call_args.kwargs
    assert call_kwargs["graph_id"] == GRAPH_ID
    assert call_kwargs["elements"] == ["us-gaap:Assets"]
    assert call_kwargs["period_type"] == "instant"

  @pytest.mark.unit
  async def test_facts_are_returned_unaggregated(self):
    """Two filers reporting the same element+period stay two records."""
    facts = [
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2024-12-31",
        "value": 1000.0,
        "unit": "USD",
        "entity_ticker": "AAPL",
        "entity_name": "Apple Inc.",
      },
      {
        "element_id": "us-gaap:Assets",
        "element_name": "Assets",
        "period_end": "2024-12-31",
        "value": 2000.0,
        "unit": "USD",
        "entity_ticker": "MSFT",
        "entity_name": "MICROSOFT CORP",
      },
    ]
    body = _make_create_view_request(entities=["AAPL", "MSFT"])

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, False),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    result = envelope.result
    assert result is not None
    assert len(result["facts"]) == 2
    assert {f["value"] for f in result["facts"]} == {1000.0, 2000.0}
    entity_dim = next(d for d in result["dimensions"] if d["type"] == "entity")
    assert entity_dim["members"] == ["AAPL", "MSFT"]

  @pytest.mark.unit
  async def test_entity_filter_passed_through(self):
    """Single-entity ticker filter threads through to query_fact_grid."""
    body = _make_create_view_request(entity="NVDA")

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(_make_facts(), False),
    ) as mock_query:
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
    """include_summary=True adds element-keyed aggregates, keyed on qname."""
    facts = [
      {
        "element_id": "us-gaap:Revenues",
        "element_name": "Revenues",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "value": 1000.0,
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:Revenues",
        "element_name": "Revenues",
        "period_start": "2023-01-01",
        "period_end": "2023-12-31",
        "value": 2000.0,
        "unit": "USD",
      },
      {
        "element_id": "us-gaap:CostOfRevenue",
        "element_name": "CostOfRevenue",
        "period_start": "2024-01-01",
        "period_end": "2024-12-31",
        "value": 500.0,
        "unit": "USD",
      },
    ]
    body = _make_create_view_request(include_summary=True)

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(facts, False),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    summary = envelope.result["summary"]  # type: ignore[index]
    assert summary["us-gaap:Revenues"]["count"] == 2
    assert summary["us-gaap:Revenues"]["total"] == 3000.0
    assert summary["us-gaap:CostOfRevenue"]["total"] == 500.0

  @pytest.mark.unit
  async def test_shared_repo_without_entity_raises_400(self):
    """Shared repos host thousands of filers; an unscoped query there returns
    an arbitrary slice of arbitrary companies."""
    body = _make_create_view_request()

    with patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True):
      with pytest.raises(HTTPException) as exc_info:
        await build_fact_grid_op(
          body=body,
          graph_id="sec",
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc_info.value.status_code == 400
    assert "entity" in exc_info.value.detail.lower()

  @pytest.mark.unit
  async def test_shared_repo_with_entity_allowed(self):
    body = _make_create_view_request(entity="NVDA")

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=(_make_facts(), False),
      ),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id="sec",
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.status == "completed"

  @pytest.mark.unit
  async def test_tenant_graph_without_entity_allowed(self):
    """A tenant graph is already entity-scoped by its URL, and its entity is
    often a private company with no ticker or CIK to filter on."""
    body = _make_create_view_request()

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=False),
      patch(
        f"{MODULE}.query_fact_grid",
        new_callable=AsyncMock,
        return_value=(_make_facts(), False),
      ),
    ):
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.status == "completed"

  @pytest.mark.unit
  async def test_limit_threads_through_and_truncation_surfaces(self):
    body = _make_create_view_request(limit=2)

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      return_value=(_make_facts(count=2), True),
    ) as mock_query:
      envelope = await build_fact_grid_op(
        body=body,
        graph_id=GRAPH_ID,
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert mock_query.call_args.kwargs["limit"] == 2
    assert envelope.result["metadata"]["truncated"] is True  # type: ignore[index]

  @pytest.mark.unit
  async def test_internal_error_audited_and_propagated(self):
    """Any error from the underlying query bubbles up through the dispatcher."""
    body = _make_create_view_request()

    with patch(
      f"{MODULE}.query_fact_grid",
      new_callable=AsyncMock,
      side_effect=RuntimeError("connection lost"),
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
  async def test_unresolvable_fiscal_year_404s_instead_of_answering(self):
    """A requested fiscal_year that matches no filing must 404.

    Previously the handler fell through to the ticker path, which sweeps
    the filer's entire history ordered by end_date DESC and never receives
    fiscal_year — so a FY2005 request came back with FY2026 numbers,
    report_id null, no error and no warning. Verified live against the SEC
    repo before the fix.
    """
    body = FinancialStatementAnalysisRequest(
      statement_type="income_statement",
      ticker="NVDA",
      fiscal_year=2005,
      period_type="annual",
    )

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
        return_value=None,
      ),
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=[],
      ) as mock_query,
    ):
      with pytest.raises(HTTPException) as exc:
        await financial_statement_analysis_op(
          body=body,
          graph_id="sec",
          user=_make_user(),
          idempotency_key=None,
          cache=_FakeCache(),
        )

    assert exc.value.status_code == 404
    assert "2005" in str(exc.value.detail)
    # The unscoped sweep must not run at all — that is the defect.
    mock_query.assert_not_called()

  @pytest.mark.unit
  async def test_no_fiscal_year_still_autoresolves_latest(self):
    """Without a fiscal_year there is no scope to violate: the documented
    'auto-resolve the latest filing' behaviour is preserved."""
    body = FinancialStatementAnalysisRequest(
      statement_type="income_statement",
      ticker="NVDA",
    )

    with (
      patch(f"{MODULE}.is_shared_repository_or_subgraph", return_value=True),
      patch(
        "robosystems.adapters.sec.mcp.resolve_sec_report",
        new_callable=AsyncMock,
        return_value=None,
      ),
      patch(
        f"{MODULE}.query_financial_statement",
        new_callable=AsyncMock,
        return_value=[],
      ) as mock_query,
    ):
      envelope = await financial_statement_analysis_op(
        body=body,
        graph_id="sec",
        user=_make_user(),
        idempotency_key=None,
        cache=_FakeCache(),
      )

    assert envelope.status == "completed"
    mock_query.assert_called_once()

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
