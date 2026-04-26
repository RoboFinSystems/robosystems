"""Ledger GraphQL resolver tests.

Exercise representative resolvers via `schema.execute_sync()` with mocked
operation calls. The ops layer has its own unit tests (in
`tests/operations/roboinvestor/*` and `tests/routers/ledger/*`); these
tests cover:

1. Schema → resolver → type serialization round-trip
2. Authentication requirement (`require_user`)
3. Null propagation on `None`/missing rows
4. Graceful null on `ProgrammingError` (schema-not-initialized)
5. The recursive `AccountTreeNode` round-trip
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from sqlalchemy.exc import ProgrammingError

from robosystems.graphql.schema import schema
from robosystems.models.api.extensions.accounts import (
  AccountTreeNode as PydAccountTreeNode,
)
from robosystems.models.api.extensions.accounts import (
  AccountTreeResponse,
)
from robosystems.models.api.extensions.agent import AgentResponse
from robosystems.models.api.extensions.entity import LedgerEntityResponse

GRAPH_ID = "kg01234567890abcdef"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test"
  user.email = "test@example.com"
  return user


def _ctx(
  user: MagicMock | None = None,
  *,
  schema_extensions: tuple[str, ...] = ("roboledger",),
  graph_type: str = "entity",
) -> dict:
  """Build a fake Strawberry context.

  Tests using `schema.execute_sync(context_value=...)` bypass
  `get_context` entirely, so `graph_id` and `user` need to be passed
  in by hand. Auth + graph access are normally enforced in
  `get_context` before resolvers run; tests assume they passed.
  `schema_extensions` defaults to `("roboledger",)` so the per-domain
  feature gate (`require_extension`) passes for ledger resolvers.
  """
  return {
    "request": MagicMock(),
    "user": user or _make_user(),
    "graph_id": GRAPH_ID,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


@contextmanager
def _patch_session():
  """Patch `extensions_session` to return a mock session.

  Resolver modules import `extensions_session` inside the function
  body (local import), so the patch target is
  `robosystems.db.extensions.extensions_session`.
  """
  mock_session = MagicMock()
  mock_ctx_mgr = MagicMock()
  mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx_mgr.__exit__ = MagicMock(return_value=False)

  with patch("robosystems.db.extensions.extensions_session", return_value=mock_ctx_mgr):
    yield mock_session


class TestAuthentication:
  def test_unauthenticated_user_gets_graphql_error(self) -> None:
    result = schema.execute_sync(
      "query { entity { id name } }",
      context_value={
        "request": MagicMock(),
        "user": None,
        "graph_id": GRAPH_ID,
        "schema_extensions": (),
        "graph_type": "",
      },
    )
    assert result.errors is not None
    assert any("Authentication required" in str(e.message) for e in result.errors)
    assert result.data == {"entity": None}


class TestExtensionGate:
  """Resolver-level `require_extension` check.

  `get_context` populates `schema_extensions` from the platform DB;
  data resolvers call `require_extension(info, "roboledger")` via the
  shared `_open_session` opener. A graph without roboledger must get a
  clean `EXTENSION_NOT_PROVISIONED` error, not a schema-missing 500.
  """

  def test_entity_graph_without_roboledger_extension(self) -> None:
    result = schema.execute_sync(
      "query { entity { id name } }",
      context_value=_ctx(schema_extensions=()),
    )
    assert result.errors is not None
    assert any("roboledger is not provisioned" in str(e.message) for e in result.errors)

  def test_graph_with_only_roboinvestor_rejects_ledger_queries(self) -> None:
    result = schema.execute_sync(
      "query { entity { id name } }",
      context_value=_ctx(schema_extensions=("roboinvestor",)),
    )
    assert result.errors is not None
    assert any(
      "EXTENSION_NOT_PROVISIONED" in str((e.extensions or {}).get("code", ""))
      for e in result.errors
    )

  def test_hello_probe_still_works_without_extension(self) -> None:
    """The liveness probe must stay reachable even on graphs without
    extensions — it never calls `require_extension`."""
    user = _make_user()
    user.email = "probe@example.com"
    result = schema.execute_sync(
      "query { hello }",
      context_value=_ctx(user=user, schema_extensions=()),
    )
    assert result.errors is None
    assert result.data == {"hello": "hello, probe@example.com"}

  def test_repository_graph_with_extension_is_allowed(self) -> None:
    """Lock in the intentional asymmetry with the REST gate.

    Repository graphs (e.g. SEC) declare `schema_extensions=["roboledger"]`
    in their manifest so ledger-shaped reads work against the shared
    data. GraphQL queries on such a graph must NOT be blocked — if
    this test starts failing, someone added a `graph_type ==
    "repository"` check to `require_extension` and broke SEC access.
    See the docstring on `graphql/resolvers/_common.py:require_extension`.
    """
    mock_response = LedgerEntityResponse(
      id="ent_sec",
      name="SEC EDGAR",
      status="active",
      is_parent=True,
      source="native",
    )
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        return_value=mock_response,
      ),
    ):
      result = schema.execute_sync(
        "query { entity { id name } }",
        context_value=_ctx(graph_type="repository"),
      )
    assert result.errors is None
    assert result.data is not None
    assert result.data["entity"]["id"] == "ent_sec"


class TestEntityResolver:
  def test_returns_entity_when_found(self) -> None:
    mock_response = LedgerEntityResponse(
      id="ent_01",
      name="Acme Corp",
      legal_name="Acme Corporation LLC",
      status="active",
      is_parent=True,
      source="native",
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        return_value=mock_response,
      ),
    ):
      result = schema.execute_sync(
        "query { entity { id name legalName status source } }",
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data is not None
    assert result.data["entity"]["id"] == "ent_01"
    assert result.data["entity"]["name"] == "Acme Corp"
    assert result.data["entity"]["legalName"] == "Acme Corporation LLC"

  def test_returns_null_when_entity_missing(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        return_value=None,
      ),
    ):
      result = schema.execute_sync(
        "query { entity { id name } }",
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"entity": None}

  def test_raises_typed_error_when_schema_not_initialized(self) -> None:
    """Schema-missing errors must surface as a typed GraphQL error.

    Regression: an earlier version returned `{entity: null}` here,
    making "ledger not initialized" indistinguishable from "no
    parent entity row exists yet" on the client. The new contract
    raises `LEDGER_NOT_INITIALIZED` so frontends and agents can
    branch on the code.
    """
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
      ),
    ):
      result = schema.execute_sync(
        "query { entity { id name } }",
        context_value=_ctx(),
      )

    assert result.errors is not None
    err = result.errors[0]
    assert "Ledger not initialized" in err.message
    assert err.extensions == {"code": "LEDGER_NOT_INITIALIZED"}


class TestEntitiesResolver:
  def test_lists_multiple_entities(self) -> None:
    entities = [
      LedgerEntityResponse(id=f"ent_{i}", name=f"Entity {i}", status="active")
      for i in range(3)
    ]

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.list_entities",
        return_value=entities,
      ),
    ):
      result = schema.execute_sync(
        "query { entities { id name } }",
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data is not None
    assert len(result.data["entities"]) == 3
    assert result.data["entities"][0]["id"] == "ent_0"

  def test_raises_typed_error_on_schema_error_for_list(self) -> None:
    """List resolvers also surface schema errors instead of returning []."""
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.entity.list_entities",
        side_effect=ValueError("bad graph_id"),
      ),
    ):
      result = schema.execute_sync(
        "query { entities { id } }",
        context_value=_ctx(),
      )

    assert result.errors is not None
    err = result.errors[0]
    assert err.extensions == {"code": "LEDGER_NOT_INITIALIZED"}


class TestAgentResolvers:
  def _agent(self) -> AgentResponse:
    return AgentResponse(
      id="agt_01",
      agent_type="vendor",
      name="Vendor Co",
      source="native",
      external_id="ven_123",
      is_active=True,
      is_1099_recipient=False,
    )

  def test_returns_agent_when_found(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.agent.get_agent",
        return_value=self._agent(),
      ),
    ):
      result = schema.execute_sync(
        'query { agent(id: "agt_01") { id name agentType source externalId } }',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {
      "agent": {
        "id": "agt_01",
        "name": "Vendor Co",
        "agentType": "vendor",
        "source": "native",
        "externalId": "ven_123",
      }
    }

  def test_agent_raises_typed_error_when_schema_not_initialized(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.agent.get_agent",
        side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
      ),
    ):
      result = schema.execute_sync(
        'query { agent(id: "agt_01") { id } }',
        context_value=_ctx(),
      )

    assert result.errors is not None
    assert result.errors[0].extensions == {"code": "LEDGER_NOT_INITIALIZED"}

  def test_agents_raises_typed_error_when_schema_not_initialized(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.agent.list_agents",
        side_effect=ValueError("bad graph_id"),
      ),
    ):
      result = schema.execute_sync(
        "query { agents { id } }",
        context_value=_ctx(),
      )

    assert result.errors is not None
    assert result.errors[0].extensions == {"code": "LEDGER_NOT_INITIALIZED"}


class TestAccountTreeResolver:
  """The recursive `AccountTreeNode` round-trip is the highest-risk part
  of the schema because the Pydantic decorator can't synthesize it."""

  def _tree(self) -> AccountTreeResponse:
    # Build a 3-level tree: Assets → Current Assets → Cash
    cash = PydAccountTreeNode(
      id="e_cash",
      code="1000",
      name="Cash",
      classification="asset",
      balance_type="debit",
      depth=3,
      is_active=True,
      children=[],
    )
    current_assets = PydAccountTreeNode(
      id="e_current",
      code="100",
      name="Current Assets",
      classification="asset",
      balance_type="debit",
      depth=2,
      is_active=True,
      children=[cash],
    )
    assets = PydAccountTreeNode(
      id="e_assets",
      code="1",
      name="Assets",
      classification="asset",
      balance_type="debit",
      depth=1,
      is_active=True,
      children=[current_assets],
    )
    return AccountTreeResponse(roots=[assets], total_accounts=3)

  def test_three_level_tree_serializes(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.accounts.get_account_tree",
        return_value=self._tree(),
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          accountTree {
            totalAccounts
            roots {
              id
              name
              children {
                id
                name
                children {
                  id
                  name
                  children { id }
                }
              }
            }
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None, result.errors
    assert result.data is not None
    tree = result.data["accountTree"]
    assert tree["totalAccounts"] == 3
    assert tree["roots"][0]["name"] == "Assets"
    assert tree["roots"][0]["children"][0]["name"] == "Current Assets"
    assert tree["roots"][0]["children"][0]["children"][0]["name"] == "Cash"
    # Leaf has empty children, not null
    assert tree["roots"][0]["children"][0]["children"][0]["children"] == []

  def test_empty_tree(self) -> None:
    empty = AccountTreeResponse(roots=[], total_accounts=0)
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.accounts.get_account_tree",
        return_value=empty,
      ),
    ):
      result = schema.execute_sync(
        "query { accountTree { totalAccounts roots { id } } }",
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"accountTree": {"totalAccounts": 0, "roots": []}}


class TestSummaryResolver:
  def test_builds_summary_from_counts(self) -> None:
    from datetime import date

    from robosystems.operations.roboledger.reads.summary import LedgerCounts

    counts = LedgerCounts(
      account_count=42,
      transaction_count=100,
      entry_count=200,
      line_item_count=400,
      earliest_transaction_date=date(2024, 1, 1),
      latest_transaction_date=date(2024, 12, 31),
    )

    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.summary.get_ledger_counts",
        return_value=counts,
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          summary {
            graphId
            accountCount
            transactionCount
            entryCount
            lineItemCount
            earliestTransactionDate
            latestTransactionDate
          }
        }
        """,
        context_value=_ctx(),
      )

    assert result.errors is None
    summary = result.data["summary"]
    assert summary["graphId"] == GRAPH_ID
    assert summary["accountCount"] == 42
    assert summary["transactionCount"] == 100
    assert summary["earliestTransactionDate"] == "2024-01-01"


class TestHelloResolver:
  def test_returns_greeting_with_email(self) -> None:
    user = _make_user()
    user.email = "alice@example.com"
    result = schema.execute_sync(
      "query { hello }",
      context_value={
        "request": MagicMock(),
        "user": user,
        "graph_id": GRAPH_ID,
        "schema_extensions": ("roboledger",),
        "graph_type": "entity",
      },
    )
    assert result.errors is None
    assert result.data == {"hello": "hello, alice@example.com"}


class TestPaginationBounds:
  """Mirror the retired REST `Query(..., ge=N, le=M)` validation.

  GraphQL doesn't have native scalar bounds, so resolvers call
  `_validate_pagination(limit, offset)` and raise a clean GraphQL
  error rather than passing unbounded values through to SQL.
  """

  def test_limit_too_small_rejected(self) -> None:
    result = schema.execute_sync(
      "query { accounts(limit: 0) { pagination { total } } }",
      context_value=_ctx(),
    )
    assert result.errors is not None
    assert any("limit must be between" in str(e.message) for e in result.errors)

  def test_limit_too_large_rejected(self) -> None:
    result = schema.execute_sync(
      "query { accounts(limit: 5000) { pagination { total } } }",
      context_value=_ctx(),
    )
    assert result.errors is not None
    assert any("limit must be between" in str(e.message) for e in result.errors)

  def test_negative_offset_rejected(self) -> None:
    result = schema.execute_sync(
      "query { accounts(offset: -1) { pagination { total } } }",
      context_value=_ctx(),
    )
    assert result.errors is not None
    assert any("offset must be" in str(e.message) for e in result.errors)

  def test_bounds_apply_to_transactions_too(self) -> None:
    result = schema.execute_sync(
      "query { transactions(limit: 99999) { pagination { total } } }",
      context_value=_ctx(),
    )
    assert result.errors is not None
    assert any("limit must be between" in str(e.message) for e in result.errors)

  def test_bounds_apply_to_agents_too(self) -> None:
    result = schema.execute_sync(
      "query { agents(limit: 0) { id } }",
      context_value=_ctx(),
    )
    assert result.errors is not None
    assert any("limit must be between" in str(e.message) for e in result.errors)


class TestReportPackageResolver:
  """GraphQL ``reportPackage(reportId)`` field — Plan-C package mode read."""

  def _make_envelope(self):
    """Minimal ReportPackageEnvelope for resolver projection tests."""
    from datetime import UTC
    from datetime import date as _date
    from datetime import datetime as _dt

    from robosystems.models.api.extensions.report_package import (
      ReportPackageEnvelope,
    )

    return ReportPackageEnvelope(
      id="rpt_01",
      name="Q1 2026 Statements",
      description=None,
      taxonomy_id="tax_usgaap_reporting",
      period_type="quarterly",
      period_start=_date(2026, 1, 1),
      period_end=_date(2026, 3, 31),
      generation_status="complete",
      last_generated=_dt(2026, 4, 1, tzinfo=UTC),
      filing_status="filed",
      filed_at=_dt(2026, 4, 15, tzinfo=UTC),
      filed_by="usr_01",
      supersedes_id=None,
      superseded_by_id=None,
      source_graph_id=None,
      source_report_id=None,
      shared_at=None,
      entity_name="Acme LLC",
      ai_generated=False,
      created_at=_dt(2026, 4, 1, tzinfo=UTC),
      created_by="usr_01",
      items=[],
    )

  def test_returns_package_when_found(self) -> None:
    envelope = self._make_envelope()
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.reports.get_report_package",
        return_value=envelope,
      ),
    ):
      result = schema.execute_sync(
        """
        query Q($id: String!) {
          reportPackage(reportId: $id) {
            id name filingStatus filedBy entityName items { factSetId }
          }
        }
        """,
        variable_values={"id": "rpt_01"},
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data is not None
    pkg = result.data["reportPackage"]
    assert pkg["id"] == "rpt_01"
    assert pkg["filingStatus"] == "filed"
    assert pkg["filedBy"] == "usr_01"
    assert pkg["entityName"] == "Acme LLC"
    assert pkg["items"] == []

  def test_returns_null_when_report_missing(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.reports.get_report_package",
        return_value=None,
      ),
    ):
      result = schema.execute_sync(
        "query Q($id: String!) { reportPackage(reportId: $id) { id } }",
        variable_values={"id": "rpt_missing"},
        context_value=_ctx(),
      )
    assert result.errors is None
    assert result.data == {"reportPackage": None}

  def test_raises_typed_error_when_schema_not_initialized(self) -> None:
    with (
      _patch_session(),
      patch(
        "robosystems.operations.roboledger.reads.reports.get_report_package",
        side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
      ),
    ):
      result = schema.execute_sync(
        "query Q($id: String!) { reportPackage(reportId: $id) { id } }",
        variable_values={"id": "rpt_01"},
        context_value=_ctx(),
      )

    assert result.errors is not None
    err = result.errors[0]
    assert "Ledger not initialized" in err.message
    assert err.extensions == {"code": "LEDGER_NOT_INITIALIZED"}
