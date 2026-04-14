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
from robosystems.models.api.extensions.entity import LedgerEntityResponse

GRAPH_ID = "kg01234567890abcdef"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test"
  user.email = "test@example.com"
  return user


def _ctx(user: MagicMock | None = None) -> dict:
  return {"request": MagicMock(), "user": user or _make_user()}


@contextmanager
def _patch_session_for(module_name: str):
  """Patch `extensions_session` inside a resolver module to return a mock session.

  The resolver modules import `extensions_session` inside the function
  body (local import), so the patch target is
  `robosystems.db.extensions.extensions_session`.
  """
  mock_session = MagicMock()
  mock_ctx_mgr = MagicMock()
  mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx_mgr.__exit__ = MagicMock(return_value=False)

  with (
    patch("robosystems.db.extensions.extensions_session", return_value=mock_ctx_mgr),
    patch(
      f"robosystems.graphql.resolvers.{module_name}.check_graph_access",
      return_value=None,
    ),
  ):
    yield mock_session


class TestAuthentication:
  def test_unauthenticated_user_gets_graphql_error(self) -> None:
    result = schema.execute_sync(
      'query { entity(graphId: "kg_x") { id name } }',
      context_value={"request": MagicMock(), "user": None},
    )
    assert result.errors is not None
    assert any("Authentication required" in str(e.message) for e in result.errors)
    assert result.data == {"entity": None}


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
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        return_value=mock_response,
      ),
    ):
      result = schema.execute_sync(
        f'query {{ entity(graphId: "{GRAPH_ID}") {{ id name legalName status source }} }}',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data is not None
    assert result.data["entity"]["id"] == "ent_01"
    assert result.data["entity"]["name"] == "Acme Corp"
    assert result.data["entity"]["legalName"] == "Acme Corporation LLC"

  def test_returns_null_when_entity_missing(self) -> None:
    with (
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        return_value=None,
      ),
    ):
      result = schema.execute_sync(
        f'query {{ entity(graphId: "{GRAPH_ID}") {{ id name }} }}',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"entity": None}

  def test_returns_null_when_schema_not_initialized(self) -> None:
    with (
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.entity.get_parent_entity",
        side_effect=ProgrammingError("stmt", {}, Exception("schema missing")),
      ),
    ):
      result = schema.execute_sync(
        f'query {{ entity(graphId: "{GRAPH_ID}") {{ id name }} }}',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"entity": None}


class TestEntitiesResolver:
  def test_lists_multiple_entities(self) -> None:
    entities = [
      LedgerEntityResponse(id=f"ent_{i}", name=f"Entity {i}", status="active")
      for i in range(3)
    ]

    with (
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.entity.list_entities",
        return_value=entities,
      ),
    ):
      result = schema.execute_sync(
        f'query {{ entities(graphId: "{GRAPH_ID}") {{ id name }} }}',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data is not None
    assert len(result.data["entities"]) == 3
    assert result.data["entities"][0]["id"] == "ent_0"

  def test_empty_list_on_schema_error(self) -> None:
    with (
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.entity.list_entities",
        side_effect=ValueError("bad graph_id"),
      ),
    ):
      result = schema.execute_sync(
        f'query {{ entities(graphId: "{GRAPH_ID}") {{ id }} }}',
        context_value=_ctx(),
      )

    assert result.errors is None
    assert result.data == {"entities": []}


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
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.accounts.get_account_tree",
        return_value=self._tree(),
      ),
    ):
      result = schema.execute_sync(
        """
        query {
          accountTree(graphId: "kg_x") {
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
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.accounts.get_account_tree",
        return_value=empty,
      ),
    ):
      result = schema.execute_sync(
        'query { accountTree(graphId: "kg_x") { totalAccounts roots { id } } }',
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
      _patch_session_for("ledger"),
      patch(
        "robosystems.operations.roboledger.reads.summary.get_ledger_counts",
        return_value=counts,
      ),
    ):
      result = schema.execute_sync(
        f"""
        query {{
          summary(graphId: "{GRAPH_ID}") {{
            graphId
            accountCount
            transactionCount
            entryCount
            lineItemCount
            earliestTransactionDate
            latestTransactionDate
          }}
        }}
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
      "query { hello }", context_value={"request": MagicMock(), "user": user}
    )
    assert result.errors is None
    assert result.data == {"hello": "hello, alice@example.com"}
