"""Taxonomy Block GraphQL resolver tests.

Exercises ``taxonomyBlock`` / ``taxonomyBlocks`` via
``schema.execute_sync()`` with mocked reads operations. The resolver is
always-on (no extension gate) and uses ``open_library_session``, so the
session patch target is ``robosystems.db.extensions.extensions_session``.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from robosystems.graphql.schema import schema
from robosystems.models.api.taxonomy_block import (
  TaxonomyBlockElement,
  TaxonomyBlockEnvelope,
)

GRAPH_ID = "kg01234567890abcdef"
LIBRARY_GRAPH_ID = "library"

GET_PATH = "robosystems.graphql.resolvers.taxonomy_block.get_taxonomy_block"
LIST_PATH = "robosystems.graphql.resolvers.taxonomy_block.list_taxonomy_blocks"


def _make_user() -> MagicMock:
  user = MagicMock()
  user.id = "usr_test"
  user.email = "test@example.com"
  return user


def _ctx(
  user: MagicMock | None = None,
  *,
  graph_id: str = GRAPH_ID,
  schema_extensions: tuple[str, ...] = ("roboledger",),
  graph_type: str = "entity",
) -> dict:
  return {
    "request": MagicMock(),
    "user": user or _make_user(),
    "graph_id": graph_id,
    "schema_extensions": schema_extensions,
    "graph_type": graph_type,
  }


@contextmanager
def _patch_session():
  mock_session = MagicMock()
  mock_ctx_mgr = MagicMock()
  mock_ctx_mgr.__enter__ = MagicMock(return_value=mock_session)
  mock_ctx_mgr.__exit__ = MagicMock(return_value=False)
  with patch("robosystems.db.extensions.extensions_session", return_value=mock_ctx_mgr):
    yield mock_session


def _envelope(
  taxonomy_id: str = "tx_coa_01",
  taxonomy_type: str = "chart_of_accounts",
) -> TaxonomyBlockEnvelope:
  return TaxonomyBlockEnvelope(
    id=taxonomy_id,
    name="Acme CoA 2026",
    taxonomy_type=taxonomy_type,
    display_name="Chart of Accounts",
    category="Chart",
    elements=[
      TaxonomyBlockElement(
        id="elem_1",
        qname="acme:Cash",
        name="Cash",
        classification="asset",
        balance_type="debit",
        period_type="instant",
        origin="tenant",
      ),
    ],
    element_count=1,
  )


_TB_FIELDS = (
  "{ id name taxonomyType displayName category elementCount "
  "elements { id qname name classification balanceType origin } }"
)


class TestTaxonomyBlockQuery:
  def test_returns_envelope_when_found(self) -> None:
    expected = _envelope("tx_coa_01")
    with (
      _patch_session(),
      patch(GET_PATH, return_value=expected),
    ):
      result = schema.execute_sync(
        f'query {{ taxonomyBlock(id: "tx_coa_01") {_TB_FIELDS} }}',
        context_value=_ctx(),
      )
    assert result.errors is None
    data = result.data["taxonomyBlock"]
    assert data["id"] == "tx_coa_01"
    assert data["taxonomyType"] == "chart_of_accounts"
    assert data["displayName"] == "Chart of Accounts"
    assert data["elementCount"] == 1
    assert data["elements"][0]["qname"] == "acme:Cash"
    assert data["elements"][0]["origin"] == "tenant"

  def test_returns_null_when_not_found(self) -> None:
    with (
      _patch_session(),
      patch(GET_PATH, return_value=None),
    ):
      result = schema.execute_sync(
        'query { taxonomyBlock(id: "tx_missing") { id } }',
        context_value=_ctx(),
      )
    assert result.errors is None
    assert result.data["taxonomyBlock"] is None

  def test_unauthenticated_returns_error(self) -> None:
    result = schema.execute_sync(
      'query { taxonomyBlock(id: "tx_x") { id } }',
      context_value={
        "request": MagicMock(),
        "user": None,
        "graph_id": GRAPH_ID,
        "schema_extensions": (),
        "graph_type": "entity",
      },
    )
    assert result.errors is not None
    assert any("Authentication required" in str(e.message) for e in result.errors)

  def test_works_without_extension_flag(self) -> None:
    """TaxonomyBlock is always-on — no extension gate."""
    expected = _envelope()
    with (
      _patch_session(),
      patch(GET_PATH, return_value=expected),
    ):
      result = schema.execute_sync(
        f'query {{ taxonomyBlock(id: "tx_coa_01") {_TB_FIELDS} }}',
        context_value=_ctx(schema_extensions=()),
      )
    assert result.errors is None
    assert result.data["taxonomyBlock"]["id"] == "tx_coa_01"


class TestTaxonomyBlocksQuery:
  def test_returns_list_for_tenant_graph(self) -> None:
    envelopes = [_envelope("tx_a"), _envelope("tx_b")]
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=envelopes) as mock_list,
    ):
      result = schema.execute_sync(
        f"query {{ taxonomyBlocks {_TB_FIELDS} }}",
        context_value=_ctx(),
      )
    assert result.errors is None
    assert len(result.data["taxonomyBlocks"]) == 2
    mock_list.assert_called_once()
    _, kwargs = mock_list.call_args
    assert kwargs["library_sentinel"] is False

  def test_library_sentinel_flag_set_for_library_graph(self) -> None:
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=[]) as mock_list,
    ):
      result = schema.execute_sync(
        f"query {{ taxonomyBlocks {_TB_FIELDS} }}",
        context_value=_ctx(graph_id=LIBRARY_GRAPH_ID),
      )
    assert result.errors is None
    _, kwargs = mock_list.call_args
    assert kwargs["library_sentinel"] is True

  def test_taxonomy_type_filter_passed_through(self) -> None:
    with (
      _patch_session(),
      patch(LIST_PATH, return_value=[]) as mock_list,
    ):
      schema.execute_sync(
        'query { taxonomyBlocks(taxonomyType: "chart_of_accounts") { id } }',
        context_value=_ctx(),
      )
    _, kwargs = mock_list.call_args
    assert kwargs["taxonomy_type"] == "chart_of_accounts"

  def test_pagination_bounds_rejected(self) -> None:
    with _patch_session():
      result = schema.execute_sync(
        "query { taxonomyBlocks(limit: 10000) { id } }",
        context_value=_ctx(),
      )
    assert result.errors is not None
