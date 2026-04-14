"""Tests for the relaxed-CSP path matcher in main.py.

The matcher decides which paths get the relaxed Content-Security-Policy
header (CDN-hosted scripts allowed) vs. the strict API CSP. GraphiQL
loads React/GraphiQL from unpkg.com, so the playground path must be
on the relaxed list — but the post-cutover URL change (graph-scoping
the GraphQL endpoint) silently broke the original
`startswith("/extensions/graphql")` check.

These tests pin the matcher contract so future URL changes that move
GraphiQL must update the matcher and its tests together.
"""

from __future__ import annotations

from main import is_relaxed_csp_path


class TestIsRelaxedCspPath:
  def test_root_swagger_ui_relaxed(self) -> None:
    assert is_relaxed_csp_path("/")

  def test_docs_page_relaxed(self) -> None:
    assert is_relaxed_csp_path("/docs")

  def test_static_assets_relaxed(self) -> None:
    assert is_relaxed_csp_path("/static/swagger.css")
    assert is_relaxed_csp_path("/static")

  def test_graph_scoped_graphiql_relaxed(self) -> None:
    """The post-cutover graph-scoped GraphQL endpoint must be relaxed.

    Regression: an earlier matcher only accepted `/extensions/graphql`
    (the pre-cutover shape) and would have served the strict API CSP
    on the new URL, blocking the CDN-hosted GraphiQL bundle.
    """
    assert is_relaxed_csp_path("/extensions/kg01234567890abcdef/graphql")
    # Subgraph IDs (with underscores) work too
    assert is_relaxed_csp_path("/extensions/kg01a2b3c_dev/graphql")

  def test_legacy_graphql_path_still_relaxed(self) -> None:
    """The pre-cutover URL is kept defensively in case anything still hits it."""
    assert is_relaxed_csp_path("/extensions/graphql")

  def test_api_routes_strict(self) -> None:
    """Core platform API routes use the strict CSP."""
    assert not is_relaxed_csp_path("/v1/status")
    assert not is_relaxed_csp_path("/v1/auth/login")
    assert not is_relaxed_csp_path("/v1/graphs/kg01a2b3c/query")
    assert not is_relaxed_csp_path("/v1/billing/subscribe")

  def test_extensions_operation_routes_strict(self) -> None:
    """REST operation endpoints under /extensions/* are JSON, not GraphiQL.

    They must NOT get the relaxed CSP — those routes never serve HTML
    and shouldn't be loading scripts from CDNs.
    """
    assert not is_relaxed_csp_path(
      "/extensions/roboledger/kg01a2b3c/operations/update-entity"
    )
    assert not is_relaxed_csp_path(
      "/extensions/roboledger/kg01a2b3c/operations/close-period"
    )
    assert not is_relaxed_csp_path(
      "/extensions/roboinvestor/kg01a2b3c/operations/create-portfolio"
    )

  def test_arbitrary_paths_strict(self) -> None:
    assert not is_relaxed_csp_path("/openapi.json")
    assert not is_relaxed_csp_path("/some-random-path")
    assert not is_relaxed_csp_path("")

  def test_path_traversal_does_not_bypass(self) -> None:
    """Defense in depth: a `/graphql` suffix anywhere else shouldn't relax CSP."""
    assert not is_relaxed_csp_path("/v1/auth/graphql")
    assert not is_relaxed_csp_path("/extensions-evil/foo/graphql")
