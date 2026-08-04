"""Tests for the CSP variant matcher in main.py.

The matcher decides which Content-Security-Policy header a path gets:

- "docs": Swagger UI / ReDoc pages and /static assets. Everything is
  self-hosted (static/vendor), so the policy allows no third-party script
  origins and no 'unsafe-inline' script.
- "graphiql": the GraphiQL playground, which loads React/GraphiQL from
  CDNs and needs the relaxed policy. The post-cutover URL change
  (graph-scoping the GraphQL endpoint) silently broke the original
  `startswith("/extensions/graphql")` check, so the URL shapes are pinned
  here.
- "api": everything else — strict policy.

These tests pin the matcher contract so future URL changes that move
docs or GraphiQL must update the matcher and its tests together.
"""

from __future__ import annotations

from main import csp_variant_for_path


class TestCspVariantForPath:
  def test_root_swagger_ui_docs_variant(self) -> None:
    assert csp_variant_for_path("/") == "docs"

  def test_docs_page_docs_variant(self) -> None:
    assert csp_variant_for_path("/docs") == "docs"

  def test_static_assets_docs_variant(self) -> None:
    assert csp_variant_for_path("/static/swagger-custom.css") == "docs"
    assert csp_variant_for_path("/static/vendor/swagger-ui-5.9.0/swagger-ui.css") == (
      "docs"
    )
    assert csp_variant_for_path("/static") == "docs"

  def test_graph_scoped_graphiql_relaxed(self) -> None:
    """The graph-scoped GraphQL endpoint must get the graphiql variant.

    Regression: an earlier matcher only accepted `/extensions/graphql`
    (the pre-cutover shape) and would have served the strict API CSP
    on the new URL, blocking the CDN-hosted GraphiQL bundle.
    """
    assert csp_variant_for_path("/extensions/kg01234567890abcdef/graphql") == (
      "graphiql"
    )
    # Subgraph IDs (with underscores) work too
    assert csp_variant_for_path("/extensions/kg01a2b3c_dev/graphql") == "graphiql"

  def test_legacy_graphql_path_still_relaxed(self) -> None:
    """The pre-cutover URL is kept defensively in case anything still hits it."""
    assert csp_variant_for_path("/extensions/graphql") == "graphiql"

  def test_api_routes_strict(self) -> None:
    """Core platform API routes use the strict CSP."""
    assert csp_variant_for_path("/v1/status") == "api"
    assert csp_variant_for_path("/v1/auth/login") == "api"
    assert csp_variant_for_path("/v1/graphs/kg01a2b3c/query") == "api"
    assert csp_variant_for_path("/v1/billing/subscribe") == "api"

  def test_extensions_operation_routes_strict(self) -> None:
    """REST operation endpoints under /extensions/* are JSON, not GraphiQL.

    They must NOT get a relaxed CSP — those routes never serve HTML
    and shouldn't be loading scripts from CDNs.
    """
    assert (
      csp_variant_for_path("/extensions/roboledger/kg01a2b3c/operations/update-entity")
      == "api"
    )
    assert (
      csp_variant_for_path("/extensions/roboledger/kg01a2b3c/operations/close-period")
      == "api"
    )
    assert (
      csp_variant_for_path(
        "/extensions/roboinvestor/kg01a2b3c/operations/create-portfolio"
      )
      == "api"
    )

  def test_arbitrary_paths_strict(self) -> None:
    assert csp_variant_for_path("/openapi.json") == "api"
    assert csp_variant_for_path("/some-random-path") == "api"
    assert csp_variant_for_path("") == "api"

  def test_path_traversal_does_not_bypass(self) -> None:
    """Defense in depth: a `/graphql` suffix anywhere else shouldn't relax CSP."""
    assert csp_variant_for_path("/v1/auth/graphql") == "api"
    assert csp_variant_for_path("/extensions-evil/foo/graphql") == "api"


class TestDocsPagesSelfHosted:
  """The rendered docs pages must not reference third-party origins.

  This is the regression guard for the docs supply-chain hardening: the
  Swagger UI / ReDoc bundles are vendored under /static/vendor, and the
  Swagger initializer is an external script (no inline <script>).
  """

  def test_swagger_page_has_no_third_party_or_inline_script(self) -> None:
    from robosystems.utils.docs_template import generate_robosystems_docs

    html = generate_robosystems_docs()
    assert "https://unpkg.com" not in html
    assert "https://cdn.jsdelivr.net" not in html
    assert "https://cdn.redoc.ly" not in html
    assert "/static/vendor/swagger-ui-5.9.0/swagger-ui-bundle.js" in html
    assert "/static/swagger-init.js" in html
    # No inline script: every <script> must carry a src attribute.
    assert "<script>" not in html

  def test_redoc_page_has_no_third_party_script_or_fonts(self) -> None:
    from robosystems.utils.docs_template import generate_robosystems_redoc

    html = generate_robosystems_redoc()
    assert "https://cdn.redoc.ly" not in html
    assert "https://fonts.googleapis.com" not in html
    assert "/static/vendor/redoc-2.5.3/redoc.standalone.js" in html
