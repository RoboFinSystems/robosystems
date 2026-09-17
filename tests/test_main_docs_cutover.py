"""The API host after the reference moved to robosystems.ai.

The rendered reference is a page per operation on the app's domain. What is
left here is the development affordance and the pointers:

- Everywhere but development, ``/`` and ``/docs`` answer ``301`` so every
  README, CONTRIBUTING file and outside link keeps working and its link
  equity moves to the published pages, and ``/static`` is not mounted at all.
  With no page on this origin, nothing needs the relaxed ``style-src`` that
  the docs CSP variant carried — the accepted residual has no surface left.
  The suite runs with ``ENVIRONMENT=test``, so the shared ``client`` fixture
  is already that shape and tests it directly.
- Development still serves Swagger at ``/`` and ReDoc at ``/docs`` from the
  vendored bundles under ``/static``, because trying a call against a local
  stack is worth having and is safe on localhost. **That branch is not
  exercised here.** The routes are decided when the app is built, so it would
  take a second ``create_app()``, and this suite already sits at the edge of
  the local development Postgres: at the default parallelism, adding any
  module that uses the shared ``client`` fixture errors unrelated modules in
  setup with ``OutOfMemory``, and a second app is more of the same cost. What
  is pinned instead is everything the branch depends on: the CSP variant
  those paths get when the pages are served (``TestDevelopmentKeepsItsPolicy``
  below, and ``tests/test_main_csp.py``) and the pages the generators produce
  (``TestDocsPagesSelfHosted``, same file). The registration itself is
  exercised by anyone running the stack locally, where a regression is
  immediate and visible.
- ``/openapi.json`` is unchanged and stays served — both SDK generators read
  it — and is marked ``noindex`` so the raw specification does not compete
  with the rendered reference in a search index.
"""

from __future__ import annotations

import pytest

from robosystems.config import env

pytestmark = pytest.mark.unit


class TestOutsideDevelopment:
  @pytest.mark.parametrize("path", ["/", "/docs"])
  def test_docs_paths_redirect_to_the_published_reference(self, client, path) -> None:
    response = client.get(path, follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == f"{env.ROBOSYSTEMS_URL}/docs/api"

  @pytest.mark.parametrize("path", ["/", "/docs"])
  def test_the_redirect_answers_head_as_well_as_get(self, client, path) -> None:
    """A link checker that probes with HEAD has to see the redirect too.

    FastAPI does not imply HEAD from GET, so before these paths became
    redirects they answered 405 to it — harmless for a page, useless for a
    URL whose only job is to be followed.
    """
    response = client.head(path, follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == f"{env.ROBOSYSTEMS_URL}/docs/api"

  def test_static_is_not_mounted(self, client) -> None:
    assert client.get("/static/swagger-init.js").status_code == 404

  @pytest.mark.parametrize("path", ["/", "/docs", "/static/swagger-custom.css"])
  def test_the_relaxed_docs_policy_is_gone(self, client, path) -> None:
    response = client.get(path, follow_redirects=False)
    csp = response.headers["content-security-policy"]
    assert "'unsafe-inline'" not in csp
    assert "worker-src" not in csp
    assert "script-src 'self';" in csp

  def test_the_specification_is_served_but_not_indexed(self, client) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.headers["x-robots-tag"] == "noindex"
    assert response.json()["info"]["title"] == "RoboSystems API"

  def test_the_disclosure_route_survives(self, client) -> None:
    """It reads its file at startup, not through the /static mount."""
    assert client.get("/.well-known/security.txt").status_code == 200


class TestDevelopmentKeepsItsPolicy:
  """The half of the development branch that costs nothing to pin."""

  def test_the_pages_would_get_the_policy_they_need(self) -> None:
    from main import csp_variant_for_path

    assert csp_variant_for_path("/", docs_enabled=True) == "docs"
    assert csp_variant_for_path("/static/swagger-init.js", docs_enabled=True) == "docs"
