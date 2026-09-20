"""The API host after the reference moved to robosystems.ai.

Two surfaces, split by what each is for, and the split is the same in every
environment:

- **``/`` keeps Swagger UI.** Its try-it panel is a tool, and there is no
  other way to run a call against a deployed API from a browser. It is
  marked ``noindex``: a tool, not a document. That keeps the relaxed
  ``style-src`` on this origin, which is the deliberate price of keeping it.
- **``/docs`` is gone.** It served ReDoc — a read-only renderer of the same
  specification, which the per-operation pages on the app's domain do far
  better, and which no crawler could read because it renders in the browser
  from a ~950 KB file. It answers ``301`` so every README, CONTRIBUTING file
  and outside link keeps working and its link equity moves with it. A
  redirect needs no relaxed policy, so it gets the strict one.
- **``/openapi.json``** is unchanged and stays served — both SDK generators
  read it — and is ``noindex`` for the same reason as ``/``.

ReDoc itself is not deleted: the Graph API microservice still serves its own
from ``generate_redoc_docs`` and mounts the same vendored bundle. What went is
the main API's route and the thin wrapper behind it.
"""

from __future__ import annotations

import pytest

from robosystems.config import env

pytestmark = pytest.mark.unit


class TestTheRedirect:
  def test_docs_redirects_to_the_published_reference(self, client) -> None:
    response = client.get("/docs", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == f"{env.ROBOSYSTEMS_URL}/docs/api"

  def test_it_answers_head_as_well_as_get(self, client) -> None:
    """A link checker that probes with HEAD has to see the redirect too.

    FastAPI does not imply HEAD from GET, so before this path became a
    redirect it answered 405 to it — harmless for a page, useless for a URL
    whose only job is to be followed.
    """
    response = client.head("/docs", follow_redirects=False)
    assert response.status_code == 301
    assert response.headers["location"] == f"{env.ROBOSYSTEMS_URL}/docs/api"

  def test_a_redirect_needs_no_relaxed_policy(self, client) -> None:
    csp = client.get("/docs", follow_redirects=False).headers["content-security-policy"]
    assert "'unsafe-inline'" not in csp
    assert "script-src 'self';" in csp

  def test_the_main_api_no_longer_renders_redoc(self) -> None:
    """The wrapper is gone; the Graph API's own generator is untouched."""
    from robosystems.utils import docs_template

    assert not hasattr(docs_template, "generate_robosystems_redoc")
    assert hasattr(docs_template, "generate_redoc_docs")


class TestTheTool:
  def test_swagger_is_served(self, client) -> None:
    response = client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]

  def test_its_bundle_is_served(self, client) -> None:
    assert client.get("/static/swagger-init.js").status_code == 200

  def test_it_keeps_the_policy_it_needs(self, client) -> None:
    csp = client.get("/").headers["content-security-policy"]
    # Swagger sets inline style attributes at runtime.
    assert "style-src 'self' 'unsafe-inline'" in csp
    # Still no third-party origin and no inline script, as the remediation set.
    assert "https://" not in csp
    assert "script-src 'self';" in csp

  def test_redocs_blob_worker_left_with_redoc(self, client) -> None:
    assert "worker-src" not in client.get("/").headers["content-security-policy"]


class TestNothingHereIsTheReference:
  """The rendered pages are on the app's domain; this origin competes with none."""

  @pytest.mark.parametrize("path", ["/", "/openapi.json", "/static/swagger-init.js"])
  def test_marked_noindex(self, client, path) -> None:
    assert client.get(path).headers["x-robots-tag"] == "noindex"

  def test_api_routes_are_not(self, client) -> None:
    assert "x-robots-tag" not in client.get("/v1/status").headers

  def test_the_specification_is_still_served(self, client) -> None:
    response = client.get("/openapi.json")
    assert response.status_code == 200
    assert response.json()["info"]["title"] == "RoboSystems API"

  def test_the_disclosure_route_survives(self, client) -> None:
    assert client.get("/.well-known/security.txt").status_code == 200


class TestSpecProseIsMarkdown:
  """The specification's prose is what `/docs/api` renders, verbatim.

  An operation's `summary`, `description` and the descriptions on its schemas
  are its page — nothing rewrites them between here and the published
  reference, and the app renders them as markdown. So reStructuredText in a
  Pydantic docstring is not a style question: ``foo`` reaches a reader as
  literal backticks around the word, on a public page.

  This went unnoticed because the markup is invisible in Python. It was found
  on three live operation pages and in 93 schema descriptions.
  """

  @staticmethod
  def _descriptions(spec: dict):
    for name, schema in (spec.get("components", {}).get("schemas") or {}).items():
      if isinstance(schema.get("description"), str):
        yield name, schema["description"]
      for prop, value in (schema.get("properties") or {}).items():
        if isinstance(value, dict) and isinstance(value.get("description"), str):
          yield f"{name}.{prop}", value["description"]
    for path, item in (spec.get("paths") or {}).items():
      for method, operation in item.items():
        if isinstance(operation, dict) and isinstance(
          operation.get("description"), str
        ):
          yield f"{method.upper()} {path}", operation["description"]

  def test_the_sweep_is_not_vacuous(self, client) -> None:
    spec = client.get("/openapi.json").json()
    assert len(list(self._descriptions(spec))) > 200

  def test_no_description_carries_rest_markup(self, client) -> None:
    spec = client.get("/openapi.json").json()
    offenders = sorted(
      where for where, text in self._descriptions(spec) if "``" in text
    )
    assert offenders == [], (
      f"Specification prose carries reStructuredText markup: {offenders}. "
      "Use single backticks — the reference renders markdown."
    )
