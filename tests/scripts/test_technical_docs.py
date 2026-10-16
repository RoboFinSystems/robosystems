"""The technical docs in ``docs/technical/`` build with no errors and no warnings.

The publish fails on a link to a page that does not exist, but only after the
change is on ``main``. Checking here moves that failure into the pull request,
and holds the anchor warnings to the same bar: a ``#heading`` that no longer
exists is a dead jump on the published page.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from robosystems.scripts import publish_docs

REPO_ROOT = Path(__file__).resolve().parents[2]
TECHNICAL_DIR = REPO_ROOT / "docs" / "technical"


@pytest.mark.unit
def test_technical_pages_build_without_errors_or_warnings():
  build = publish_docs.Build()
  publish_docs.build_technical(TECHNICAL_DIR, REPO_ROOT, build)
  assert build.pages
  assert build.errors == []
  assert build.warnings == []


@pytest.mark.unit
def test_product_links_into_the_technical_docs_land():
  build = publish_docs.Build()
  publish_docs.build_technical(TECHNICAL_DIR, REPO_ROOT, build)
  publish_docs.build_product(REPO_ROOT / "docs" / "product", REPO_ROOT, build)
  assert build.errors == []
