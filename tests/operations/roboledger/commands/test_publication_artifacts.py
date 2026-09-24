"""Tests for the publication artifacts a share carries across
(``_load_publication_artifacts`` / ``_copy_publication_artifacts``).

The Tavi is stamped at publish as the anchor; the holon is derived on demand.
A share builds whichever artifact storage lacks (a generation stamped before
the Tavi became the anchor has none until downloaded), off one bundle, and
copies each under the recipient's keys with its own media type.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.commands.reports import (
  PUBLICATION_MEDIA_TYPES,
  _copy_publication_artifacts,
  _load_publication_artifacts,
)

_CMD = "robosystems.operations.roboledger.commands.reports"


def _s3(stored: dict[str, str]) -> MagicMock:
  s3 = MagicMock()
  s3.download_string.side_effect = lambda bucket, key: next(
    (content for suffix, content in stored.items() if key.endswith(suffix)), None
  )
  s3.upload_string.return_value = True
  return s3


@pytest.mark.unit
def test_missing_artifacts_are_built_off_one_bundle() -> None:
  s3 = _s3({"g1.tavi.json": "{tavi}"})
  with (
    patch(f"{_CMD}.S3Client", return_value=s3),
    patch(f"{_CMD}.build_report_bundle") as build,
    patch(f"{_CMD}.serialize_to_holon_jsonld", return_value="{holon}"),
    patch(f"{_CMD}.serialize_to_tavi") as tavi,
    patch("robosystems.db.extensions.extensions_session"),
  ):
    artifacts = _load_publication_artifacts("kg1", "rpt_1", 1)

  assert artifacts == {".tavi.json": "{tavi}", ".holon.jsonld": "{holon}"}
  build.assert_called_once()
  tavi.assert_not_called()
  uploads = {
    call.kwargs["key"].rsplit("/", 1)[-1]: call.kwargs["content_type"]
    for call in s3.upload_string.call_args_list
  }
  assert uploads == {"g1.holon.jsonld": "application/ld+json"}


@pytest.mark.unit
def test_a_generation_stamped_before_the_tavi_anchor_gets_one_built() -> None:
  s3 = _s3({"g1.jsonld": "{flat}"})
  with (
    patch(f"{_CMD}.S3Client", return_value=s3),
    patch(f"{_CMD}.build_report_bundle") as build,
    patch(f"{_CMD}.serialize_to_holon_jsonld", return_value="{holon}"),
    patch(f"{_CMD}.serialize_to_tavi", return_value=b"{tavi}"),
    patch("robosystems.db.extensions.extensions_session"),
  ):
    artifacts = _load_publication_artifacts("kg1", "rpt_1", 1)

  assert artifacts == {".tavi.json": "{tavi}", ".holon.jsonld": "{holon}"}
  build.assert_called_once()
  uploads = {
    call.kwargs["key"].rsplit("/", 1)[-1]: call.kwargs["content_type"]
    for call in s3.upload_string.call_args_list
  }
  assert uploads == {
    "g1.tavi.json": "application/json",
    "g1.holon.jsonld": "application/ld+json",
  }


@pytest.mark.unit
def test_cached_artifacts_are_read_not_rebuilt() -> None:
  s3 = _s3({"g1.holon.jsonld": "{holon}", "g1.tavi.json": "{t}"})
  with (
    patch(f"{_CMD}.S3Client", return_value=s3),
    patch(f"{_CMD}.build_report_bundle") as build,
  ):
    artifacts = _load_publication_artifacts("kg1", "rpt_1", 1)

  assert set(artifacts) == {".holon.jsonld", ".tavi.json"}
  build.assert_not_called()
  s3.upload_string.assert_not_called()


@pytest.mark.unit
def test_one_failing_encoder_keeps_the_other_artifacts() -> None:
  s3 = _s3({})
  with (
    patch(f"{_CMD}.S3Client", return_value=s3),
    patch(f"{_CMD}.build_report_bundle"),
    patch(f"{_CMD}.serialize_to_holon_jsonld", return_value="{holon}"),
    patch(f"{_CMD}.serialize_to_tavi", side_effect=RuntimeError("boom")),
    patch("robosystems.db.extensions.extensions_session"),
  ):
    artifacts = _load_publication_artifacts("kg1", "rpt_1", 1)

  assert artifacts == {".holon.jsonld": "{holon}"}


@pytest.mark.unit
def test_copy_writes_each_artifact_under_its_own_media_type() -> None:
  s3 = MagicMock()
  s3.upload_string.return_value = True
  report = SimpleNamespace(id="rpt_copy", generation_count=0, bundle_url=None)
  artifacts = {".holon.jsonld": "{holon}", ".tavi.json": "{tavi}"}
  with patch(f"{_CMD}.S3Client", return_value=s3):
    _copy_publication_artifacts(artifacts, "kg2", report, 1)  # type: ignore[arg-type]

  written = {
    call.kwargs["key"].rsplit("/", 1)[-1]: call.kwargs["content_type"]
    for call in s3.upload_string.call_args_list
  }
  assert written == {
    "g1.holon.jsonld": PUBLICATION_MEDIA_TYPES[".holon.jsonld"],
    "g1.tavi.json": "application/json",
  }
  assert all(
    call.kwargs["key"].startswith("report-bundles/kg2/rpt_copy/")
    for call in s3.upload_string.call_args_list
  )
  assert report.generation_count == 1
  assert report.bundle_url is not None and report.bundle_url.endswith("/g1.tavi.json")


@pytest.mark.unit
def test_copy_without_the_anchor_leaves_bundle_url_unset() -> None:
  s3 = MagicMock()
  s3.upload_string.return_value = True
  report = SimpleNamespace(id="rpt_copy", generation_count=0, bundle_url=None)
  with patch(f"{_CMD}.S3Client", return_value=s3):
    _copy_publication_artifacts({".holon.jsonld": "{holon}"}, "kg2", report, 1)  # type: ignore[arg-type]

  assert report.bundle_url is None
