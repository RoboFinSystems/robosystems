"""Tests for ``get_report_download_url`` — the presigned-URL read that
replaced the retired ``GET .../reports/{id}/download`` REST endpoint.

Covers both flavors (JSON-LD stamped at publish, XBRL materialized +
cached on first download), the cache hit/miss split, and the
not-found / not-available / signing-failure error paths.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.roboledger.reads.reports import (
  BundleSigningError,
  ReportBundleNotAvailableError,
  get_report_download_url,
)

_REPORTS = "robosystems.operations.roboledger.reads.reports"


def _report(
  bundle_url: str | None = "s3://bkt/report-bundles/kg1/rpt_1/g3.jsonld",
  *,
  generation_count: int = 3,
):
  """A minimal stand-in for a Report ORM row."""
  return SimpleNamespace(bundle_url=bundle_url, generation_count=generation_count)


def _session(report) -> MagicMock:
  session = MagicMock()
  session.get.return_value = report
  return session


class TestNotFoundAndNotAvailable:
  @pytest.mark.unit
  def test_missing_report_returns_none(self):
    session = _session(None)
    assert get_report_download_url(session, "kg1", "rpt_missing") is None

  @pytest.mark.unit
  def test_unpublished_report_raises_not_available(self):
    session = _session(_report(bundle_url=None, generation_count=0))
    with pytest.raises(ReportBundleNotAvailableError):
      get_report_download_url(session, "kg1", "rpt_1")

  @pytest.mark.unit
  def test_unsupported_flavor_raises_value_error(self):
    session = _session(_report())
    with pytest.raises(ValueError, match="Unsupported download format"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="turtle")


class TestJsonLd:
  @pytest.mark.unit
  def test_presigns_stored_bundle(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.generate_presigned_url.return_value = "https://signed.example/jsonld"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="jsonld")

    assert resp is not None
    assert resp.download_url == "https://signed.example/jsonld"
    assert resp.content_type == "application/ld+json"
    assert resp.format == "jsonld"
    assert resp.generation_count == 3
    # Presigns the bucket/key parsed from the stored bundle_url.
    _, kwargs = s3.generate_presigned_url.call_args
    assert kwargs["bucket"] == "bkt"
    assert kwargs["key"] == "report-bundles/kg1/rpt_1/g3.jsonld"
    assert 'filename="rpt_1-g3.jsonld"' in kwargs["response_content_disposition"]

  @pytest.mark.unit
  def test_malformed_bundle_url_raises_signing_error(self):
    session = _session(_report(bundle_url="not-an-s3-uri"))
    with patch(f"{_REPORTS}.S3Client"):
      with pytest.raises(BundleSigningError, match="malformed"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="jsonld")

  @pytest.mark.unit
  def test_presign_failure_raises_signing_error(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3_cls.return_value.generate_presigned_url.return_value = None
      with pytest.raises(BundleSigningError, match="Failed to sign"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="jsonld")


class TestXbrl:
  @pytest.mark.unit
  def test_cache_hit_presigns_without_rebuilding(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/xbrl"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="xbrl-2.1")

    assert resp is not None
    assert resp.content_type == "application/zip"
    assert resp.format == "xbrl-2.1"
    # Cache hit → never rebuilds or uploads.
    build.assert_not_called()
    s3.upload_bytes.assert_not_called()

  @pytest.mark.unit
  def test_cache_miss_materializes_then_presigns(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.serialize_to_xbrl",
        return_value=b"zip-bytes",
      ) as serialize,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_bytes.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/xbrl"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="xbrl-2.1")

    assert resp is not None
    build.assert_called_once()
    serialize.assert_called_once()
    _, up_kwargs = s3.upload_bytes.call_args
    assert up_kwargs["content"] == b"zip-bytes"
    assert up_kwargs["content_type"] == "application/zip"
    assert up_kwargs["key"].endswith("g3.zip")

  @pytest.mark.unit
  def test_upload_failure_raises_signing_error(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle"),
      patch(
        "robosystems.operations.serialization.serialize_to_xbrl",
        return_value=b"zip-bytes",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_bytes.return_value = False
      with pytest.raises(BundleSigningError, match="materialize"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="xbrl-2.1")


class TestHolonJsonLd:
  """The dataset-form holon — derived + cached on demand, like XBRL, but
  serialized as JSON-LD (via ``upload_string``) rather than a zip."""

  @pytest.mark.unit
  def test_cache_hit_presigns_without_rebuilding(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/holon"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")

    assert resp is not None
    assert resp.content_type == "application/ld+json"
    assert resp.format == "holon-jsonld"
    build.assert_not_called()
    s3.upload_string.assert_not_called()

  @pytest.mark.unit
  def test_cache_miss_materializes_then_presigns(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.serialize_to_holon_jsonld",
        return_value='{"@graph": []}',
      ) as serialize,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/holon"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")

    assert resp is not None
    build.assert_called_once()
    serialize.assert_called_once()
    _, up_kwargs = s3.upload_string.call_args
    assert up_kwargs["content"] == '{"@graph": []}'
    assert up_kwargs["content_type"] == "application/ld+json"
    assert up_kwargs["key"].endswith("g3.holon.jsonld")
    _, sign_kwargs = s3.generate_presigned_url.call_args
    assert (
      'filename="rpt_1-g3.holon.jsonld"'
      in (sign_kwargs["response_content_disposition"])
    )

  @pytest.mark.unit
  def test_upload_failure_raises_signing_error(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle"),
      patch(
        "robosystems.operations.serialization.serialize_to_holon_jsonld",
        return_value="{}",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_string.return_value = False
      with pytest.raises(BundleSigningError, match="materialize"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")
