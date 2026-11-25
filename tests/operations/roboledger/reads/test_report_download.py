"""Tests for ``get_report_download_url`` — the presigned-URL read that
replaced the retired ``GET .../reports/{id}/download`` REST endpoint.

Covers every flavor (the holon stamped at publish; XBRL and the Tavi
materialized + cached on first download), the cache hit/miss split, and the
not-found / not-available / signing-failure error paths.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from xbrlkit import __version__ as XBRLKIT_VERSION

from robosystems.operations.roboledger.reads.reports import (
  BundleSigningError,
  ReportBundleNotAvailableError,
  get_report_download_url,
)

_REPORTS = "robosystems.operations.roboledger.reads.reports"


# A built DataBook or workbook is kept under the version of xbrlkit that wrote
# it; the name it downloads under carries no version.
_DATABOOK_KEY = f"report-bundles/kg1/rpt_1/g3.xbrlkit-{XBRLKIT_VERSION}.databook.md"
_WORKBOOK_KEY = f"report-bundles/kg1/rpt_1/g3.xbrlkit-{XBRLKIT_VERSION}.xlsx"


def _report(
  bundle_url: str | None = "s3://bkt/report-bundles/kg1/rpt_1/g3.holon.jsonld",
  *,
  generation_count: int = 3,
  generation_status: str = "published",
  source_graph_id: str | None = None,
):
  """A minimal stand-in for a Report ORM row."""
  return SimpleNamespace(
    bundle_url=bundle_url,
    generation_count=generation_count,
    generation_status=generation_status,
    origin="shared" if source_graph_id else "authored",
    is_received=source_graph_id is not None,
    source_graph_id=source_graph_id,
    source_report_id=None,
    filing_status="draft",
    last_generated=None,
    bundle_stamped_at=None,
  )


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
    session = _session(
      _report(bundle_url=None, generation_count=0, generation_status="pending")
    )
    with pytest.raises(ReportBundleNotAvailableError):
      get_report_download_url(session, "kg1", "rpt_1")

  @pytest.mark.unit
  def test_unsupported_flavor_raises_value_error(self):
    session = _session(_report())
    with pytest.raises(ValueError, match="Unsupported download format"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="turtle")

  @pytest.mark.unit
  def test_a_published_report_serves_a_holon_without_a_bundle_url(self):
    """A cross-graph shared copy can arrive without the anchor — the sender's
    holon is copied in beside the rows. `bundle_url` names the anchor the
    holon path never reads, so gating the holon on it would hide an artifact
    that is present and valid."""
    session = _session(_report(bundle_url=None))
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/holon"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")

    assert resp is not None
    assert resp.download_url == "https://signed.example/holon"
    assert resp.format == "holon-jsonld"
    # Written by the same model bridge as the Tavi, so it drops the same content.
    assert resp.omitted_content == [
      "ib_envelopes",
      "definition_links",
      "framework_pins",
      "filing_lifecycle",
    ]

  @pytest.mark.unit
  def test_an_unpublished_report_still_has_no_holon(self):
    """The negative control: dropping the `bundle_url` gate must not turn the
    holon into a draft-preview surface."""
    session = _session(_report(bundle_url=None, generation_status="draft"))
    with pytest.raises(ReportBundleNotAvailableError, match="not published"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")


class TestReceivedReportsAreNeverRederived:
  """A report that arrived from another graph is served as the sender's
  published artifact or not at all.

  For an authored report the rows are the truth and the artifact is a
  projection, so rebuilding a lost cache entry is free and correct. For a
  received report the relationship inverts: the artifact is the sender's
  document and the rows are what the share managed to bring across. Rebuilding
  would present a locally-derived document as the sender's publication, and
  nothing at the point of use would show the difference.
  """

  @pytest.mark.unit
  @pytest.mark.parametrize(
    ("flavor", "artifact"),
    [
      ("holon-jsonld", "holon"),
      ("xbrl-2.1", "XBRL bundle"),
      ("tavi", "Tavi model"),
    ],
  )
  def test_a_cache_miss_on_a_received_report_is_a_miss(self, flavor, artifact):
    session = _session(_report(source_graph_id="kg_sender"))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3_cls.return_value.object_exists.return_value = False
      with pytest.raises(ReportBundleNotAvailableError, match=artifact):
        get_report_download_url(session, "kg1", "rpt_1", flavor=flavor)

    build.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.parametrize("flavor", ["holon-jsonld", "xbrl-2.1", "tavi"])
  def test_a_report_that_arrived_as_a_file_is_never_rederived(self, flavor):
    """A file has no source graph; its origin alone marks it received."""
    report = _report()
    report.origin, report.is_received = "uploaded", True
    session = _session(report)
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3_cls.return_value.object_exists.return_value = False
      # Only the holon was ever stored; the other formats say so.
      expected = (
        "ingest the file again" if flavor == "holon-jsonld" else "kept as a holon only"
      )
      with pytest.raises(ReportBundleNotAvailableError, match=expected) as refused:
        get_report_download_url(session, "kg1", "rpt_1", flavor=flavor)

    build.assert_not_called()
    # A format the file does not have points at the two it does.
    if flavor != "holon-jsonld":
      assert "`holon-jsonld`" in str(refused.value)
      assert "`databook`" in str(refused.value)
      assert "`workbook`" in str(refused.value)

  @pytest.mark.unit
  @pytest.mark.parametrize("flavor", ["holon-jsonld", "xbrl-2.1", "tavi"])
  def test_a_cached_received_artifact_is_still_served(self, flavor):
    """The refusal is about re-deriving, not about received reports — the
    sender's own artifact serves normally."""
    session = _session(_report(source_graph_id="kg_sender"))
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/artifact"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor=flavor)

    assert resp is not None
    assert resp.download_url == "https://signed.example/artifact"

  @pytest.mark.unit
  def test_an_authored_report_still_rebuilds_on_a_miss(self):
    """The negative control: the guard must not disable caching for the
    reports this graph actually owns."""
    session = _session(_report(source_graph_id=None))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.serialize_to_holon_jsonld",
        return_value="{}",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/holon"
      get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")

    build.assert_called_once()


class TestRetiredFlatJsonLd:
  @pytest.mark.unit
  def test_the_flat_json_ld_flavor_is_no_longer_served(self):
    session = _session(_report())
    with pytest.raises(ValueError, match="Unsupported download format"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="jsonld")

  @pytest.mark.unit
  def test_the_default_flavor_is_the_holon(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/holon"
      resp = get_report_download_url(session, "kg1", "rpt_1")

    assert resp is not None
    assert resp.format == "holon-jsonld"
    _, kwargs = s3.generate_presigned_url.call_args
    assert kwargs["key"] == "report-bundles/kg1/rpt_1/g3.holon.jsonld"


class TestDataBook:
  """The DataBook is written from the stored holon, never from the rows."""

  @staticmethod
  def _exists(present: set[str]):
    return lambda bucket, key: key.rsplit("/", 1)[-1] in present

  @pytest.mark.unit
  def test_cache_hit_presigns_without_reading_the_holon(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/databook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

    assert resp is not None
    assert resp.format == "databook"
    assert resp.content_type == "text/markdown; charset=utf-8"
    s3.read_string.assert_not_called()
    s3.upload_string.assert_not_called()
    _, kwargs = s3.generate_presigned_url.call_args
    assert kwargs["key"] == _DATABOOK_KEY
    assert kwargs["response_content_disposition"].endswith('rpt_1-g3.databook.md"')
    # A projection by design: what it leaves to the holon is declared on a
    # cache hit too.
    assert "dimensional_facts" in resp.omitted_content
    assert "calculation_links" in resp.omitted_content

  @pytest.mark.unit
  def test_cache_miss_writes_it_from_the_stored_holon(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.rdf.databook.databook_from_holon",
        return_value="# the report",
      ) as project,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = self._exists({"g3.holon.jsonld"})
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/databook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

    assert resp is not None
    # The holon was already in storage: nothing is rebuilt from the rows.
    build.assert_not_called()
    _, read_args = s3.read_string.call_args[0]
    assert read_args == "report-bundles/kg1/rpt_1/g3.holon.jsonld"
    project.assert_called_once_with('{"@graph": []}', href="rpt_1-g3.holon.jsonld")
    _, up_kwargs = s3.upload_string.call_args
    assert up_kwargs["content"] == "# the report"
    assert up_kwargs["key"] == _DATABOOK_KEY
    assert up_kwargs["content_type"] == "text/markdown; charset=utf-8"
    # The link serves what was just written, as Markdown, under its own name.
    _, sign_kwargs = s3.generate_presigned_url.call_args
    assert sign_kwargs["key"] == _DATABOOK_KEY
    assert sign_kwargs["response_content_type"] == "text/markdown; charset=utf-8"
    assert sign_kwargs["response_content_disposition"].endswith('rpt_1-g3.databook.md"')

  @pytest.mark.unit
  def test_an_authored_report_with_no_holon_in_storage_gets_one_first(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.serialize_to_holon_jsonld",
        return_value='{"@graph": []}',
      ),
      patch(
        "robosystems.operations.serialization.rdf.databook.databook_from_holon",
        return_value="# the report",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/databook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

    assert resp is not None
    build.assert_called_once()
    keys = [call.kwargs["key"] for call in s3.upload_string.call_args_list]
    assert keys == [
      "report-bundles/kg1/rpt_1/g3.holon.jsonld",
      _DATABOOK_KEY,
    ]

  @pytest.mark.unit
  def test_a_received_report_has_one_when_its_holon_is_in_storage(self):
    """Written from the file that arrived, so it is not a local re-derivation."""
    report = _report()
    report.origin, report.is_received = "uploaded", True
    session = _session(report)
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.rdf.databook.databook_from_holon",
        return_value="# the report",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = self._exists({"g3.holon.jsonld"})
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/databook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

    assert resp is not None and resp.format == "databook"
    build.assert_not_called()

  @pytest.mark.unit
  def test_a_received_report_whose_holon_is_gone_has_none(self):
    session = _session(_report(source_graph_id="kg_sender"))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3_cls.return_value.object_exists.return_value = False
      with pytest.raises(ReportBundleNotAvailableError, match="holon"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

    build.assert_not_called()

  @pytest.mark.unit
  def test_an_unpublished_report_has_no_databook(self):
    session = _session(_report(generation_status="generating"))
    with pytest.raises(ReportBundleNotAvailableError, match="DataBook"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

  @pytest.mark.unit
  def test_a_holon_that_cannot_be_read_back_is_a_signing_error(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = self._exists({"g3.holon.jsonld"})
      s3.read_string.return_value = None
      with pytest.raises(BundleSigningError, match="holon"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="databook")

  @pytest.mark.unit
  def test_upload_failure_raises_signing_error(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch(
        "robosystems.operations.serialization.rdf.databook.databook_from_holon",
        return_value="# the report",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = self._exists({"g3.holon.jsonld"})
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_string.return_value = False
      with pytest.raises(BundleSigningError, match="DataBook"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="databook")


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
    # The emitter strips tenant-authored notes on every generation, so the
    # omission is declared on a cache hit too — the file is valid and would
    # otherwise be silently incomplete next to the notes shown on screen.
    assert resp.omitted_content == ["disclosure_notes"]

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
    assert resp.omitted_content == ["disclosure_notes"]
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
      with pytest.raises(BundleSigningError, match="materialize") as excinfo:
        get_report_download_url(session, "kg1", "rpt_1", flavor="xbrl-2.1")
      assert "s3://" not in str(excinfo.value)


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
      with pytest.raises(BundleSigningError, match="materialize") as excinfo:
        get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld")
      assert "s3://" not in str(excinfo.value)


class TestTavi:
  """The Tavi compiled model — derived + cached on demand like the holon,
  gated on publication rather than ``bundle_url``, written as bytes."""

  @pytest.mark.unit
  def test_cache_hit_presigns_without_rebuilding(self):
    session = _session(_report(bundle_url=None))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/tavi"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="tavi")

    assert resp is not None
    assert resp.content_type == "application/json"
    assert resp.format == "tavi"
    assert "ib_envelopes" in resp.omitted_content
    build.assert_not_called()
    s3.upload_bytes.assert_not_called()

  @pytest.mark.unit
  def test_cache_miss_materializes_then_presigns(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.serialize_to_tavi",
        return_value=b'{"documentInfo":{}}',
      ) as serialize,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_bytes.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/tavi"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="tavi")

    assert resp is not None
    build.assert_called_once()
    serialize.assert_called_once()
    _, up_kwargs = s3.upload_bytes.call_args
    assert up_kwargs["content"] == b'{"documentInfo":{}}'
    assert up_kwargs["content_type"] == "application/json"
    assert up_kwargs["key"].endswith("g3.tavi.json")
    _, sign_kwargs = s3.generate_presigned_url.call_args
    assert sign_kwargs["response_content_type"] == "application/json"
    assert (
      'filename="rpt_1-g3.tavi.json"' in sign_kwargs["response_content_disposition"]
    )

  @pytest.mark.unit
  def test_an_unpublished_report_has_no_tavi(self):
    session = _session(_report(generation_status="draft"))
    with pytest.raises(ReportBundleNotAvailableError, match="not published"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="tavi")

  @pytest.mark.unit
  def test_upload_failure_raises_signing_error(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle"),
      patch(
        "robosystems.operations.serialization.serialize_to_tavi",
        return_value=b"{}",
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_bytes.return_value = False
      with pytest.raises(BundleSigningError, match="materialize") as excinfo:
        get_report_download_url(session, "kg1", "rpt_1", flavor="tavi")
      assert "s3://" not in str(excinfo.value)


class TestLineageHolon:
  """`holon-jsonld-lineage`: the holon with its fourth graph, built on first
  download for the graph's own members, logged, and never served for a report
  that was shared in from another graph."""

  @pytest.mark.unit
  def test_cache_hit_presigns_under_the_lineage_key(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/lineage"
      resp = get_report_download_url(
        session, "kg1", "rpt_1", flavor="holon-jsonld-lineage"
      )

    assert resp is not None
    assert resp.format == "holon-jsonld-lineage"
    assert resp.content_type == "application/ld+json"
    _, kwargs = s3.generate_presigned_url.call_args
    assert kwargs["key"] == "report-bundles/kg1/rpt_1/g3.lineage.holon.jsonld"
    assert "g3.lineage.holon.jsonld" in kwargs["response_content_disposition"]

  @pytest.mark.unit
  def test_cache_miss_builds_the_holon_with_lineage_from_the_books(self):
    from robosystems.operations.serialization.rdf.lineage import Lineage

    session = _session(_report())
    complete = Lineage(nodes=[], bridges={"f_1": ["c_1"]})
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.rdf.lineage.build_lineage_holon",
        return_value=('{"@graph": []}', complete),
      ) as built,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/lineage"
      resp = get_report_download_url(
        session, "kg1", "rpt_1", flavor="holon-jsonld-lineage"
      )

    assert resp is not None
    assert resp.omitted_content == []
    build.assert_called_once_with(session, "kg1", "rpt_1")
    built.assert_called_once_with(session, build.return_value)
    _, kwargs = s3.upload_string.call_args
    assert kwargs["key"] == "report-bundles/kg1/rpt_1/g3.lineage.holon.jsonld"
    assert kwargs["content"] == '{"@graph": []}'

  @pytest.mark.unit
  def test_a_file_with_refused_figures_is_served_but_never_cached(self):
    """A figure whose reconstruction missed its hash is named in
    `omitted_content`, and the file goes under a partial key the next
    download rebuilds instead of the generation's cache key."""
    from robosystems.operations.serialization.rdf.lineage import Lineage

    session = _session(_report())
    partial = Lineage(nodes=[], bridges={"f_1": ["c_1"]}, refused=["f_2", "f_3"])
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle"),
      patch(
        "robosystems.operations.serialization.rdf.lineage.build_lineage_holon",
        return_value=('{"@graph": []}', partial),
      ),
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = False
      s3.upload_string.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/partial"
      resp = get_report_download_url(
        session, "kg1", "rpt_1", flavor="holon-jsonld-lineage"
      )

    assert resp is not None
    assert resp.omitted_content == ["lineage:f_2", "lineage:f_3"]
    _, kwargs = s3.upload_string.call_args
    assert kwargs["key"] == "report-bundles/kg1/rpt_1/g3.lineage.partial.holon.jsonld"
    _, signed = s3.generate_presigned_url.call_args
    assert signed["key"] == kwargs["key"]

  @pytest.mark.unit
  def test_a_lineage_too_large_for_one_file_is_not_available(self):
    from robosystems.operations.serialization.rdf.lineage import (
      LineageTooLargeError,
    )

    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle"),
      patch(
        "robosystems.operations.serialization.rdf.lineage.build_lineage_holon",
        side_effect=LineageTooLargeError("too many lines; narrower span"),
      ),
    ):
      s3_cls.return_value.object_exists.return_value = False
      with pytest.raises(ReportBundleNotAvailableError, match="narrower span"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld-lineage")
    s3_cls.return_value.upload_string.assert_not_called()

  @pytest.mark.unit
  def test_a_received_report_has_no_lineage_even_when_a_file_exists(self):
    """Lineage does not cross the graph boundary: the refusal is about the
    report's origin, not about a cache miss."""
    session = _session(_report(source_graph_id="kg_sender"))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3_cls.return_value.object_exists.return_value = True
      with pytest.raises(ReportBundleNotAvailableError, match="received from outside"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld-lineage")

    build.assert_not_called()
    s3_cls.return_value.generate_presigned_url.assert_not_called()

  @pytest.mark.unit
  def test_an_unpublished_report_has_no_lineage(self):
    session = _session(
      _report(bundle_url=None, generation_count=0, generation_status="draft")
    )
    with pytest.raises(ReportBundleNotAvailableError, match="not published"):
      get_report_download_url(session, "kg1", "rpt_1", flavor="holon-jsonld-lineage")

  @pytest.mark.unit
  def test_the_download_is_logged_with_the_actor(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch(f"{_REPORTS}.logger") as log,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/lineage"
      get_report_download_url(
        session, "kg1", "rpt_1", flavor="holon-jsonld-lineage", actor_id="usr_9"
      )

    served = [c for c in log.info.call_args_list if "Lineage holon served" in c.args[0]]
    assert len(served) == 1
    message = served[0].args[0] % served[0].args[1:]
    assert "usr_9" in message and "rpt_1" in message and "g3" in message


class TestWorkbook:
  """The workbook is written from the stored holon, as the DataBook is."""

  _TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

  @pytest.mark.unit
  def test_cache_hit_presigns_without_reading_the_holon(self):
    session = _session(_report())
    with patch(f"{_REPORTS}.S3Client") as s3_cls:
      s3 = s3_cls.return_value
      s3.object_exists.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/workbook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="workbook")

    assert resp is not None
    assert resp.format == "workbook"
    assert resp.content_type == self._TYPE
    s3.read_string.assert_not_called()
    s3.upload_bytes.assert_not_called()
    _, kwargs = s3.generate_presigned_url.call_args
    assert kwargs["key"] == _WORKBOOK_KEY
    assert kwargs["response_content_disposition"].endswith('rpt_1-g3.xlsx"')
    # It draws the breakdowns, so unlike the DataBook it does not omit them.
    assert "dimensional_facts" not in resp.omitted_content
    assert "sparse_period_facts" in resp.omitted_content
    assert "calculation_links" in resp.omitted_content

  @pytest.mark.unit
  def test_cache_miss_writes_it_from_the_stored_holon(self):
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
      patch(
        "robosystems.operations.serialization.rdf.workbook.workbook_from_holon",
        return_value=b"PK-the-workbook",
      ) as project,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = TestDataBook._exists({"g3.holon.jsonld"})
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_bytes.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/workbook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="workbook")

    assert resp is not None
    # The holon was already in storage: nothing is rebuilt from the rows.
    build.assert_not_called()
    _, read_args = s3.read_string.call_args[0]
    assert read_args == "report-bundles/kg1/rpt_1/g3.holon.jsonld"
    project.assert_called_once_with('{"@graph": []}', href="rpt_1-g3.holon.jsonld")
    _, up_kwargs = s3.upload_bytes.call_args
    assert up_kwargs["content"] == b"PK-the-workbook"
    assert up_kwargs["key"] == _WORKBOOK_KEY
    assert up_kwargs["content_type"] == self._TYPE
    _, sign_kwargs = s3.generate_presigned_url.call_args
    assert sign_kwargs["key"] == _WORKBOOK_KEY
    assert sign_kwargs["response_content_type"] == self._TYPE

  @pytest.mark.unit
  def test_a_file_an_older_xbrlkit_wrote_is_not_served(self):
    """The cache key names the writer's version: after an upgrade the old
    file is still in storage and is passed over for a new one."""
    session = _session(_report())
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch(
        "robosystems.operations.serialization.rdf.workbook.workbook_from_holon",
        return_value=b"PK-rebuilt",
      ) as project,
    ):
      s3 = s3_cls.return_value
      s3.object_exists.side_effect = TestDataBook._exists(
        {"g3.holon.jsonld", "g3.xlsx", "g3.xbrlkit-0.0.1.xlsx"}
      )
      s3.read_string.return_value = '{"@graph": []}'
      s3.upload_bytes.return_value = True
      s3.generate_presigned_url.return_value = "https://signed.example/workbook"
      resp = get_report_download_url(session, "kg1", "rpt_1", flavor="workbook")

    assert resp is not None
    project.assert_called_once()
    _, up_kwargs = s3.upload_bytes.call_args
    assert up_kwargs["key"] == _WORKBOOK_KEY
    # The name a reader saves it under carries no version.
    _, sign_kwargs = s3.generate_presigned_url.call_args
    assert sign_kwargs["response_content_disposition"].endswith('rpt_1-g3.xlsx"')

  @pytest.mark.unit
  def test_a_draft_report_has_none(self):
    report = _report()
    report.generation_status = "draft"
    session = _session(report)
    with patch(f"{_REPORTS}.S3Client"):
      with pytest.raises(ReportBundleNotAvailableError, match="workbook"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="workbook")

  @pytest.mark.unit
  def test_a_received_report_whose_holon_is_gone_has_none(self):
    session = _session(_report(source_graph_id="kg_sender"))
    with (
      patch(f"{_REPORTS}.S3Client") as s3_cls,
      patch("robosystems.operations.serialization.build_report_bundle") as build,
    ):
      s3_cls.return_value.object_exists.return_value = False
      with pytest.raises(ReportBundleNotAvailableError, match="holon"):
        get_report_download_url(session, "kg1", "rpt_1", flavor="workbook")

    build.assert_not_called()
