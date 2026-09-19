import hashlib
from unittest.mock import MagicMock, patch

import pytest

from robosystems.adapters.sec.processors.textblock import (
  TextBlockExternalizer,
)


@pytest.fixture
def mock_s3_client():
  client = MagicMock()
  client.upload_string = MagicMock()
  client.batch_upload_strings = MagicMock(return_value={"key1": True, "key2": True})
  client.object_exists = MagicMock(return_value=False)
  client.iter_object_keys = MagicMock(return_value=[])
  return client


@pytest.fixture
def entity_data():
  return {"cik": "0000320193", "name": "Apple Inc"}


@pytest.fixture
def report_data():
  return {
    "filing_date": "2023-09-30",
    "accession_number": "0000320193-23-000077",
  }


class TestTextBlockExternalizerInitialization:
  def test_initialization_basic(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
      enabled=True,
    )

    assert externalizer.s3_client == mock_s3_client
    assert externalizer.bucket == "test-bucket"
    assert externalizer.cdn_url == "https://cdn.example.com"
    assert externalizer.threshold == 5000
    assert externalizer.enabled is True
    assert externalizer.upload_queue == []
    assert externalizer.upload_map == {}
    assert externalizer.content_cache == {}

  def test_initialization_disabled(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
      enabled=False,
    )

    assert externalizer.enabled is False

  def test_initialization_without_cdn(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    assert externalizer.cdn_url is None


class TestShouldExternalize:
  def test_should_externalize_disabled(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
      enabled=False,
    )

    result = externalizer.should_externalize("Some text content")

    assert result is False

  def test_should_externalize_empty_value(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    assert externalizer.should_externalize(None) is False
    assert externalizer.should_externalize("") is False

  def test_should_externalize_html_content(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.should_externalize("<p>HTML content</p>")

    assert result is True

  def test_should_externalize_exceeds_threshold(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=100,
    )

    long_text = "a" * 150

    result = externalizer.should_externalize(long_text)

    assert result is True

  def test_should_not_externalize_below_threshold(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=1000,
    )

    short_text = "Short text"

    result = externalizer.should_externalize(short_text)

    assert result is False


class TestGenerateS3KeyWithHash:
  def test_generate_s3_key_with_hash(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    content_hash = "abcdef1234567890" * 4

    result = externalizer._generate_s3_key_with_hash(
      content_hash, entity_data, report_data, "html"
    )

    assert result == "2023/0000320193/0000320193-23-000077/fact_abcdef123456.html"

  def test_generate_s3_key_with_hash_no_data(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    content_hash = "abcdef1234567890" * 4

    with patch(
      "robosystems.adapters.sec.processors.textblock.datetime"
    ) as mock_datetime:
      mock_datetime.now.return_value.strftime.return_value = "2024"

      result = externalizer._generate_s3_key_with_hash(content_hash, None, None, "txt")

      assert result.startswith("2024/")
      assert "unknown" in result
      assert "fact_abcdef123456.txt" in result


class TestCheckS3ObjectExists:
  def _externalizer(self, s3_client):
    return TextBlockExternalizer(
      s3_client=s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

  def test_one_listing_answers_every_key_in_the_folder(self, mock_s3_client):
    mock_s3_client.iter_object_keys.return_value = ["path/to/object.html"]
    externalizer = self._externalizer(mock_s3_client)

    assert externalizer._check_s3_object_exists("path/to/object.html") is True
    assert externalizer._check_s3_object_exists("path/to/other.html") is False

    mock_s3_client.iter_object_keys.assert_called_once_with("test-bucket", "path/to/")
    mock_s3_client.object_exists.assert_not_called()

  def test_each_folder_is_listed_once(self, mock_s3_client):
    externalizer = self._externalizer(mock_s3_client)

    externalizer._check_s3_object_exists("a/b/one.html")
    externalizer._check_s3_object_exists("a/c/two.html")
    externalizer._check_s3_object_exists("a/b/three.html")

    assert [
      call.args[1] for call in mock_s3_client.iter_object_keys.call_args_list
    ] == [
      "a/b/",
      "a/c/",
    ]

  def test_check_object_exists_no_client(self):
    externalizer = self._externalizer(None)

    assert externalizer._check_s3_object_exists("path/to/object.html") is False

  def test_a_failed_listing_falls_back_to_a_head_per_key(self, mock_s3_client):
    # A listing that failed must not read as an empty folder: every block
    # would upload again.
    mock_s3_client.iter_object_keys.side_effect = Exception("S3 error")
    mock_s3_client.object_exists.return_value = True
    externalizer = self._externalizer(mock_s3_client)

    assert externalizer._check_s3_object_exists("path/to/object.html") is True
    assert externalizer._check_s3_object_exists("path/to/other.html") is True

    mock_s3_client.iter_object_keys.assert_called_once()
    assert mock_s3_client.object_exists.call_count == 2

  def test_a_failed_head_reads_as_absent(self, mock_s3_client):
    mock_s3_client.iter_object_keys.side_effect = Exception("S3 error")
    mock_s3_client.object_exists.side_effect = Exception("S3 error")
    externalizer = self._externalizer(mock_s3_client)

    assert externalizer._check_s3_object_exists("path/to/object.html") is False


class TestQueueValueForS3:
  def test_queue_value_html_content(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    html_content = "<p>Test HTML content</p>"

    result = externalizer.queue_value_for_s3(
      html_content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert result["value_type"] == "external"
    assert result["content_type"] == "text/html"
    assert "cdn.example.com" in result["url"]
    assert len(externalizer.upload_queue) == 1

  def test_queue_value_plain_text(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    text_content = "Plain text content"

    result = externalizer.queue_value_for_s3(
      text_content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert result["content_type"] == "text/plain"
    # No CDN configured: the URL points at the browser-reachable LocalStack
    # endpoint (pytest sets AWS_ENDPOINT_URL=http://localhost:4566), path-style,
    # never the production virtual-hosted s3.amazonaws.com host.
    assert "localhost:4566/test-bucket/" in result["url"]
    assert "s3.amazonaws.com" not in result["url"]

  def test_queue_value_cache_hit(self, mock_s3_client, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    content = "<p>Test content</p>"

    result1 = externalizer.queue_value_for_s3(
      content, "fact1", entity_data, report_data
    )
    result2 = externalizer.queue_value_for_s3(
      content, "fact2", entity_data, report_data
    )

    assert result1 is not None
    assert result2 is not None
    assert result1["url"] == result2["url"]
    assert len(externalizer.upload_queue) == 1

  def test_queue_value_object_exists(self, mock_s3_client, entity_data, report_data):
    content_hash = hashlib.sha256(b"<p>Test content</p>").hexdigest()[:12]
    mock_s3_client.iter_object_keys.return_value = [
      f"2023/0000320193/0000320193-23-000077/fact_{content_hash}.html"
    ]

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=5000,
    )

    content = "<p>Test content</p>"

    result = externalizer.queue_value_for_s3(
      content, "fact123", entity_data, report_data
    )

    assert result is not None
    assert len(externalizer.upload_queue) == 0

  def test_queue_value_no_s3_client(self, entity_data, report_data):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    result = externalizer.queue_value_for_s3(
      "content", "fact123", entity_data, report_data
    )

    assert result is None

  def test_queue_value_exception(self, mock_s3_client, entity_data, report_data):
    with patch(
      "robosystems.adapters.sec.processors.textblock.hashlib.sha256"
    ) as mock_sha256:
      mock_sha256.side_effect = Exception("Hash error")

      externalizer = TextBlockExternalizer(
        s3_client=mock_s3_client,
        bucket="test-bucket",
        cdn_url=None,
        threshold=5000,
      )

      result = externalizer.queue_value_for_s3(
        "content", "fact123", entity_data, report_data
      )

      assert result is None


class TestProcessBatchUploads:
  def test_process_batch_uploads_success(self, mock_s3_client):
    mock_s3_client.batch_upload_strings.return_value = {
      "key1": True,
      "key2": True,
      "key3": True,
    }

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [
      ("content1", "test-bucket", "key1"),
      ("content2", "test-bucket", "key2"),
      ("content3", "test-bucket", "key3"),
    ]

    externalizer.process_batch_uploads()

    mock_s3_client.batch_upload_strings.assert_called_once()
    assert len(externalizer.upload_queue) == 0

  def test_process_batch_uploads_partial_failure(self, mock_s3_client):
    mock_s3_client.batch_upload_strings.return_value = {
      "key1": True,
      "key2": False,
      "key3": True,
    }

    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [
      ("content1", "test-bucket", "key1"),
      ("content2", "test-bucket", "key2"),
      ("content3", "test-bucket", "key3"),
    ]

    externalizer.process_batch_uploads()

    assert len(externalizer.upload_queue) == 0

  def test_each_block_is_stored_with_the_content_type_of_its_extension(
    self, mock_s3_client
  ):
    # An object stored without a Content-Type is served as binary/octet-stream,
    # which the CDN does not compress.
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )
    externalizer.upload_queue = [
      ("<p>one</p>", "test-bucket", "f/fact_1.html"),
      ("two", "test-bucket", "f/fact_2.txt"),
      ("<p>three</p>", "test-bucket", "f/fact_3.html"),
    ]

    externalizer.process_batch_uploads()

    calls = {
      call.kwargs["content_type"]: call.kwargs
      for call in mock_s3_client.batch_upload_strings.call_args_list
    }
    assert set(calls) == {"text/html; charset=utf-8", "text/plain; charset=utf-8"}
    html = calls["text/html; charset=utf-8"]
    assert [item[2] for item in html["items"]] == ["f/fact_1.html", "f/fact_3.html"]
    assert html["cache_control"] == "public, max-age=86400"
    assert html["storage_class"] == "INTELLIGENT_TIERING"
    assert externalizer.upload_queue == []

  def test_process_batch_uploads_empty_queue(self, mock_s3_client):
    externalizer = TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.process_batch_uploads()

    mock_s3_client.batch_upload_strings.assert_not_called()

  def test_process_batch_uploads_no_client(self):
    externalizer = TextBlockExternalizer(
      s3_client=None,
      bucket="test-bucket",
      cdn_url=None,
      threshold=5000,
    )

    externalizer.upload_queue = [("content1", "test-bucket", "key1")]

    externalizer.process_batch_uploads()

    assert len(externalizer.upload_queue) == 1


class TestKeepInline:
  """XBRL_KEEP_TEXTBLOCKS_INLINE: the value stays in the graph AND is uploaded.

  The local control for the Filing Ladder's rung 7b — raw Cypher on a graph that still
  holds the note text. Off everywhere real, so the default path must be unchanged.
  """

  def _externalizer(self, mock_s3_client, keep_inline):
    return TextBlockExternalizer(
      s3_client=mock_s3_client,
      bucket="test-bucket",
      cdn_url="https://cdn.example.com",
      threshold=100,
      enabled=True,
      keep_inline=keep_inline,
    )

  def test_default_stores_the_url_as_external(
    self, mock_s3_client, entity_data, report_data
  ):
    ext = self._externalizer(mock_s3_client, keep_inline=False)
    text = "<div>" + "narrative " * 40 + "</div>"
    result = ext.queue_value_for_s3(text, "fact-1", entity_data, report_data)
    assert result is not None
    assert result["value_type"] == "external"
    assert result["stored_value"] == result["url"]
    assert result["url"].startswith("https://cdn.example.com/")
    assert len(ext.upload_queue) == 1

  def test_keep_inline_stores_the_text_and_still_uploads(
    self, mock_s3_client, entity_data, report_data
  ):
    ext = self._externalizer(mock_s3_client, keep_inline=True)
    text = "<div>" + "narrative " * 40 + "</div>"
    result = ext.queue_value_for_s3(text, "fact-1", entity_data, report_data)
    assert result is not None
    assert result["value_type"] == "inline"
    assert result["stored_value"] == text
    assert result["url"].startswith("https://cdn.example.com/")
    assert len(ext.upload_queue) == 1  # the CDN copy is still made

  def test_keep_inline_survives_the_content_cache(
    self, mock_s3_client, entity_data, report_data
  ):
    ext = self._externalizer(mock_s3_client, keep_inline=True)
    text = "<p>" + "same note " * 30 + "</p>"
    first = ext.queue_value_for_s3(text, "fact-1", entity_data, report_data)
    second = ext.queue_value_for_s3(text, "fact-2", entity_data, report_data)
    assert first is not None and second is not None
    assert second["stored_value"] == text and second["value_type"] == "inline"
    assert len(ext.upload_queue) == 1  # identical content is uploaded once
