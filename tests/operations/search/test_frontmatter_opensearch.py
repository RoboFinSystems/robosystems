"""Indexing a document holds its frontmatter to the parse bounds end to end,
against the local OpenSearch (a throwaway index, deleted after)."""

import types
import uuid
from unittest import mock

import pytest

from robosystems.models.api.search import DocumentUploadRequest
from robosystems.operations import document_service as ds
from robosystems.operations.search.client import OpenSearchClient
from robosystems.operations.search.service import SearchService

OPENSEARCH_URL = "http://localhost:9200"


@pytest.fixture
def throwaway_search():
  client = OpenSearchClient(OPENSEARCH_URL, f"test-frontmatter-{uuid.uuid4().hex[:8]}")
  try:
    client.client.info()
  except Exception:
    pytest.skip("local OpenSearch not reachable")
  client.create_index_if_not_exists()
  service = SearchService(client)
  service._embedding_service = types.SimpleNamespace(
    embed_batch=lambda texts: [[0.05] * 384 for _ in texts]
  )
  yield client, service
  client.client.indices.delete(index=client.index_name, ignore=[404])


@pytest.mark.integration
def test_indexed_tags_stay_bounded_for_a_second_frontmatter_block(throwaway_search):
  client, service = throwaway_search
  names = "abcd"
  lines = ["a: &a [" + ", ".join(["xxxxxxxxxx"] * 10) + "]"]
  for i in range(1, 4):
    lines.append(
      f"{names[i]}: &{names[i]} [" + ", ".join([f"*{names[i - 1]}"] * 10) + "]"
    )
  content = (
    "---\ntitle: Quarterly memo\n---\n---\n"
    + "\n".join(lines)
    + "\ntags: [*d]\n---\n# Memo\n\n"
    + "word " * 40
  )

  def fake_create(**kw):
    return types.SimpleNamespace(
      id="doc1", is_file=False, update=lambda *a, **k: None, **kw
    )

  with (
    mock.patch.object(ds.Document, "create", side_effect=fake_create),
    mock.patch(
      "robosystems.operations.search.get_search_service", return_value=service
    ),
  ):
    ds.DocumentService(session=None).create_document(
      "kg_frontmatter_bound",
      "user1",
      DocumentUploadRequest(title="Quarterly memo", content=content),
    )

  client.client.indices.refresh(index=client.index_name)
  tags = client.client.get(index=client.index_name, id="udoc_doc1_0")["_source"].get(
    "tags"
  )
  assert tags is None or (len(tags) <= 50 and all(isinstance(t, str) for t in tags))
