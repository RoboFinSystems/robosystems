"""Tests for the append-only bulk import into a subgraph's tables."""

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.graph import subgraph_import
from robosystems.operations.graph.subgraph_import import (
  SubgraphImportError,
  begin_import,
  import_key,
  run_import,
)

GRAPH = "kg1234567890abcdef_scratch"
USER = "user_1"
SCHEMA = [
  {"name": "Person", "type": "NODE"},
  {"name": "Company", "type": "NODE"},
  {"name": "PERSON_WORKS_AT_COMPANY", "type": "REL"},
]


@pytest.fixture
def graph_client():
  client = MagicMock()
  client.graph_id = None

  async def get_schema():
    # The real client reads the graph it is bound to, and `sec` when unbound.
    assert client.graph_id == GRAPH, f"schema read from {client.graph_id or 'sec'}"
    return SCHEMA

  client.get_schema = AsyncMock(side_effect=get_schema)
  client.create_table = AsyncMock(return_value={"status": "completed"})
  client.materialize_table = AsyncMock(return_value={"rows_ingested": 7})
  client.delete_table = AsyncMock(return_value={})
  client.close = AsyncMock()
  return client


@pytest.fixture
def wired(graph_client):
  """run_import with the Graph API client and S3 replaced."""
  s3 = MagicMock()
  s3.object_exists.return_value = True
  with (
    patch(
      "robosystems.graph_api.client.factory.GraphClientFactory.create_client",
      AsyncMock(return_value=graph_client),
    ),
    patch("robosystems.operations.aws.s3.S3Client", return_value=s3),
  ):
    yield graph_client, s3


class TestBeginImport:
  @pytest.mark.unit
  def test_one_presigned_upload_per_table_under_the_import(self):
    with patch(
      "robosystems.operations.uploads.presign_upload", return_value="https://put"
    ) as presign:
      result = begin_import(graph_id=GRAPH, user_id=USER, tables=["Person", "Company"])

    uuid.UUID(result["import_id"])
    assert [u["table_name"] for u in result["uploads"]] == ["Person", "Company"]
    assert presign.call_args_list[0].args[0] == import_key(
      USER, GRAPH, result["import_id"], "Person"
    )
    assert result["content_type"] == "application/x-parquet"

  @pytest.mark.unit
  @pytest.mark.parametrize(
    "tables",
    [[], ["Bad Name"], ["../Person"], ["Person", "Person"], ["1Person"], [None]],
  )
  def test_bad_table_lists_are_refused(self, tables):
    with pytest.raises(SubgraphImportError):
      begin_import(graph_id=GRAPH, user_id=USER, tables=tables)

  @pytest.mark.unit
  def test_too_many_tables_is_refused(self):
    tables = [f"T{i}" for i in range(subgraph_import.MAX_IMPORT_TABLES + 1)]
    with pytest.raises(SubgraphImportError):
      begin_import(graph_id=GRAPH, user_id=USER, tables=tables)


class TestRunImport:
  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_nodes_are_copied_before_relationships(self, wired):
    client, _ = wired
    result = await run_import(
      graph_id=GRAPH,
      user_id=USER,
      import_id=str(uuid.uuid4()),
      tables=["PERSON_WORKS_AT_COMPANY", "Person", "Company"],
    )

    copied = [c.kwargs["table_name"] for c in client.materialize_table.call_args_list]
    assert copied == ["Person", "Company", "PERSON_WORKS_AT_COMPANY"]
    assert result["done"] is True
    assert result["rows_ingested"] == 21

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_the_copy_appends_and_never_rebuilds(self, wired):
    client, _ = wired
    await run_import(
      graph_id=GRAPH, user_id=USER, import_id=str(uuid.uuid4()), tables=["Person"]
    )

    assert client.materialize_table.call_args.kwargs["incremental"] is True
    client.delete_table.assert_awaited_once_with(GRAPH, "Person")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_the_file_is_read_from_the_callers_own_import_prefix(self, wired):
    client, _ = wired
    import_id = str(uuid.uuid4())
    await run_import(
      graph_id=GRAPH, user_id=USER, import_id=import_id, tables=["Person"]
    )

    (pattern,) = client.create_table.call_args.kwargs["s3_pattern"]
    assert pattern.endswith(import_key(USER, GRAPH, import_id, "Person"))

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_an_unknown_table_and_a_missing_file_are_reported_not_raised(
    self, wired
  ):
    client, s3 = wired
    s3.object_exists.side_effect = lambda bucket, key: "Company" not in key
    result = await run_import(
      graph_id=GRAPH,
      user_id=USER,
      import_id=str(uuid.uuid4()),
      tables=["Person", "Company", "Nope"],
    )

    status = {t["table_name"]: t["status"] for t in result["tables"]}
    assert status == {
      "Nope": "no_such_table",
      "Person": "imported",
      "Company": "not_uploaded",
    }

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_a_failed_copy_still_drops_the_staging_table(self, wired):
    client, _ = wired
    client.materialize_table.side_effect = RuntimeError("copy refused")
    result = await run_import(
      graph_id=GRAPH, user_id=USER, import_id=str(uuid.uuid4()), tables=["Person"]
    )

    assert result["tables"][0]["status"] == "failed"
    assert result["tables"][0]["stage"] == "copy"
    client.delete_table.assert_awaited_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_a_file_that_cannot_be_staged_is_not_copied(self, wired):
    client, _ = wired
    client.create_table.return_value = {"status": "failed", "error": "bad parquet"}
    result = await run_import(
      graph_id=GRAPH, user_id=USER, import_id=str(uuid.uuid4()), tables=["Person"]
    )

    assert result["tables"][0]["stage"] == "staging"
    client.materialize_table.assert_not_awaited()
    client.delete_table.assert_awaited_once_with(GRAPH, "Person")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_a_spent_budget_hands_back_what_is_left(self, wired):
    client, _ = wired
    result = await run_import(
      graph_id=GRAPH,
      user_id=USER,
      import_id=str(uuid.uuid4()),
      tables=["Person", "Company", "PERSON_WORKS_AT_COMPANY"],
      budget_seconds=-1,
    )

    assert [t["table_name"] for t in result["tables"]] == ["Person"]
    assert result["remaining"] == ["Company", "PERSON_WORKS_AT_COMPANY"]
    assert result["done"] is False

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_an_import_id_this_server_did_not_issue_is_refused(self, wired):
    with pytest.raises(SubgraphImportError):
      await run_import(
        graph_id=GRAPH, user_id=USER, import_id="../other", tables=["Person"]
      )
