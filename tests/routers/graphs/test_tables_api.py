"""Tests for main API tables and files routes."""

from types import SimpleNamespace

import pytest

from robosystems.models.api.graphs.tables import FileUploadRequest
from robosystems.routers.graphs.files import upload as files_upload
from robosystems.routers.graphs.tables import main as tables_main


@pytest.mark.asyncio
async def test_list_tables_success(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    tables_main,
    "get_universal_repository",
    fake_repo,
  )

  table_records = [
    SimpleNamespace(
      table_name="Entity",
      row_count=10,
      file_count=2,
      total_size_bytes=2048,
    )
  ]

  monkeypatch.setattr(
    tables_main.GraphTable,
    "get_all_for_graph",
    classmethod(lambda cls, graph_id, session: table_records),
  )

  class FakeQuery:
    def __init__(self, value):
      self._value = value

    def filter(self, *args, **kwargs):
      return self

    def first(self):
      return SimpleNamespace(user_id="user-123")

  fake_db = SimpleNamespace(query=lambda model: FakeQuery(model))

  class FakeTableService:
    def __init__(self, session):
      self.session = session

    def get_s3_pattern_for_table(self, graph_id, table_name, user_id):
      return f"s3://bucket/{user_id}/{graph_id}/{table_name}/**/*.parquet"

  monkeypatch.setattr(
    "robosystems.operations.graph.table_service.TableService",
    FakeTableService,
  )

  result = await tables_main.list_tables(
    graph_id="graph-123",
    current_user=SimpleNamespace(id="user-123"),
    _rate_limit=None,
    db=fake_db,
  )

  assert result.total_count == 1
  assert result.tables[0].table_name == "Entity"
  assert result.tables[0].s3_location.startswith("s3://")


@pytest.mark.asyncio
async def test_list_tables_not_found(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return None

  monkeypatch.setattr(
    tables_main,
    "get_universal_repository",
    fake_repo,
  )

  with pytest.raises(Exception) as exc:
    await tables_main.list_tables(
      graph_id="graph-404",
      current_user=SimpleNamespace(id="user-123"),
      _rate_limit=None,
      db=SimpleNamespace(),
    )

  assert getattr(exc.value, "status_code", None) == 404


@pytest.mark.asyncio
async def test_get_upload_url_rejects_extension(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_upload,
    "get_universal_repository",
    fake_repo,
  )

  monkeypatch.setattr(
    files_upload.GraphTable,
    "get_by_name",
    classmethod(
      lambda cls, graph_id, table_name, session: SimpleNamespace(id="table-1")
    ),
  )

  request = FileUploadRequest(
    file_name="data.csv",
    content_type="application/x-parquet",
    table_name="Entity",
  )

  with pytest.raises(Exception) as exc:
    await files_upload.create_file_upload(
      graph_id="graph-123",
      request=request,
      current_user=SimpleNamespace(id="user-1"),
      _rate_limit=None,
      db=SimpleNamespace(),
    )

  assert getattr(exc.value, "status_code", None) == 400


@pytest.mark.asyncio
async def test_get_upload_url_success(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_upload,
    "get_universal_repository",
    fake_repo,
  )

  table_record = SimpleNamespace(id="table-1")
  monkeypatch.setattr(
    files_upload.GraphTable,
    "get_by_name",
    classmethod(lambda cls, graph_id, table_name, session: table_record),
  )

  monkeypatch.setattr(
    files_upload.GraphFile,
    "create",
    classmethod(lambda cls, **kwargs: SimpleNamespace(id="file-123")),
  )

  class FakeS3Client:
    def __init__(self):
      self.s3_client = SimpleNamespace(
        generate_presigned_url=lambda *args, **kwargs: "https://upload.test"
      )

  monkeypatch.setattr(files_upload, "S3Client", FakeS3Client)
  monkeypatch.setattr(files_upload.env, "AWS_S3_BUCKET", "bucket")

  class _StubUUID:
    def __str__(self):
      return "uuid-1"

  monkeypatch.setattr("uuid.uuid4", lambda: _StubUUID())

  request = FileUploadRequest(
    file_name="data.parquet",
    content_type="application/x-parquet",
    table_name="Entity",
  )

  result = await files_upload.create_file_upload(
    graph_id="graph-123",
    request=request,
    current_user=SimpleNamespace(id="user-1"),
    _rate_limit=None,
    db=SimpleNamespace(),
  )

  assert result.upload_url.startswith("https://")
  assert result.s3_key.endswith("data.parquet")
