from datetime import UTC
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from robosystems.models.api.graphs.tables import FileUploadRequest
from robosystems.routers.graphs.files import main as files_router


@pytest.mark.asyncio
async def test_list_files_all_in_graph(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_router,
    "get_universal_repository",
    fake_repo,
  )

  file_records = [
    SimpleNamespace(
      id="file-1",
      file_name="data1.parquet",
      file_format="parquet",
      file_size_bytes=1024,
      row_count=100,
      upload_status="uploaded",
      upload_method="presigned",
      created_at=None,
      uploaded_at=None,
      s3_key="user/graph/table/data1.parquet",
    ),
    SimpleNamespace(
      id="file-2",
      file_name="data2.parquet",
      file_format="parquet",
      file_size_bytes=2048,
      row_count=200,
      upload_status="uploaded",
      upload_method="presigned",
      created_at=None,
      uploaded_at=None,
      s3_key="user/graph/table/data2.parquet",
    ),
  ]

  class FakeQuery:
    def filter(self, *args, **kwargs):
      return self

    def all(self):
      return file_records

  fake_db = SimpleNamespace(query=lambda model: FakeQuery())

  result = await files_router.list_files(
    graph_id="graph-123",
    table_name=None,
    file_status=None,
    current_user=SimpleNamespace(id="user-123"),
    _rate_limit=None,
    db=fake_db,
  )

  assert result.graph_id == "graph-123"
  assert result.total_files == 2
  assert result.total_size_bytes == 3072
  assert len(result.files) == 2


@pytest.mark.asyncio
async def test_list_files_filtered_by_table(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_router,
    "get_universal_repository",
    fake_repo,
  )

  table_record = SimpleNamespace(id="table-123", table_name="Entity")
  monkeypatch.setattr(
    files_router.GraphTable,
    "get_by_name",
    classmethod(lambda cls, graph_id, table_name, session: table_record),
  )

  file_records = [
    SimpleNamespace(
      id="file-1",
      file_name="entities.parquet",
      file_format="parquet",
      file_size_bytes=1024,
      row_count=100,
      upload_status="uploaded",
      upload_method="presigned",
      created_at=None,
      uploaded_at=None,
      s3_key="user/graph/Entity/entities.parquet",
    ),
  ]

  monkeypatch.setattr(
    files_router.GraphFile,
    "get_all_for_table",
    classmethod(lambda cls, table_id, session: file_records),
  )

  fake_db = SimpleNamespace()

  result = await files_router.list_files(
    graph_id="graph-123",
    table_name="Entity",
    file_status=None,
    current_user=SimpleNamespace(id="user-123"),
    _rate_limit=None,
    db=fake_db,
  )

  assert result.table_name == "Entity"
  assert result.total_files == 1
  assert result.files[0].file_name == "entities.parquet"


@pytest.mark.asyncio
async def test_get_file_returns_enhanced_status(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_router,
    "get_universal_repository",
    fake_repo,
  )

  from datetime import datetime

  uploaded_time = datetime(2025, 1, 20, 10, 0, 0, tzinfo=UTC)
  duckdb_time = datetime(2025, 1, 20, 10, 1, 0, tzinfo=UTC)
  graph_time = datetime(2025, 1, 20, 10, 5, 0, tzinfo=UTC)

  file_record = SimpleNamespace(
    id="file-123",
    graph_id="graph-123",
    table_id="table-456",
    file_name="data.parquet",
    file_format="parquet",
    file_size_bytes=1048576,
    row_count=5000,
    upload_status="uploaded",
    upload_method="presigned",
    created_at=uploaded_time,
    uploaded_at=uploaded_time,
    s3_key="user/graph/table/data.parquet",
    duckdb_status="staged",
    duckdb_row_count=5000,
    duckdb_staged_at=duckdb_time,
    graph_status="ingested",
    graph_ingested_at=graph_time,
  )

  table_record = SimpleNamespace(id="table-456", table_name="Entity")

  monkeypatch.setattr(
    files_router.GraphFile,
    "get_by_id",
    classmethod(lambda cls, file_id, session: file_record),
  )

  monkeypatch.setattr(
    files_router.GraphTable,
    "get_by_id",
    classmethod(lambda cls, table_id, session: table_record),
  )

  fake_db = SimpleNamespace()

  result = await files_router.get_file(
    graph_id="graph-123",
    file_id="file-123",
    current_user=SimpleNamespace(id="user-123"),
    _rate_limit=None,
    db=fake_db,
  )

  assert result.file_id == "file-123"
  assert result.layers is not None
  assert result.layers.s3.status == "uploaded"
  assert result.layers.s3.size_bytes == 1048576
  assert result.layers.s3.row_count == 5000
  assert result.layers.duckdb.status == "staged"
  assert result.layers.duckdb.row_count == 5000
  assert result.layers.graph.status == "ingested"


@pytest.mark.asyncio
async def test_get_file_not_found(monkeypatch):
  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    files_router,
    "get_universal_repository",
    fake_repo,
  )

  monkeypatch.setattr(
    files_router.GraphFile,
    "get_by_id",
    classmethod(lambda cls, file_id, session: None),
  )

  fake_db = SimpleNamespace()

  with pytest.raises(Exception) as exc:
    await files_router.get_file(
      graph_id="graph-123",
      file_id="file-404",
      current_user=SimpleNamespace(id="user-123"),
      _rate_limit=None,
      db=fake_db,
    )

  assert getattr(exc.value, "status_code", None) == 404


@pytest.mark.asyncio
async def test_create_file_upload_generates_presigned_url(monkeypatch):
  from robosystems.routers.graphs.files import upload as upload_router

  async def fake_repo(*args, **kwargs):
    return SimpleNamespace()

  monkeypatch.setattr(
    upload_router,
    "get_universal_repository",
    fake_repo,
  )

  mock_table = SimpleNamespace(id="table-123", table_name="Entity")
  monkeypatch.setattr(
    upload_router.GraphTable,
    "get_by_name",
    classmethod(lambda cls, graph_id, table_name, session: mock_table),
  )

  mock_file = SimpleNamespace(id="file-123")
  monkeypatch.setattr(
    upload_router.GraphFile,
    "create",
    classmethod(lambda cls, **kwargs: mock_file),
  )

  with patch("robosystems.routers.graphs.files.upload.S3Client") as mock_s3_class:
    mock_s3_instance = SimpleNamespace()
    mock_s3_instance.s3_client = SimpleNamespace()
    mock_s3_instance.s3_client.generate_presigned_url = (
      lambda *args, **kwargs: "https://s3.url"
    )
    mock_s3_class.return_value = mock_s3_instance

    request = FileUploadRequest(
      file_name="data.parquet",
      content_type="application/x-parquet",
      table_name="Entity",
    )

    result = await upload_router.create_file_upload(
      graph_id="graph-123",
      request=request,
      current_user=SimpleNamespace(id="user-123"),
      _rate_limit=None,
      db=SimpleNamespace(),
    )

    assert result.file_id == "file-123"
    assert result.upload_url == "https://s3.url"


@pytest.mark.asyncio
async def test_create_file_upload_requires_table_name(monkeypatch):
  from robosystems.routers.graphs.files import upload as upload_router

  request = FileUploadRequest(
    file_name="data.parquet",
    content_type="application/x-parquet",
  )

  with pytest.raises(Exception) as exc:
    await upload_router.create_file_upload(
      graph_id="graph-123",
      request=request,
      current_user=SimpleNamespace(id="user-123"),
      _rate_limit=None,
      db=SimpleNamespace(),
    )

  assert getattr(exc.value, "status_code", None) == 400
  assert "table_name is required" in str(exc.value.detail)
