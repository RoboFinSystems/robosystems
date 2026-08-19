"""Tests for the presign-time file size gate.

Rejecting an over-limit file at presign spares the caller a push to S3 that
`ingest-file` is already certain to refuse. The check is advisory — the caller
declares the size and could under-declare — so the post-upload measurement
remains authoritative and is not replaced by this.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException, status

from robosystems.config.constants import MAX_FILE_SIZE_MB
from robosystems.models.api.graphs.tables import FileUploadRequest
from robosystems.operations.graph.commands.create_file_upload import (
  create_file_upload_cmd,
)

MB = 1024**2


def _request(file_size_bytes: int | None) -> FileUploadRequest:
  return FileUploadRequest(
    file_name="data.csv",
    content_type="text/csv",
    table_name="Entity",
    file_size_bytes=file_size_bytes,
  )


async def _run(file_size_bytes: int | None):
  graph = MagicMock()
  graph.graph_type = "generic"

  with (
    patch(
      "robosystems.middleware.billing.enforcement.require_graph_access",
      return_value=graph,
    ),
    patch(
      "robosystems.operations.graph.commands.create_file_upload.get_universal_repository",
    ) as repo,
  ):
    repo.return_value = MagicMock()
    return await create_file_upload_cmd(
      graph_id="kg123",
      request=_request(file_size_bytes),
      current_user=MagicMock(id="user123"),
      db=MagicMock(),
    )


@pytest.mark.asyncio
async def test_oversized_declared_file_is_refused_before_upload():
  with pytest.raises(HTTPException) as exc:
    await _run((MAX_FILE_SIZE_MB + 1) * MB)

  assert exc.value.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
  assert str(MAX_FILE_SIZE_MB) in str(exc.value.detail)


async def _assert_size_gate_passed(file_size_bytes: int | None) -> None:
  """Run the command and assert only that the size gate did not fire.

  The presign path continues into S3 and response construction, which these
  mocks do not satisfy; any failure past the gate is out of scope here.
  """
  try:
    await _run(file_size_bytes)
  except HTTPException as exc:
    assert exc.status_code != status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, (
      f"size gate rejected {file_size_bytes} bytes but should not have"
    )
  except Exception:
    pass


@pytest.mark.asyncio
async def test_declared_size_at_the_limit_is_allowed_through_the_gate():
  """Exactly at the cap must pass — ingest-file compares with `>`, so this matches."""
  await _assert_size_gate_passed(MAX_FILE_SIZE_MB * MB)


@pytest.mark.asyncio
async def test_omitted_size_skips_the_gate():
  """The field is optional; existing clients that omit it are unaffected."""
  await _assert_size_gate_passed(None)


# ── Signing the declared length into the presigned PUT ──────────────────


async def _presign_params(file_size_bytes: int | None) -> dict:
  """Run the command with S3 stubbed and return the Params it presigned."""
  graph = MagicMock()
  graph.graph_type = "generic"
  s3 = MagicMock()
  s3.s3_client.generate_presigned_url.return_value = "https://s3/put"

  with (
    patch(
      "robosystems.middleware.billing.enforcement.require_graph_access",
      return_value=graph,
    ),
    patch(
      "robosystems.operations.graph.commands.create_file_upload.get_universal_repository",
      return_value=MagicMock(),
    ),
    patch(
      "robosystems.operations.graph.commands.create_file_upload.S3Client",
      return_value=s3,
    ),
    patch("robosystems.models.core.GraphTable.get_by_name", return_value=MagicMock()),
    patch(
      "robosystems.models.core.GraphFile.create",
      return_value=MagicMock(id="file_1"),
    ),
    patch(
      "robosystems.operations.graph.commands.create_file_upload.get_endpoint_metrics",
      return_value=MagicMock(),
    ),
  ):
    await create_file_upload_cmd(
      graph_id="kg123",
      request=_request(file_size_bytes),
      current_user=MagicMock(id="user123"),
      db=MagicMock(),
    )

  s3.s3_client.generate_presigned_url.assert_called_once()
  call = s3.s3_client.generate_presigned_url.call_args
  assert call.args[0] == "put_object"
  return call.kwargs["Params"]


@pytest.mark.asyncio
async def test_declared_size_is_signed_into_the_presigned_put():
  """SigV4 signs Content-Length, so a PUT of any other size fails at S3."""
  params = await _presign_params(12_345)
  assert params["ContentLength"] == 12_345
  assert params["ContentType"] == "text/csv"


@pytest.mark.asyncio
async def test_undeclared_size_leaves_content_length_unsigned():
  """Clients that never send a size (the integration template) keep working."""
  params = await _presign_params(None)
  assert "ContentLength" not in params
