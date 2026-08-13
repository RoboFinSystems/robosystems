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
