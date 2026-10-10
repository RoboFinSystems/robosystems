"""What every presigned upload shares: the file name rules and the URL.

A client uploads straight to the user-data bucket with a presigned PUT, then
calls a second operation that acts on what landed. Table files
(``create-file-upload`` → ``ingest-file``) and document files
(``create-document-upload`` → ``complete-document-upload``) both work that
way; what the second step does with the bytes is theirs.
"""

from __future__ import annotations

from pathlib import PurePosixPath

MAX_FILE_NAME_LENGTH = 255


class UploadNameError(ValueError):
  """The file name cannot be used as given."""


def check_upload_file_name(name: str) -> None:
  """Refuse a name that is not a plain file name.

  It becomes the last segment of an object key and, on download, a quoted
  header value: no path, no leading dot, no quotes, backslashes or control
  characters. Raises `UploadNameError`.
  """
  if not name or len(name) > MAX_FILE_NAME_LENGTH:
    raise UploadNameError(
      f"File name must be between 1 and {MAX_FILE_NAME_LENGTH} characters"
    )
  if (
    PurePosixPath(name).name != name
    or name.startswith(".")
    or any(ch in '"\\' or ord(ch) < 32 or ord(ch) == 127 for ch in name)
  ):
    raise UploadNameError(
      "File name contains invalid characters: it must be a plain name, with "
      "no path, leading dot, quotes, backslashes or control characters"
    )


def presign_upload(
  key: str,
  *,
  content_type: str,
  size_bytes: int | None,
  expires_in: int,
) -> str:
  """A presigned PUT to ``key`` in the user-data bucket.

  The type is signed in, and so is the size when the caller declares one, so
  a PUT of anything else fails at S3. Signed for the address a browser
  reaches, which differs from the API's own only in local development.
  """
  from robosystems.config import env
  from robosystems.operations.aws.s3 import S3Client

  return S3Client().generate_presigned_put_url(
    env.USER_DATA_BUCKET,
    key,
    content_type=content_type,
    content_length=size_bytes,
    expires_in=expires_in,
  )
