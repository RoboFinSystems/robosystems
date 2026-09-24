"""Per-filing S3 zip cache of parquet outputs, so a spot-interrupted run resumes
without reprocessing.

A single-part PUT is atomic, so an entry is complete or absent; no marker file.
Keys are ``sec/cache/{partition_date}/{source_file_id}.zip``.
"""

import io
import zipfile
from pathlib import Path

from botocore.exceptions import ClientError


def cache_exists(s3_client, bucket: str, cache_key: str) -> bool:
  """Check if a cache entry exists on S3 via HEAD request."""
  try:
    s3_client.head_object(Bucket=bucket, Key=cache_key)
    return True
  except ClientError as e:
    if e.response["Error"]["Code"] == "404":
      return False
    raise


def zip_and_upload(
  s3_client,
  bucket: str,
  cache_key: str,
  tables: dict[str, bytes],
) -> None:
  """Zip a filing's parquet tables and upload as a single S3 object.

  `tables` maps a table_key like "nodes/Entity" to its parquet bytes.
  """
  buf = io.BytesIO()
  with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for table_key, parquet_bytes in tables.items():
      zf.writestr(f"{table_key}.parquet", parquet_bytes)
  buf.seek(0)

  s3_client.put_object(Bucket=bucket, Key=cache_key, Body=buf.getvalue())


def download_and_extract(
  s3_client,
  bucket: str,
  cache_key: str,
  work_dir: Path,
  source_file_id: str,
) -> None:
  """Download a cached zip from S3 and extract parquet files to the work directory.

  Extracts files into the same structure that process_single_filing_to_memory
  would produce: work_dir/{entity_type}/{table_name}/{source_file_id}.parquet
  """
  response = s3_client.get_object(Bucket=bucket, Key=cache_key)
  zip_bytes = response["Body"].read()

  with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
    for name in zf.namelist():
      # Entries are named "{table_key}.parquet", e.g. "nodes/Entity.parquet".
      if not name.endswith(".parquet"):
        continue
      table_key = name.removesuffix(".parquet")
      table_dir = (work_dir / table_key).resolve()
      if not str(table_dir).startswith(str(work_dir.resolve())):
        continue
      table_dir.mkdir(parents=True, exist_ok=True)
      parquet_path = table_dir / f"{source_file_id}.parquet"
      parquet_path.write_bytes(zf.read(name))


def delete_cache_keys(
  s3_client,
  bucket: str,
  cache_keys: list[str],
) -> int:
  """Bulk delete cache entries, returning the number of objects deleted."""
  if not cache_keys:
    return 0

  deleted = 0
  # S3 bulk delete accepts at most 1000 keys per request.
  for i in range(0, len(cache_keys), 1000):
    batch = cache_keys[i : i + 1000]
    response = s3_client.delete_objects(
      Bucket=bucket,
      Delete={"Objects": [{"Key": key} for key in batch], "Quiet": True},
    )
    deleted += len(batch) - len(response.get("Errors", []))

  return deleted
