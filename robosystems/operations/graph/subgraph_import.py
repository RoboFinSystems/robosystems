"""Append-only bulk import into a subgraph's own tables.

A subgraph is written to directly (schema DDL plus Cypher), so the file
staging pipeline is refused on it: that pipeline rebuilds the database and
swaps it into place, which would discard those writes. This import adds rows
without a rebuild. Each uploaded Parquet file is staged, copied into the table
of the same name in incremental mode (only rows whose key is not there yet),
and the staging table is dropped.
"""

import re
import time
import uuid
from typing import Any

from robosystems.config import env
from robosystems.config.constants import MAX_FILE_SIZE_MB, PRESIGNED_URL_EXPIRY_SECONDS
from robosystems.logger import logger

PARQUET_CONTENT_TYPE = "application/x-parquet"
MAX_IMPORT_TABLES = 500
# One call works until this budget is spent, then hands back what is left, so
# a long import never outlives a request.
DEFAULT_BUDGET_SECONDS = 40.0

_TABLE_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


class SubgraphImportError(ValueError):
  """The import request cannot be carried out as given."""


def _check_table_names(tables: list[str]) -> list[str]:
  if not tables:
    raise SubgraphImportError("Name at least one table.")
  if len(tables) > MAX_IMPORT_TABLES:
    raise SubgraphImportError(
      f"At most {MAX_IMPORT_TABLES} tables in one import; split it into several."
    )
  seen: set[str] = set()
  for name in tables:
    if not isinstance(name, str) or not _TABLE_NAME.match(name):
      raise SubgraphImportError(
        f"Invalid table name {name!r}: letters, digits and underscores, "
        "starting with a letter."
      )
    if name in seen:
      raise SubgraphImportError(f"Table {name!r} is named twice.")
    seen.add(name)
  return tables


def _check_import_id(import_id: str) -> str:
  try:
    return str(uuid.UUID(str(import_id)))
  except ValueError:
    raise SubgraphImportError("import_id is not one this server issued.")


def import_key(user_id: str, graph_id: str, import_id: str, table_name: str) -> str:
  """Where one table's file sits in the user-data bucket."""
  return f"user-staging/{user_id}/{graph_id}/_imports/{import_id}/{table_name}.parquet"


def begin_import(*, graph_id: str, user_id: str, tables: list[str]) -> dict[str, Any]:
  """Issue an import id and one presigned Parquet upload per table."""
  from robosystems.operations.uploads import presign_upload

  names = _check_table_names(tables)
  import_id = str(uuid.uuid4())
  uploads = [
    {
      "table_name": name,
      "upload_url": presign_upload(
        import_key(user_id, graph_id, import_id, name),
        content_type=PARQUET_CONTENT_TYPE,
        size_bytes=None,
        expires_in=PRESIGNED_URL_EXPIRY_SECONDS,
      ),
    }
    for name in names
  ]
  return {
    "import_id": import_id,
    "content_type": PARQUET_CONTENT_TYPE,
    "expires_in": PRESIGNED_URL_EXPIRY_SECONDS,
    "max_file_mb": MAX_FILE_SIZE_MB,
    "uploads": uploads,
  }


def _order_nodes_first(
  tables: list[str], schema: list[dict[str, Any]]
) -> tuple[list[str], list[str]]:
  """Requested tables with node tables ahead of relationship tables (a
  relationship row needs both its nodes), and the names the graph lacks."""
  kinds = {
    str(table.get("name")): str(table.get("type") or "").upper() for table in schema
  }
  missing = [name for name in tables if name not in kinds]
  known = [name for name in tables if name in kinds]
  return (
    [n for n in known if kinds[n] == "NODE"] + [n for n in known if kinds[n] != "NODE"]
  ), missing


async def _drop_staging(client: Any, graph_id: str, table_name: str) -> None:
  """The staging table is scratch: left behind, the next import of this table
  would copy this file's rows again."""
  try:
    await client.delete_table(graph_id, table_name)
  except Exception as exc:
    logger.warning(f"Could not drop staging table {graph_id}.{table_name}: {exc}")


async def run_import(
  *,
  graph_id: str,
  user_id: str,
  import_id: str,
  tables: list[str],
  budget_seconds: float = DEFAULT_BUDGET_SECONDS,
) -> dict[str, Any]:
  """Copy each uploaded file into its table, node tables first.

  Returns one result per table handled and, under ``remaining``, the tables
  the time budget did not reach. The budget is checked between tables, so a
  table once started is finished: one large file can outrun it. Calling again with ``remaining`` continues;
  repeating a table is safe, since the copy skips rows already present.
  """
  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.middleware.operations import run_off_loop
  from robosystems.operations.aws.s3 import S3Client

  names = _check_table_names(tables)
  import_id = _check_import_id(import_id)
  bucket = env.USER_DATA_BUCKET
  s3 = S3Client()
  started = time.monotonic()

  client = await GraphClientFactory.create_client(
    graph_id=graph_id, operation_type="write"
  )
  results: list[dict[str, Any]] = []
  try:
    ordered, missing = _order_nodes_first(names, await client.get_schema())
    for name in missing:
      results.append(
        {
          "table_name": name,
          "status": "no_such_table",
          "message": "Create it first with add-node-table or add-relationship-table.",
        }
      )

    remaining: list[str] = []
    for index, name in enumerate(ordered):
      if index and time.monotonic() - started > budget_seconds:
        remaining = ordered[index:]
        break

      key = import_key(user_id, graph_id, import_id, name)
      if not await run_off_loop(s3.object_exists, bucket, key):
        results.append(
          {
            "table_name": name,
            "status": "not_uploaded",
            "message": "No file was uploaded for this table.",
          }
        )
        continue

      staged = await client.create_table(
        graph_id=graph_id, table_name=name, s3_pattern=[f"s3://{bucket}/{key}"]
      )
      if staged.get("status") == "failed":
        logger.warning(f"Import staging failed for {graph_id}.{name}: {staged}")
        results.append(
          {
            "table_name": name,
            "status": "failed",
            "stage": "staging",
            "message": str(staged.get("error") or "The file could not be read."),
          }
        )
        await _drop_staging(client, graph_id, name)
        continue

      try:
        copied = await client.materialize_table(
          graph_id=graph_id, table_name=name, incremental=True
        )
        results.append(
          {
            "table_name": name,
            "status": "imported",
            "rows_ingested": int(copied.get("rows_ingested", 0)),
          }
        )
      except Exception as exc:
        logger.warning(f"Import copy failed for {graph_id}.{name}: {exc}")
        results.append(
          {
            "table_name": name,
            "status": "failed",
            "stage": "copy",
            "message": str(getattr(exc, "detail", None) or exc),
          }
        )
      finally:
        await _drop_staging(client, graph_id, name)
  finally:
    await client.close()

  return {
    "import_id": import_id,
    "tables": results,
    "rows_ingested": sum(r.get("rows_ingested", 0) for r in results),
    "remaining": remaining,
    "done": not remaining,
  }
