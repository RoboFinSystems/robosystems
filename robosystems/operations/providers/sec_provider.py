"""SEC connection provider — CIK registration and EDGAR filing sync."""

from typing import Any

import httpx
from sqlalchemy.orm import Session

from ...adapters.sec.config import SEC_VALIDATE_CIK
from ...config import env
from ...logger import logger
from ...middleware.graph import get_graph_repository
from ...models.api.graphs.connections import SECConnectionConfig
from ...operations.connection_service import ConnectionService


async def validate_cik_with_sec_api(cik: str) -> dict[str, Any]:
  """Look up a 10-digit CIK at SEC EDGAR, returning `is_valid` plus what's known.

  Tries the ticker index first (one request covers every listed filer), then
  falls back to the per-CIK submissions endpoint, which also carries SIC. A CIK
  missing from both is reported invalid; an unreachable SEC raises.
  """
  try:
    url = "https://www.sec.gov/files/entity_tickers.json"

    headers = {
      "User-Agent": env.SEC_GOV_USER_AGENT,
      "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
      response = await client.get(url, headers=headers)
      response.raise_for_status()

      companies_data = response.json()

      target_cik = int(cik)  # the ticker index stores cik_str as an int

      for entry in companies_data.values():
        if entry.get("cik_str") == target_cik:
          return {
            "is_valid": True,
            "cik": cik,
            "entity_name": entry.get("title"),
            "ticker": entry.get("ticker"),
            "sic": None,
            "sic_description": None,
          }

      submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"

      try:
        sub_response = await client.get(submissions_url, headers=headers)
        if sub_response.status_code == 200:
          sub_data = sub_response.json()
          return {
            "is_valid": True,
            "cik": cik,
            "entity_name": sub_data.get("name"),
            "ticker": sub_data.get("tickers", [None])[0]
            if sub_data.get("tickers")
            else None,
            "sic": sub_data.get("sic"),
            "sic_description": sub_data.get("sicDescription"),
          }
        else:
          return {
            "is_valid": False,
            "cik": cik,
            "error": f"CIK {cik} not found in SEC database",
          }

      except httpx.HTTPError:
        # Absent from the ticker index and the submissions endpoint is
        # unreachable — treat as invalid rather than block registration.
        return {
          "is_valid": False,
          "cik": cik,
          "error": f"CIK {cik} not found in SEC database",
        }

  except httpx.HTTPError as http_error:
    logger.error(f"SEC API request failed: {http_error}")
    raise Exception(f"SEC API unavailable: {http_error}")

  except Exception as e:
    logger.error(f"CIK validation error: {e}")
    raise Exception(f"CIK validation failed: {e}")


async def get_sec_filing_count(cik: str, graph_id: str | None = None) -> int:
  """Approximate how many EDGAR filings a CIK has.

  The submissions endpoint returns recent filings inline and older ones as
  paged files, so anything beyond the recent page is estimated at 100 filings
  per page. Good enough for sizing a sync, not for reporting. Returns 0 rather
  than raising when SEC is unavailable.
  """
  try:
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
      "User-Agent": env.SEC_GOV_USER_AGENT,
      "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
      response = await client.get(url, headers=headers)
      if response.status_code == 200:
        data = response.json()

        filings = data.get("filings", {})
        recent_count = 0

        if "recent" in filings:
          recent_filings = filings["recent"]
          forms = recent_filings.get("form", [])
          recent_count = len(forms)

        files = filings.get("files", [])
        total_files = len(files)

        estimated_total = recent_count + (total_files * 100)

        logger.info(
          f"Found {recent_count} recent filings + {total_files} file batches for CIK {cik}"
        )
        return (
          max(recent_count, estimated_total)
          if estimated_total > recent_count
          else recent_count
        )

  except Exception as api_error:
    logger.warning(f"SEC API filing count failed for CIK {cik}: {api_error}")

  return 0


async def create_sec_connection(
  entity_id: str | None,
  config: SECConnectionConfig,
  user_id: str,
  graph_id: str,
  db: Session,
) -> str:
  """Create SEC connection.

  Only requires a CIK. The entity is created by the SEC sync pipeline
  when it processes XBRL filings — not assumed to exist in the graph.

  Optionally validates the CIK with SEC API to get the entity name.
  """
  entity_name = None

  if SEC_VALIDATE_CIK:
    try:
      cik_info = await validate_cik_with_sec_api(config.cik)
      if not cik_info["is_valid"]:
        logger.warning(f"CIK {config.cik} not found in SEC database")
      else:
        entity_name = cik_info.get("entity_name")
    except Exception as e:
      logger.warning(f"SEC CIK validation failed: {e}")

  metadata = {
    "cik": config.cik,
    "entity_name": entity_name,
  }

  connection_data = await ConnectionService.create_connection(
    entity_id=entity_id or "",
    provider="sec",
    user_id=user_id,
    credentials={"cik": config.cik},
    metadata=metadata,
    graph_id=graph_id,
  )

  return connection_data["connection_id"]


async def sync_sec_connection(
  connection: dict[str, Any], sync_options: dict[str, Any] | None, graph_id: str
) -> str:
  """Submit the `sec_entity_sync` Dagster job; returns its run id.

  The job discovers the CIK's filings in the shared raw S3 bucket, processes
  XBRL into parquet, and loads it into `graph_id` — which must already exist
  with the RoboLedger schema. `sync_options` accepts `form_types` and
  `skip_enrichment`.
  """
  from robosystems.middleware.sse.dagster_monitor import submit_dagster_job_sync

  metadata = connection.get("metadata", {}) or {}
  options = sync_options or {}

  cik = metadata.get("cik", "")
  connection_id = connection.get("connection_id", "")
  user_id = connection.get("user_id", "")

  if not cik:
    raise ValueError("SEC CIK not found in connection metadata")

  # Every asset in the job shares one SECEntitySyncConfig, so the same dict
  # is bound to each op below.
  sync_config = {
    "graph_id": graph_id,
    "connection_id": connection_id,
    "user_id": user_id,
    "cik": cik,
    "form_types": options.get("form_types", ["10-K", "10-Q", "20-F", "40-F"]),
    "skip_enrichment": options.get("skip_enrichment", True),
  }

  run_config = {
    "ops": {
      "sec_entity_extract": {"config": sync_config},
      "sec_entity_transform": {"config": sync_config},
      "sec_entity_load": {"config": sync_config},
    }
  }

  run_id = submit_dagster_job_sync(
    job_name="sec_entity_sync",
    run_config=run_config,
    tags={
      "graph_id": graph_id,
      "connection_id": connection_id,
      "cik": cik,
      "pipeline": "sec_entity",
    },
  )

  logger.info(
    f"SEC entity sync submitted for graph={graph_id}, "
    f"cik={cik}, connection={connection_id}, run_id={run_id}"
  )
  return run_id


async def cleanup_sec_connection(connection: dict[str, Any], graph_id: str) -> None:
  """Clear the CIK from the entity; already-loaded filing data is left in place."""
  repository = await get_graph_repository(graph_id, operation_type="write")
  update_query = """
    MATCH (c:Entity {identifier: $entity_id})
    SET c.cik = null
    RETURN c.identifier as identifier
    """
  repository.execute_single(update_query, {"entity_id": connection["entity_id"]})
