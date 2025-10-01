"""SEC provider-specific operations."""

import httpx
from typing import Optional, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session

from ...logger import logger
from ...middleware.graph import get_graph_repository
from ...operations.connection_service import ConnectionService
from ...middleware.graph.multitenant_utils import MultiTenantUtils
from ...models.api.connection import SECConnectionConfig
from ...config import env


async def validate_cik_with_sec_api(cik: str) -> Dict[str, Any]:
  """
  Validate CIK with SEC EDGAR API and get entity information.

  Args:
      cik: 10-digit CIK number

  Returns:
      Dict with validation results and entity info
  """
  try:
    # SEC Entity Tickers API endpoint
    url = "https://www.sec.gov/files/entity_tickers.json"

    headers = {
      "User-Agent": env.SEC_GOV_USER_AGENT,
      "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
      response = await client.get(url, headers=headers)
      response.raise_for_status()

      companies_data = response.json()

      # Search for the CIK in the entity tickers data
      target_cik = int(cik)  # Convert to int for comparison

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

      # If not found in tickers, try the submissions API for more detailed info
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
          # CIK not found
          return {
            "is_valid": False,
            "cik": cik,
            "error": f"CIK {cik} not found in SEC database",
          }

      except httpx.HTTPError:
        # If submissions API fails, but CIK wasn't in tickers, assume invalid
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


async def get_sec_filing_count(cik: str, graph_id: Optional[str] = None) -> int:
  """
  Get the count of SEC filings for a CIK.

  Args:
      cik: 10-digit CIK number
      graph_id: Optional graph ID for checking local database

  Returns:
      Number of filings found
  """
  try:
    # Try SEC submissions API for recent count
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {
      "User-Agent": env.SEC_GOV_USER_AGENT,
      "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
      response = await client.get(url, headers=headers)
      if response.status_code == 200:
        data = response.json()

        # Count recent filings
        filings = data.get("filings", {})
        recent_count = 0

        # Count entries in the recent section
        if "recent" in filings:
          recent_filings = filings["recent"]
          forms = recent_filings.get("form", [])
          recent_count = len(forms)

        # Also count older filings if available
        files = filings.get("files", [])
        total_files = len(files)

        estimated_total = recent_count + (total_files * 100)  # Rough estimate

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

  # If all else fails, return 0
  return 0


async def create_sec_connection(
  entity_id: str, config: SECConnectionConfig, user_id: str, graph_id: str, db: Session
) -> str:
  """Create SEC connection."""
  # Update entity with CIK
  repository = await get_graph_repository(graph_id, operation_type="write")

  # Verify entity exists
  entity_query = """
    MATCH (c:Entity {identifier: $entity_id})
    RETURN c.identifier as identifier, c.name as name
    """
  entity_result = repository.execute_single(entity_query, {"entity_id": entity_id})

  if not entity_result:
    raise HTTPException(status_code=404, detail="Entity not found")

  # Optionally validate CIK with SEC API
  if env.SEC_VALIDATE_CIK:
    try:
      cik_info = await validate_cik_with_sec_api(config.cik)
      if not cik_info["is_valid"]:
        logger.warning(f"CIK {config.cik} not found in SEC database")
      elif not config.entity_name and cik_info.get("entity_name"):
        config.entity_name = cik_info["entity_name"]
    except Exception as e:
      logger.warning(f"SEC CIK validation failed: {e}")
      # Continue without validation

  # Store connection
  credentials = {"cik": config.cik}
  metadata = {
    "cik": config.cik,
    "entity_name": config.entity_name or entity_result.get("name"),
  }

  connection_data = await ConnectionService.create_connection(
    entity_id=entity_id,
    provider="SEC",
    user_id=user_id,
    credentials=credentials,
    metadata=metadata,
    graph_id=graph_id,
  )

  # Update entity with CIK
  update_query = """
    MATCH (c:Entity {identifier: $entity_id})
    SET c.cik = $cik, c.database = $database_name
    RETURN c.identifier as identifier
    """
  repository.execute_single(
    update_query,
    {
      "entity_id": entity_id,
      "cik": config.cik,
      "database_name": MultiTenantUtils.get_database_name(graph_id),
    },
  )

  return connection_data["connection_id"]


async def sync_sec_connection(
  connection: Dict[str, Any], sync_options: Optional[Dict[str, Any]], graph_id: str
) -> str:
  """Trigger SEC filing sync."""

  # TODO: Rebuild individual entity SEC sync to work with new orchestration system
  # The old pipeline.orchestrate_bulk_pipeline is obsolete and doesn't align with
  # the new phased orchestration approach. Need to implement a single-company
  # loader that works with the connection system.
  #
  # Requirements for new implementation:
  # 1. Use the company's CIK from the connection
  # 2. Load data into the entity's specific graph (not the shared SEC repo)
  # 3. Work with the new phased orchestration if possible
  # 4. Support sync_options like year, max_filings, force_update
  #
  # For now, individual SEC syncing is disabled.
  raise NotImplementedError(
    "Individual SEC entity syncing is temporarily disabled. "
    "Use 'just sec-load' for testing or the SEC orchestrator for batch processing."
  )


async def cleanup_sec_connection(connection: Dict[str, Any], graph_id: str) -> None:
  """Clean up SEC-specific data when connection is deleted."""
  # Remove CIK from entity record
  repository = await get_graph_repository(graph_id, operation_type="write")
  update_query = """
    MATCH (c:Entity {identifier: $entity_id})
    SET c.cik = null
    RETURN c.identifier as identifier
    """
  repository.execute_single(update_query, {"entity_id": connection["entity_id"]})
