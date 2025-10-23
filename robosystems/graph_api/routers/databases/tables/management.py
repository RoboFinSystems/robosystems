from typing import List

from fastapi import APIRouter, HTTPException, Path, Body
from fastapi import status as http_status

from robosystems.graph_api.core.duckdb_manager import (
  DuckDBTableManager,
  TableCreateRequest,
  TableCreateResponse,
  TableInfo,
)
from robosystems.logger import logger

router = APIRouter(prefix="/databases/{graph_id}/tables")

table_manager = DuckDBTableManager()


@router.post("", response_model=TableCreateResponse)
async def create_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  request: TableCreateRequest = Body(...),
) -> TableCreateResponse:
  logger.info(
    f"Creating table {request.table_name} for graph {graph_id} from {request.s3_pattern}"
  )

  request.graph_id = graph_id

  try:
    return table_manager.create_table(request)
  except Exception as e:
    logger.error(f"Failed to create table {request.table_name}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to create table: {str(e)}",
    )


@router.get("", response_model=List[TableInfo])
async def list_tables(
  graph_id: str = Path(..., description="Graph database identifier"),
) -> List[TableInfo]:
  logger.info(f"Listing tables for graph {graph_id}")

  try:
    return table_manager.list_tables(graph_id)
  except Exception as e:
    logger.error(f"Failed to list tables for graph {graph_id}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to list tables: {str(e)}",
    )


@router.delete("/{table_name}")
async def delete_table(
  graph_id: str = Path(..., description="Graph database identifier"),
  table_name: str = Path(..., description="Table name"),
) -> dict:
  logger.info(f"Deleting table {table_name} from graph {graph_id}")

  try:
    return table_manager.delete_table(graph_id, table_name)
  except Exception as e:
    logger.error(f"Failed to delete table {table_name}: {e}")
    raise HTTPException(
      status_code=http_status.HTTP_500_INTERNAL_SERVER_ERROR,
      detail=f"Failed to delete table: {str(e)}",
    )
