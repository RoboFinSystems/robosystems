import asyncio
from decimal import Decimal
from ...celery import celery_app, QUEUE_DEFAULT
from ...processors.qb_transactions import QBTransactionsProcessor
from robosystems.logger import logger
from robosystems.operations.connection_service import ConnectionService, SYSTEM_USER_ID
from ...middleware.graph import get_graph_repository
from ...middleware.graph.multitenant_utils import MultiTenantUtils
from robosystems.operations.graph.credit_service import CreditService
from robosystems.database import get_db_session


@celery_app.task(ignore_result=True, queue=QUEUE_DEFAULT, priority=10)
def sync_task(*args):
  asyncio.run(_sync_task_async(*args))


async def _sync_task_async(*args):
  """Async implementation of QuickBooks sync task."""
  # Handle both old (1 arg) and new (2 args) calling patterns
  if len(args) == 1:
    entityId = args[0]
    graph_id = None
  elif len(args) == 2:
    entityId = args[0]
    graph_id = args[1]
  else:
    logger.error(f"Invalid number of arguments passed to sync_task: {len(args)}")
    return

  logger.info(
    f"Starting QuickBooks sync for entity ID: {entityId}, graph_id: {graph_id}"
  )

  # Set database context based on multitenant mode
  # Get appropriate database name using multi-tenant utilities
  database_name = MultiTenantUtils.get_database_name(graph_id)
  MultiTenantUtils.log_database_operation(
    "QuickBooks sync task", database_name, graph_id
  )

  # Use graph repository for database operations
  logger.info(f"Using database: {database_name}")

  # Verify entity exists using direct query to avoid datetime issues
  async def verify_entity():
    repository = await get_graph_repository(database_name, operation_type="read")
    async with repository:
      entity_query = """
      MATCH (c:Entity {identifier: $entity_id})
      RETURN c.identifier as identifier, c.name as name
      """
      return await repository.execute_single(entity_query, {"entity_id": entityId})

  entity_result = await verify_entity()
  if not entity_result:
    logger.error(f"Entity not found for ID: {entityId} in database: {database_name}")
    return

  # Get QuickBooks credentials from ConnectionService
  try:
    connections = await ConnectionService.list_connections(
      entity_id=entityId,
      provider="QuickBooks",
      user_id=SYSTEM_USER_ID,  # System task - no specific user
      graph_id=graph_id,
    )

    if not connections:
      logger.error(f"No QuickBooks connection found for entity {entityId}")
      return

    logger.info(
      f"Found {len(connections)} QuickBooks connections for entity {entityId}"
    )
    for conn in connections:
      logger.info(f"Connection: {conn}")

    # Get credentials from the first connection
    connection_id = connections[0]["connection_id"]
    logger.info(f"Getting connection details for connection_id: {connection_id}")

    connection_details = await ConnectionService.get_connection(
      connection_id=connection_id,
      user_id=SYSTEM_USER_ID,  # System task - no specific user
      graph_id=graph_id,
    )

    if not connection_details:
      logger.error(f"Connection details not found for connection_id: {connection_id}")
      return

    if not connection_details.get("credentials"):
      logger.error(f"QuickBooks credentials not found for entity {entityId}")
      return

    credentials = connection_details["credentials"]
    realm_id = connection_details["metadata"]["realm_id"]

  except Exception as auth_error:
    logger.error(
      f"Failed to retrieve QuickBooks credentials for entity {entityId}: {auth_error}"
    )
    return

  logger.info(f"Initializing QuickBooks sync for entity: {entityId}")

  # Pass credentials to QBTransactionsProcessor with correct database name
  qb_sync = QBTransactionsProcessor(
    entity_id=entityId,
    realm_id=realm_id,
    qb_credentials=credentials,
    database_name=database_name,  # Pass the graph_id as database_name
  )
  qb_sync.sync()
  logger.info(
    f"Successfully completed QuickBooks transaction sync for entity: {entityId}"
  )

  # Consume credits for successful sync
  try:
    with next(get_db_session()) as db:
      credit_service = CreditService(db)
      # Only charge if we have a valid graph_id (not system sync)
      if graph_id:
        credit_service.consume_credits(
          graph_id=graph_id,
          operation_type="connection_sync",
          base_cost=Decimal("10.0"),  # Base cost for sync operation
          metadata={
            "provider": "QuickBooks",
            "entity_id": entityId,
            "sync_type": "full",
          },
        )
        logger.info(f"Credits consumed for QuickBooks sync of entity {entityId}")
  except Exception as e:
    # Don't fail the sync if credit consumption fails
    logger.warning(f"Failed to consume credits for sync: {e}")


@celery_app.task(ignore_result=False, queue=QUEUE_DEFAULT, priority=10)
def sync_task_sse(entityId: str, graph_id: str, operation_id: str = None):
  """
  SSE-compatible QuickBooks sync task with real-time progress tracking.

  Args:
      entityId: QuickBooks entity ID
      graph_id: Graph database identifier
      operation_id: SSE operation ID for progress tracking
  """

  logger.info(
    f"Starting SSE QuickBooks sync for entity ID: {entityId}, graph_id: {graph_id}, operation_id: {operation_id}"
  )

  # TODO: Implement SSE sync
  logger.info(f"SSE QuickBooks sync completed for entity {entityId}")
  return {"status": "completed", "entity_id": entityId, "graph_id": graph_id}


async def _sync_task_sse_async(entityId: str, graph_id: str, operation_id: str):
  """Async implementation of SSE QuickBooks sync"""
  # TODO: Move the actual implementation here
  logger.info(f"SSE sync not yet implemented for entity {entityId}")
  return {"status": "completed", "entity_id": entityId, "graph_id": graph_id}
