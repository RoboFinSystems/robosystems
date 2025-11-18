"""
Subgraph creation task with fork support.
"""

import logging
import asyncio
from typing import Dict, Any
from ...celery import celery_app
from ...operations.graph.subgraph_service import SubgraphService
from ...database import get_db_session
from ...models.iam.graph import Graph
from ...models.iam.user import User

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, name="create_subgraph_with_fork_sse")
def create_subgraph_with_fork_sse_task(
  self, task_data: Dict[str, Any], operation_id: str
) -> Dict[str, Any]:
  """
  Create a new subgraph with optional fork from parent, with SSE progress tracking.

  This SSE-compatible version of the subgraph creation task emits real-time
  progress events through the unified operation monitoring system.

  Args:
      task_data: Dictionary containing:
          - parent_graph_id: Parent graph ID
          - user_id: ID of the user creating the subgraph
          - name: Subgraph name (alphanumeric, 1-20 chars)
          - description: Display name for the subgraph
          - subgraph_type: Type of subgraph (static, etc.)
          - metadata: Additional metadata
          - fork_parent: If True, copy parent data to subgraph
          - fork_options: Options for selective forking (tables, exclude_patterns)
      operation_id: SSE operation ID for progress tracking

  Returns:
      Dictionary containing subgraph details and fork status

  Raises:
      Exception: If any step of the process fails
  """
  user_id = task_data.get("user_id")
  parent_graph_id = task_data.get("parent_graph_id")
  name = task_data.get("name")

  logger.info(
    f"Starting SSE subgraph creation task for parent {parent_graph_id}, "
    f"name: {name}, user_id: {user_id}, operation_id: {operation_id}"
  )

  # Run async function in sync context
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)

  try:
    result = loop.run_until_complete(
      _async_create_subgraph_with_fork(
        task_data, operation_id, parent_graph_id, user_id, name
      )
    )
    return result
  finally:
    loop.close()
    asyncio.set_event_loop(None)


async def _async_create_subgraph_with_fork(
  task_data: Dict[str, Any],
  operation_id: str,
  parent_graph_id: str,
  user_id: str,
  name: str,
) -> Dict[str, Any]:
  """Async implementation of subgraph creation with fork."""
  # Initialize SSE progress tracker
  from robosystems.middleware.sse.task_progress import TaskSSEProgressTracker

  progress_tracker = TaskSSEProgressTracker(operation_id)

  # Initialize subgraph service
  service = SubgraphService()

  # Get database session for parent graph lookup
  session = next(get_db_session())

  try:
    # Start operation
    progress_tracker.emit_progress("Starting subgraph creation", 0)

    # Get parent graph and user
    parent_graph = (
      session.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
    )

    if not parent_graph:
      error = Exception(f"Parent graph {parent_graph_id} not found")
      progress_tracker.emit_error(error)
      raise error

    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      error = Exception(f"User {user_id} not found")
      progress_tracker.emit_error(error)
      raise error

    # Progress: Creating subgraph database
    progress_tracker.emit_progress("Creating subgraph database structure", 10)

    # Create the subgraph
    subgraph_result = await service.create_subgraph(
      parent_graph=parent_graph,
      user=user,
      name=name,
      description=task_data.get("description"),
      subgraph_type=task_data.get("subgraph_type", "static"),
      metadata=task_data.get("metadata"),
      fork_parent=False,  # We'll handle forking separately for progress tracking
      fork_options=None,
    )

    subgraph_id = subgraph_result["graph_id"]

    # Progress: Database created
    progress_tracker.emit_progress(
      f"Subgraph database {subgraph_id} created successfully", 30
    )

    # Handle fork if requested
    fork_status = None
    if task_data.get("fork_parent"):
      progress_tracker.emit_progress("Starting data fork from parent graph", 40)

      fork_options = task_data.get("fork_options", {})

      # List tables to copy
      progress_tracker.emit_progress("Listing parent graph tables", 50)

      # Perform the fork with progress updates
      try:
        # Create a progress callback wrapper (sync, not async)
        def progress_callback(msg: str, pct: float):
          progress_tracker.emit_progress(
            msg,
            50 + (pct * 0.4),  # Map fork progress from 50% to 90%
          )

        fork_status = await service.fork_parent_data(
          parent_graph_id=parent_graph_id,
          subgraph_id=subgraph_id,
          options=fork_options,
          progress_callback=progress_callback,
        )

        progress_tracker.emit_progress(
          f"Fork completed: {fork_status['row_count']} rows copied", 90
        )

      except Exception as fork_error:
        # Fork failed but subgraph was created
        logger.error(f"Fork failed for subgraph {subgraph_id}: {fork_error}")
        progress_tracker.emit_progress(
          f"Fork failed: {str(fork_error)}, but subgraph was created", 90
        )
        fork_status = {"status": "failed", "error": str(fork_error)}

    # Complete the operation
    result = {**subgraph_result, "fork_status": fork_status}

    progress_tracker.emit_completion(
      result, {"message": f"Subgraph {subgraph_id} created successfully"}
    )

    logger.info(f"SSE subgraph creation task completed successfully for {subgraph_id}")
    return result

  except Exception as e:
    # Log task-specific error details
    error_msg = f"Subgraph creation task failed: {type(e).__name__}: {str(e)}"
    logger.error(error_msg)
    logger.error(f"Task parameters: {task_data}")

    # Notify via SSE
    progress_tracker.emit_error(e)

    # Re-raise for Celery error handling
    raise

  finally:
    session.close()


@celery_app.task(bind=True, name="create_subgraph")
def create_subgraph_task(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
  """
  Create a new subgraph database synchronously (without SSE).

  This is the non-SSE version for backward compatibility or programmatic usage.

  Args:
      task_data: Dictionary containing subgraph creation parameters

  Returns:
      Dictionary containing subgraph details

  Raises:
      Exception: If any step of the process fails
  """
  user_id = task_data.get("user_id")
  parent_graph_id = task_data.get("parent_graph_id")
  name = task_data.get("name")

  logger.info(
    f"Starting subgraph creation task for parent {parent_graph_id}, "
    f"name: {name}, user_id: {user_id}"
  )

  # Run async function in sync context
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)

  try:
    result = loop.run_until_complete(
      _async_create_subgraph_simple(task_data, parent_graph_id, user_id, name)
    )
    return result
  finally:
    loop.close()
    asyncio.set_event_loop(None)


async def _async_create_subgraph_simple(
  task_data: Dict[str, Any], parent_graph_id: str, user_id: str, name: str
) -> Dict[str, Any]:
  """Async implementation of simple subgraph creation."""
  # Initialize subgraph service
  service = SubgraphService()

  # Get database session for parent graph lookup
  session = next(get_db_session())

  try:
    # Get parent graph and user
    parent_graph = (
      session.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
    )

    if not parent_graph:
      raise Exception(f"Parent graph {parent_graph_id} not found")

    user = session.query(User).filter(User.id == user_id).first()

    if not user:
      raise Exception(f"User {user_id} not found")

    # Create the subgraph (synchronous version handles forking internally)
    result = await service.create_subgraph(
      parent_graph=parent_graph,
      user=user,
      name=name,
      description=task_data.get("description"),
      subgraph_type=task_data.get("subgraph_type", "static"),
      metadata=task_data.get("metadata"),
      fork_parent=task_data.get("fork_parent", False),
      fork_options=task_data.get("fork_options"),
    )

    logger.info(
      f"Subgraph creation task completed successfully for {result['graph_id']}"
    )
    return result

  except Exception as e:
    # Log task-specific error details
    logger.error(f"Subgraph creation task failed: {type(e).__name__}: {str(e)}")
    logger.error(f"Task parameters: {task_data}")

    # Re-raise for Celery error handling
    raise

  finally:
    session.close()
