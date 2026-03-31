"""Direct operation execution with SSE progress tracking.

This module provides utilities for running operations directly in the API process
(or via BackgroundTasks) while emitting SSE progress events.

Note: Graph creation (run_graph_creation, run_entity_graph_creation) has been
migrated to worker/tasks/graph_creation.py via GraphCreationService. The remaining
functions here handle subgraph creation, graph provisioning (billing-triggered),
and user repository provisioning.
"""

import asyncio
import os
import time
from typing import Any

from robosystems.logger import logger
from robosystems.middleware.sse.operation_manager import get_operation_manager

# Timeout for Dagster materialization reporting (seconds)
# This prevents the API from hanging if Dagster is unreachable.
# Default: 15s to allow for cold Dagster instances under load.
# Configurable via DAGSTER_MATERIALIZATION_TIMEOUT env var.
DAGSTER_REPORT_TIMEOUT = float(os.getenv("DAGSTER_MATERIALIZATION_TIMEOUT", "15.0"))


class ProgressEmitter:
  """
  Adapter that converts service progress_callback to SSE events.

  Services expect a sync callback: (message: str, percent: float) -> None
  This adapter emits SSE events asynchronously.

  Usage:
      progress = ProgressEmitter(operation_id)
      result = await service.create_graph(..., progress_callback=progress)
  """

  def __init__(self, operation_id: str):
    self.operation_id = operation_id
    self.manager = get_operation_manager()

  def __call__(self, message: str, percent: float):
    """Sync callback for services to call."""
    try:
      asyncio.get_running_loop()
      # We're in an async context, schedule as task
      asyncio.create_task(self._emit_async(message, percent))
    except RuntimeError:
      # No running event loop (shouldn't happen in BackgroundTasks context)
      logger.debug(f"Skipping progress emit (no event loop): {message}")

  async def _emit_async(self, message: str, percent: float):
    """Async SSE emission."""
    try:
      await self.manager.emit_progress(
        self.operation_id,
        message=message,
        progress_percent=percent,
      )
    except Exception as e:
      # Don't fail operation if SSE emit fails
      logger.warning(f"Failed to emit progress event: {e}")


async def run_subgraph_creation(
  operation_id: str,
  user_id: str,
  parent_graph_id: str,
  subgraph_name: str,
  description: str | None = None,
  fork_data: bool = False,
) -> dict[str, Any]:
  """
  Direct subgraph creation with SSE progress tracking.

  Replaces: run_and_monitor_dagster_job("create_subgraph_job", ...)

  Args:
      operation_id: SSE operation ID for progress tracking
      user_id: User creating the subgraph
      parent_graph_id: Parent graph to create subgraph under
      subgraph_name: Name for the subgraph
      description: Optional description
      fork_data: Whether to fork data from parent

  Returns:
      Subgraph creation result with graph_id
  """
  from robosystems.database import get_db_session
  from robosystems.models.core import Graph, User
  from robosystems.operations.graph.subgraph_service import SubgraphService

  manager = get_operation_manager()

  start_time = time.time()

  await manager.emit_progress(operation_id, "Initializing subgraph creation...", 0)

  try:
    db_gen = get_db_session()
    db = next(db_gen)

    try:
      # Get parent graph and user for the service
      parent_graph = db.query(Graph).filter(Graph.graph_id == parent_graph_id).first()
      if not parent_graph:
        raise ValueError(f"Parent graph {parent_graph_id} not found")

      user = db.query(User).filter(User.id == user_id).first()
      if not user:
        raise ValueError(f"User {user_id} not found")

      service = SubgraphService()

      # Build fork options
      fork_options = None
      if fork_data:
        fork_options = {"tables": []}  # Fork all tables

      result = await service.create_subgraph(
        parent_graph=parent_graph,
        user=user,
        name=subgraph_name,
        description=description,
        fork_parent=fork_data,
        fork_options=fork_options,
      )

      duration_ms = (time.time() - start_time) * 1000

      await manager.complete_operation(
        operation_id,
        result=result,
        message="Subgraph created successfully",
      )

      # Report to Dagster
      await _report_graph_materialization_async(
        asset_key="user_subgraph_creation",
        description=f"Direct creation of subgraph {result.get('graph_id')}",
        metadata={
          "graph_id": result.get("graph_id", ""),
          "parent_graph_id": parent_graph_id,
          "subgraph_name": subgraph_name,
          "user_id": user_id,
          "method": "direct",
          "duration_ms": duration_ms,
          "fork_data": str(fork_data),
        },
      )

      logger.info(
        f"Subgraph creation completed in {duration_ms:.0f}ms: {result.get('graph_id')}"
      )
      return result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  except Exception as e:
    logger.error(f"Subgraph creation failed: {e}")
    await manager.fail_operation(
      operation_id,
      error=str(e),
      error_details={"error_type": type(e).__name__},
    )
    raise


# =============================================================================
# DAGSTER OBSERVABILITY REPORTING
# =============================================================================
# All direct operations report AssetMaterializations to Dagster for observability.
# This gives you "cake and eat it too" - fast execution + Dagster UI visibility.
# Pattern copied from: robosystems/operations/lbug/direct_staging.py


async def _report_graph_materialization_async(
  asset_key: str,
  description: str,
  metadata: dict[str, Any],
) -> None:
  """Report an AssetMaterialization to Dagster without blocking."""
  try:
    await asyncio.wait_for(
      asyncio.to_thread(
        _report_graph_materialization_sync,
        asset_key,
        description,
        metadata,
      ),
      timeout=DAGSTER_REPORT_TIMEOUT,
    )
    logger.debug(f"Reported {asset_key} materialization to Dagster")
  except TimeoutError:
    logger.warning(
      f"Dagster materialization reporting timed out after {DAGSTER_REPORT_TIMEOUT}s. "
      "Operation succeeded but won't appear in Dagster UI."
    )
  except Exception as e:
    # Don't fail the operation if Dagster reporting fails - it's just observability
    logger.warning(
      f"Failed to report AssetMaterialization to Dagster: {e}. "
      "Operation succeeded but won't appear in Dagster UI."
    )


def _report_graph_materialization_sync(
  asset_key: str,
  description: str,
  metadata: dict[str, Any],
) -> None:
  """Synchronous Dagster materialization reporting (runs in thread)."""
  from robosystems.config import env

  # Skip Dagster reporting in test environment
  if env.ENVIRONMENT == "test":
    logger.debug(f"Skipping Dagster reporting in test environment for {asset_key}")
    return

  from dagster import AssetKey, AssetMaterialization, DagsterInstance, MetadataValue

  instance = DagsterInstance.get()

  # Convert metadata values to Dagster MetadataValue types
  dagster_metadata = {}
  for key, value in metadata.items():
    if isinstance(value, int):
      dagster_metadata[key] = MetadataValue.int(value)
    elif isinstance(value, float):
      dagster_metadata[key] = MetadataValue.float(value)
    elif isinstance(value, bool):
      dagster_metadata[key] = MetadataValue.bool(value)
    else:
      dagster_metadata[key] = MetadataValue.text(str(value))

  materialization = AssetMaterialization(
    asset_key=AssetKey(asset_key),
    description=description,
    metadata=dagster_metadata,
  )

  instance.report_runless_asset_event(materialization)


# =============================================================================
# PROVISIONING OPERATIONS
# =============================================================================
# These functions handle resource provisioning after payment confirmation.
# They replace the sensor-triggered Dagster jobs for faster execution.


async def run_graph_provisioning(
  operation_id: str | None,
  subscription_id: str,
  user_id: str,
  tier: str,
) -> dict[str, Any]:
  """
  Direct graph provisioning after payment confirmation.

  Replaces: provision_graph_job triggered by pending_subscription_sensor

  This function:
  1. Gets subscription details and validates status
  2. Creates the graph database (entity or generic)
  3. Activates the subscription with the created graph
  4. Reports to Dagster for observability

  Args:
      operation_id: Optional SSE operation ID (may be None for webhook-triggered)
      subscription_id: Subscription ID to provision
      user_id: User who owns the subscription
      tier: Instance tier (ladybug-standard, ladybug-large, ladybug-xlarge)

  Returns:
      Provisioning result with graph_id and status
  """
  from robosystems.database import get_db_session
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingCustomer,
    BillingEventType,
    BillingSubscription,
  )
  from robosystems.operations.graph.graph_creation_service import (
    GraphCreationConfig,
    GraphCreationService,
  )
  from robosystems.operations.graph.subscription_service import (
    generate_subscription_invoice,
  )

  manager = get_operation_manager()
  start_time = time.time()

  # Emit started event if we have an operation_id
  if operation_id:
    await manager.emit_progress(operation_id, "Starting graph provisioning...", 0)

  try:
    db_gen = get_db_session()
    db = next(db_gen)

    try:
      # Step 1: Get subscription details
      if operation_id:
        await manager.emit_progress(operation_id, "Validating subscription...", 10)

      subscription = (
        db.query(BillingSubscription)
        .filter(BillingSubscription.id == subscription_id)
        .first()
      )

      if not subscription:
        raise ValueError(f"Subscription {subscription_id} not found")

      if subscription.status not in ["provisioning", "pending_payment"]:
        logger.warning(
          f"Subscription {subscription_id} is in status {subscription.status}, "
          f"expected 'provisioning'"
        )

      # Extract graph config from subscription metadata
      graph_config = subscription.subscription_metadata or {}
      graph_type = graph_config.get("graph_type", "generic")
      graph_name = graph_config.get("graph_name")
      description = graph_config.get("description")
      schema_extensions = graph_config.get("schema_extensions", [])
      tags = graph_config.get("tags", [])
      entity_name = graph_config.get("entity_name")
      entity_identifier = graph_config.get("entity_identifier")
      entity_identifier_type = graph_config.get("entity_identifier_type")
      create_entity = graph_config.get("create_entity", True)

      # Step 2: Create the graph database
      if operation_id:
        await manager.emit_progress(operation_id, "Creating graph database...", 30)

      has_entity = graph_type in ["entity", "company"] and entity_name
      entity_data = None
      if has_entity:
        entity_data = {
          "name": entity_name,
          "uri": entity_name.lower().replace(" ", "-"),
          "extensions": schema_extensions,
          "ein": entity_identifier if entity_identifier_type == "ein" else None,
        }

      service = GraphCreationService()
      creation_result = await service.create(
        GraphCreationConfig(
          user_id=user_id,
          tier=tier,
          graph_name=graph_name or entity_name or f"Graph-{subscription_id[:8]}",
          graph_type="entity" if has_entity else "generic",
          schema_extensions=schema_extensions,
          entity_data=entity_data,
          create_entity=create_entity if has_entity else False,
          description=description,
          tags=tags,
        )
      )
      graph_id = creation_result.graph_id
      logger.info(f"Created graph {graph_id} for subscription {subscription_id}")

      # Step 3: Activate the subscription
      if operation_id:
        await manager.emit_progress(operation_id, "Activating subscription...", 70)

      subscription.resource_id = graph_id
      subscription.activate(db)

      # Log activation audit event
      BillingAuditLog.log_event(
        session=db,
        event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
        org_id=subscription.org_id,
        subscription_id=subscription.id,
        description=f"Activated subscription for graph {graph_id}",
        actor_type="system",
        event_data={
          "current_period_start": subscription.current_period_start.isoformat(),
          "current_period_end": subscription.current_period_end.isoformat(),
          "provisioning_method": "checkout",
        },
      )

      # Generate invoice for manual billing (non-Stripe) subscriptions
      if not subscription.stripe_subscription_id:
        customer = BillingCustomer.get_by_user_id(user_id, db)
        if customer and customer.invoice_billing_enabled:
          generate_subscription_invoice(
            subscription=subscription,
            customer=customer,
            description=f"Graph Database Subscription - {subscription.plan_name}",
            session=db,
          )
          logger.info(f"Generated invoice for subscription {subscription_id}")

      duration_ms = (time.time() - start_time) * 1000

      # Step 4: Complete operation
      provisioning_result = {
        "subscription_id": subscription_id,
        "graph_id": graph_id,
        "user_id": user_id,
        "tier": tier,
        "status": "activated",
      }

      if operation_id:
        await manager.complete_operation(
          operation_id,
          result=provisioning_result,
          message="Graph provisioned and subscription activated",
        )

      # Report to Dagster for observability (unified asset for all graph creation)
      await _report_graph_materialization_async(
        asset_key="user_graph_creation",
        description=f"Provisioning of graph {graph_id} for subscription {subscription_id}",
        metadata={
          "graph_id": graph_id,
          "user_id": user_id,
          "tier": tier,
          "graph_type": graph_type,
          "provisioning_method": "subscription",
          "subscription_id": subscription_id,
          "duration_ms": duration_ms,
        },
      )

      logger.info(
        f"Graph provisioning completed in {duration_ms:.0f}ms: "
        f"subscription={subscription_id}, graph={graph_id}"
      )
      return provisioning_result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  except Exception as e:
    logger.error(f"Graph provisioning failed for subscription {subscription_id}: {e}")
    if operation_id:
      await manager.fail_operation(
        operation_id,
        error=str(e),
        error_details={"error_type": type(e).__name__},
      )

    # Mark subscription as failed and cancel Stripe subscription
    try:
      db_gen = get_db_session()
      db = next(db_gen)
      try:
        subscription = (
          db.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .first()
        )
        if subscription:
          subscription.status = "failed"
          if subscription.subscription_metadata:
            metadata = dict(subscription.subscription_metadata)
            metadata["error"] = str(e)
            subscription.subscription_metadata = metadata
          else:
            subscription.subscription_metadata = {"error": str(e)}

          # Cancel Stripe subscription so the customer isn't charged
          if subscription.stripe_subscription_id:
            try:
              from robosystems.operations.providers.payment_provider import (
                get_payment_provider,
              )

              provider = get_payment_provider("stripe")
              provider.cancel_subscription(subscription.stripe_subscription_id)
            except Exception as cancel_error:
              logger.error(
                f"Failed to cancel Stripe subscription "
                f"{subscription.stripe_subscription_id}: {cancel_error}"
              )

          try:
            db.commit()
          except Exception as commit_error:
            logger.error(
              f"Failed to commit subscription failure status: {commit_error}"
            )
            db.rollback()
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as cleanup_error:
      logger.error(f"Failed to mark subscription as failed: {cleanup_error}")

    raise


async def run_user_repository_provisioning(
  operation_id: str | None,
  subscription_id: str,
  user_id: str,
  repository_name: str,
) -> dict[str, Any]:
  """
  Direct user repository provisioning after payment confirmation.

  Replaces: provision_repository_job triggered by pending_repository_sensor

  This function:
  1. Gets subscription details and validates
  2. Grants access to the shared repository
  3. Allocates credits based on plan
  4. Activates the subscription
  5. Reports to Dagster for observability

  Args:
      operation_id: Optional SSE operation ID (may be None for webhook-triggered)
      subscription_id: Subscription ID to provision
      user_id: User who owns the subscription
      repository_name: Repository to grant access to (sec, industry, economic)

  Returns:
      Provisioning result with access status
  """
  from robosystems.database import get_db_session
  from robosystems.models.core import RepositoryType
  from robosystems.models.core.billing import (
    BillingAuditLog,
    BillingCustomer,
    BillingEventType,
    BillingSubscription,
  )
  from robosystems.operations.graph.repository_subscription_service import (
    RepositorySubscriptionService,
  )
  from robosystems.operations.graph.subscription_service import (
    generate_subscription_invoice,
  )

  manager = get_operation_manager()
  start_time = time.time()

  if operation_id:
    await manager.emit_progress(operation_id, "Starting repository provisioning...", 0)

  try:
    db_gen = get_db_session()
    db = next(db_gen)

    try:
      # Step 1: Get subscription details
      if operation_id:
        await manager.emit_progress(operation_id, "Validating subscription...", 10)

      subscription = (
        db.query(BillingSubscription)
        .filter(BillingSubscription.id == subscription_id)
        .first()
      )

      if not subscription:
        raise ValueError(f"Subscription {subscription_id} not found")

      customer = BillingCustomer.get_by_user_id(user_id, db)
      if not customer:
        raise ValueError(f"Customer not found for user {user_id}")

      # Extract plan tier from plan name
      plan_tier = (
        subscription.plan_name.split("-")[-1]
        if "-" in subscription.plan_name
        else subscription.plan_name
      )

      # Validate repository type and plan
      try:
        repository_type = RepositoryType(repository_name)
        repository_plan = plan_tier
      except ValueError as e:
        raise ValueError(
          f"Invalid repository type '{repository_name}' or plan '{plan_tier}': {e}"
        )

      # Step 2: Grant repository access
      if operation_id:
        await manager.emit_progress(
          operation_id, f"Granting access to {repository_name}...", 30
        )

      repo_service = RepositorySubscriptionService(db)

      access_granted = repo_service.grant_access(
        repository_type=repository_type,
        user_id=user_id,
        repository_plan=repository_plan,
      )

      # Step 3: Allocate credits
      if operation_id:
        await manager.emit_progress(operation_id, "Allocating credits...", 50)

      credits_allocated = repo_service.allocate_credits(
        repository_type=repository_type,
        repository_plan=repository_plan,
        user_id=user_id,
      )

      logger.info(
        f"Granted access to {repository_name} for user {user_id}, "
        f"allocated {credits_allocated} credits"
      )

      # Step 4: Activate subscription
      if operation_id:
        await manager.emit_progress(operation_id, "Activating subscription...", 70)

      subscription.resource_id = repository_name
      subscription.activate(db)

      # Log audit event
      BillingAuditLog.log_event(
        session=db,
        event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
        org_id=customer.org_id,
        subscription_id=subscription.id,
        description=f"Activated subscription for {repository_name} repository",
        actor_type="system",
        event_data={
          "current_period_start": subscription.current_period_start.isoformat(),
          "current_period_end": subscription.current_period_end.isoformat(),
          "credits_allocated": credits_allocated,
        },
      )

      # Generate invoice (only for non-Stripe subscriptions;
      # Stripe-managed subscriptions handle invoicing via webhooks)
      if customer and not subscription.stripe_subscription_id:
        generate_subscription_invoice(
          subscription=subscription,
          customer=customer,
          description=f"{repository_name.upper()} Repository Subscription - {subscription.plan_name}",
          session=db,
        )
      elif subscription.stripe_subscription_id:
        logger.info(
          f"Stripe will create invoice for repository subscription {subscription.id}"
        )

      duration_ms = (time.time() - start_time) * 1000

      # Step 5: Complete operation
      provisioning_result = {
        "subscription_id": subscription_id,
        "repository_name": repository_name,
        "user_id": user_id,
        "status": "activated",
        "access_granted": access_granted,
        "credits_allocated": credits_allocated,
      }

      if operation_id:
        await manager.complete_operation(
          operation_id,
          result=provisioning_result,
          message="Repository access provisioned and subscription activated",
        )

      # Report to Dagster for observability
      await _report_graph_materialization_async(
        asset_key="user_repository_provisioning",
        description=f"Provisioning of {repository_name} access for user {user_id}",
        metadata={
          "repository_name": repository_name,
          "user_id": user_id,
          "subscription_id": subscription_id,
          "plan_tier": plan_tier,
          "credits_allocated": credits_allocated,
          "provisioning_method": "direct",
          "duration_ms": duration_ms,
        },
      )

      logger.info(
        f"Repository provisioning completed in {duration_ms:.0f}ms: "
        f"subscription={subscription_id}, repository={repository_name}"
      )
      return provisioning_result

    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  except Exception as e:
    logger.error(
      f"Repository provisioning failed for subscription {subscription_id}: {e}"
    )
    if operation_id:
      await manager.fail_operation(
        operation_id,
        error=str(e),
        error_details={"error_type": type(e).__name__},
      )

    # Mark subscription as failed and cancel Stripe subscription
    try:
      db_gen = get_db_session()
      db = next(db_gen)
      try:
        subscription = (
          db.query(BillingSubscription)
          .filter(BillingSubscription.id == subscription_id)
          .first()
        )
        if subscription:
          subscription.status = "failed"
          if subscription.subscription_metadata:
            metadata = dict(subscription.subscription_metadata)
            metadata["error"] = str(e)
            subscription.subscription_metadata = metadata
          else:
            subscription.subscription_metadata = {"error": str(e)}

          # Cancel Stripe subscription so the customer isn't charged
          if subscription.stripe_subscription_id:
            try:
              from robosystems.operations.providers.payment_provider import (
                get_payment_provider,
              )

              provider = get_payment_provider("stripe")
              provider.cancel_subscription(subscription.stripe_subscription_id)
            except Exception as cancel_error:
              logger.error(
                f"Failed to cancel Stripe subscription "
                f"{subscription.stripe_subscription_id}: {cancel_error}"
              )

          try:
            db.commit()
          except Exception as commit_error:
            logger.error(
              f"Failed to commit subscription failure status: {commit_error}"
            )
            db.rollback()
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as cleanup_error:
      logger.error(f"Failed to mark subscription as failed: {cleanup_error}")

    raise
