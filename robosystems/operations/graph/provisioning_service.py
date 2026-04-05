"""Provisioning operations for graphs and repositories after payment confirmation.

These functions handle resource provisioning triggered by billing webhooks
(Stripe payment confirmation) or direct subscription creation. They are
called synchronously from Dagster billing jobs and subscription routers.

Moved from middleware/sse/direct_monitor.py — these are service-layer
orchestration, not background tasks or SSE monitors.
"""

import time
from typing import Any

from robosystems.dagster.reporting import report_asset_materialization
from robosystems.logger import logger
from robosystems.middleware.sse.operation_manager import get_operation_manager


async def run_graph_provisioning(
  operation_id: str | None,
  subscription_id: str,
  user_id: str,
  tier: str,
) -> dict[str, Any]:
  """Provision a graph database after payment confirmation.

  Creates the graph via GraphCreationService, activates the subscription,
  and reports to Dagster for observability.

  Args:
      operation_id: Optional SSE operation ID (None for webhook-triggered)
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

  if operation_id:
    await manager.emit_progress(operation_id, "Starting graph provisioning...", 0)

  try:
    db_gen = get_db_session()
    db = next(db_gen)

    try:
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

      if operation_id:
        await manager.emit_progress(operation_id, "Activating subscription...", 70)

      subscription.resource_id = graph_id
      subscription.activate(db)

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

      await report_asset_materialization(
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

    _mark_subscription_failed(subscription_id, str(e))
    raise


async def run_user_repository_provisioning(
  operation_id: str | None,
  subscription_id: str,
  user_id: str,
  repository_name: str,
) -> dict[str, Any]:
  """Provision user repository access after payment confirmation.

  Grants access to a shared repository, allocates credits, activates
  the subscription, and reports to Dagster for observability.

  Args:
      operation_id: Optional SSE operation ID (None for webhook-triggered)
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

      plan_tier = (
        subscription.plan_name.split("-")[-1]
        if "-" in subscription.plan_name
        else subscription.plan_name
      )

      try:
        repository_type = RepositoryType(repository_name)
        repository_plan = plan_tier
      except ValueError as e:
        raise ValueError(
          f"Invalid repository type '{repository_name}' or plan '{plan_tier}': {e}"
        )

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

      if operation_id:
        await manager.emit_progress(operation_id, "Activating subscription...", 70)

      subscription.resource_id = repository_name
      subscription.activate(db)

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

      await report_asset_materialization(
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

    _mark_subscription_failed(subscription_id, str(e))
    raise


def _mark_subscription_failed(subscription_id: str, error: str) -> None:
  """Mark a subscription as failed and cancel its Stripe subscription."""
  from robosystems.database import get_db_session
  from robosystems.models.core.billing import BillingSubscription

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
          metadata["error"] = error
          subscription.subscription_metadata = metadata
        else:
          subscription.subscription_metadata = {"error": error}

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
          logger.error(f"Failed to commit subscription failure status: {commit_error}")
          db.rollback()
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass
  except Exception as cleanup_error:
    logger.error(f"Failed to mark subscription as failed: {cleanup_error}")
