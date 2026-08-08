"""Resource provisioning that runs once a payment is confirmed.

Driven by Stripe billing webhooks or direct subscription creation, and called
synchronously from the Dagster billing jobs and the subscription routers.

Failure disposition belongs to the caller's protocol, not to these functions.
The webhook path holds the provisioning claim, so a failed attempt stays
claim-held and non-terminal — the provider's redelivery retries it once the
staleness window passes, and the stalled-provisioning reaper is the one place
a dead attempt is finally written off. The direct router path has no
redelivery behind it, so it records the failure on the row itself and cancels
the payment subscription. Writing a terminal status here would preempt both.
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
  """Create the graph, activate the subscription, and report to Dagster.

  ``operation_id`` streams progress over SSE; it is None on the
  webhook-triggered path, which has no client listening. On failure the error
  is re-raised and the row is left exactly as the caller staged it — the
  webhook path still holds the provisioning claim, so the attempt stays
  retryable by redelivery until the reaper writes it off.
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

      # Link the graph before anything else can fail. The lifecycle sensors
      # join subscriptions to graphs on resource_id, so a graph created here
      # and orphaned by a later failure would otherwise be unreachable by the
      # machinery whose whole job is reclaiming it — running, unbilled, and
      # invisible. Committing the link also makes the claim's terminal
      # condition true, so no redelivery can create a second graph.
      subscription.resource_id = graph_id
      db.commit()

      if operation_id:
        await manager.emit_progress(operation_id, "Activating subscription...", 70)

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
          from robosystems.config.billing import BillingConfig

          plan_config = BillingConfig.get_subscription_plan(subscription.plan_name)
          tier_label = (
            plan_config["display_name"] if plan_config else subscription.plan_name
          )
          generate_subscription_invoice(
            subscription=subscription,
            customer=customer,
            description=f"Graph subscription - {tier_label}",
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

    raise


async def run_user_repository_provisioning(
  operation_id: str | None,
  subscription_id: str,
  user_id: str,
  repository_name: str,
) -> dict[str, Any]:
  """Grant repository access, allocate credits, and activate the subscription.

  ``operation_id`` streams progress over SSE; it is None on the
  webhook-triggered path. On failure the error is re-raised and disposition is
  the caller's: the webhook path keeps the claim held for redelivery, and the
  subscription router marks the row failed and cancels the payment
  subscription itself.
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

    raise
