"""
Graph provisioning task for payment-first flow.

This task is called by webhooks after payment confirmation to provision
a graph that was created during the checkout flow.
"""

import logging
from typing import Dict, Any
from sqlalchemy.exc import OperationalError
from ...celery import celery_app
from ...database import get_db_session
from ...models.billing import BillingSubscription

logger = logging.getLogger(__name__)


@celery_app.task(
  bind=True,
  name="provision_graph",
  autoretry_for=(ConnectionError, TimeoutError, OperationalError),
  retry_kwargs={"max_retries": 3, "countdown": 60},
  retry_backoff=True,
  retry_backoff_max=600,
)
def provision_graph_task(
  self, user_id: str, subscription_id: str, graph_config: Dict[str, Any]
) -> Dict[str, Any]:
  """
  Provision a graph database after payment confirmation.

  This task is called by payment webhooks after a user has added a payment
  method. It creates the graph and updates the existing subscription.

  The graph_config comes from the frontend checkout flow and contains:
  - graph_type: 'entity', 'company', or 'generic'
  - graph_name, description, tags, schema_extensions
  - For entity graphs: entity_name, entity_identifier, entity_identifier_type
  - For company graphs: company_name, company_identifier, company_identifier_type
  - create_entity: whether to populate initial entity data
  - tier: subscription tier (added by webhook handler)

  This task transforms the flat config into the proper format expected by
  the underlying Celery tasks (create_entity_with_new_graph_task or create_graph_task).

  Args:
      user_id: ID of the user who owns the graph
      subscription_id: ID of the existing subscription in PENDING_PAYMENT status
      graph_config: Dictionary containing graph configuration from checkout

  Returns:
      Dictionary containing graph_id and creation details

  Raises:
      Exception: If any step of the process fails
  """
  logger.info(
    f"Starting graph provisioning for user {user_id}, subscription {subscription_id}"
  )
  logger.info(f"Graph config received: {graph_config}")

  session = next(get_db_session())

  try:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {subscription_id} not found")

    if subscription.status != "provisioning":
      logger.warning(
        f"Subscription {subscription_id} is in status {subscription.status}, "
        f"expected 'provisioning'"
      )

    # Determine graph type and prepare appropriate task data
    graph_type = graph_config.get("graph_type", "generic")
    tier = graph_config.get("tier", "kuzu-standard")

    # Validate tier against subscription plan to prevent tier mismatch exploits
    if tier != subscription.plan_name:
      logger.warning(
        f"Tier mismatch detected: requested tier '{tier}' doesn't match "
        f"subscription plan '{subscription.plan_name}'. Using subscription plan tier."
      )
      tier = subscription.plan_name

    logger.info(f"Detected graph type: {graph_type}")

    # Check if this is an entity/company graph (has initial entity data)
    has_entity = graph_type in ["entity", "company"] and (
      graph_config.get("entity_name") or graph_config.get("company_name")
    )

    if has_entity:
      # Entity graph - use create_entity_with_new_graph_task
      logger.info("Provisioning as entity graph")

      from ...tasks.graph_operations.create_entity_graph import (
        create_entity_with_new_graph_task,
      )

      # Build entity data in the format expected by create_entity_with_new_graph_task
      entity_name = graph_config.get("entity_name") or graph_config.get("company_name")
      entity_identifier = graph_config.get("entity_identifier") or graph_config.get(
        "company_identifier"
      )
      entity_identifier_type = graph_config.get(
        "entity_identifier_type"
      ) or graph_config.get("company_identifier_type")

      # Build initial entity data
      initial_entity = {
        "name": entity_name,
        "uri": entity_name.lower().replace(" ", "-") if entity_name else "entity",
      }

      # Add identifier based on type
      if entity_identifier and entity_identifier_type:
        if entity_identifier_type == "ein":
          initial_entity["ein"] = entity_identifier
        elif entity_identifier_type == "cik":
          initial_entity["cik"] = entity_identifier

      # Build entity_data in format expected by task
      entity_data = {
        **initial_entity,
        "graph_tier": tier,
        "subscription_tier": "standard",
        "extensions": graph_config.get("schema_extensions", []),
        "graph_name": graph_config.get("graph_name"),
        "graph_description": graph_config.get("description"),
        "tags": graph_config.get("tags", []),
        "create_entity": graph_config.get("create_entity", True),
        "skip_billing": True,
      }

      logger.info(f"Entity data prepared: {entity_data}")

      # Call entity graph task
      result = create_entity_with_new_graph_task(self, entity_data, user_id)

    else:
      # Generic graph - use create_graph_task
      logger.info("Provisioning as generic graph")

      from ...tasks.graph_operations.create_graph import create_graph_task

      # Build metadata object
      metadata = {
        "graph_name": graph_config.get("graph_name"),
        "description": graph_config.get("description"),
        "tags": graph_config.get("tags", []),
        "schema_extensions": graph_config.get("schema_extensions", []),
      }

      # Build task_data in format expected by create_graph_task
      task_data = {
        "graph_id": None,  # Auto-generated
        "schema_extensions": graph_config.get("schema_extensions", []),
        "metadata": metadata,
        "tier": tier,
        "graph_tier": tier,
        "initial_data": None,
        "user_id": user_id,
        "custom_schema": graph_config.get("custom_schema"),
        "tags": graph_config.get("tags", []),
        "skip_billing": True,
      }

      logger.info(f"Task data prepared: {task_data}")

      # Call generic graph task
      result = create_graph_task(self, task_data)

    graph_id = result.get("graph_id")

    subscription.resource_id = graph_id
    subscription.activate(session)

    if not subscription.stripe_subscription_id:
      from ...operations.graph.subscription_service import generate_subscription_invoice
      from ...models.billing import BillingCustomer

      customer = BillingCustomer.get_by_user_id(user_id, session)
      if customer and customer.invoice_billing_enabled:
        generate_subscription_invoice(
          subscription=subscription,
          customer=customer,
          description=f"Graph Database Subscription - {subscription.plan_name}",
          session=session,
        )
        logger.info(
          f"Generated invoice for manual billing subscription {subscription_id}",
          extra={"user_id": user_id, "subscription_id": subscription_id},
        )
      else:
        logger.info(
          f"Skipping invoice generation for Stripe-managed subscription {subscription_id}",
          extra={"user_id": user_id, "subscription_id": subscription_id},
        )
    else:
      logger.info(
        f"Stripe will create invoice for subscription {subscription_id}",
        extra={
          "user_id": user_id,
          "subscription_id": subscription_id,
          "stripe_subscription_id": subscription.stripe_subscription_id,
        },
      )

    session.commit()

    logger.info(
      "Graph provisioning completed successfully",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "graph_id": graph_id,
        "graph_type": graph_type,
      },
    )

    return result

  except Exception as e:
    logger.error(
      f"Graph provisioning failed: {type(e).__name__}: {str(e)}",
      extra={
        "user_id": user_id,
        "subscription_id": subscription_id,
        "graph_config": graph_config,
      },
      exc_info=True,
    )

    try:
      session.rollback()

      subscription = (
        session.query(BillingSubscription)
        .filter(BillingSubscription.id == subscription_id)
        .first()
      )

      if subscription:
        subscription.status = "failed"
        if subscription.subscription_metadata:
          subscription.subscription_metadata["error"] = str(e)  # type: ignore[index]
        else:
          subscription.subscription_metadata = {"error": str(e)}
        session.commit()

        logger.info(
          f"Marked subscription {subscription_id} as failed",
          extra={"subscription_id": subscription_id, "error": str(e)},
        )
      else:
        logger.error(
          f"Subscription {subscription_id} not found for status update",
          extra={"subscription_id": subscription_id},
        )

    except Exception as update_error:
      logger.error(
        f"Failed to update subscription status: {update_error}",
        extra={"subscription_id": subscription_id},
      )
      try:
        session.rollback()
      except Exception:
        pass

    raise

  finally:
    session.close()
