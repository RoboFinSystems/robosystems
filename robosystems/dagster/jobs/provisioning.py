"""Dagster provisioning jobs.

These jobs handle resource provisioning triggered by subscription events:
- Graph database provisioning after payment
- Repository access provisioning after payment

Migration Notes:
- Replaces: robosystems.tasks.graph_operations.provision_graph
- Replaces: robosystems.tasks.billing.provision_repository
- Triggered by: pending_subscription_sensor
"""

from typing import Any

from dagster import (
  Config,
  OpExecutionContext,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource
from robosystems.models.billing import BillingSubscription
from robosystems.models.iam import RepositoryPlan, RepositoryType


class ProvisionGraphConfig(Config):
  """Configuration for graph provisioning."""

  subscription_id: str
  user_id: str
  tier: str


class ProvisionRepositoryConfig(Config):
  """Configuration for repository access provisioning."""

  subscription_id: str
  user_id: str
  repository_name: str


# ============================================================================
# Graph Provisioning Job
# Replaces: robosystems.tasks.graph_operations.provision_graph.provision_graph_task
# ============================================================================


@op
def get_subscription_details(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: ProvisionGraphConfig,
) -> dict[str, Any]:
  """Get subscription details and validate status."""
  with db.get_session() as session:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == config.subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {config.subscription_id} not found")

    if subscription.status != "provisioning":
      context.log.warning(
        f"Subscription {config.subscription_id} is in status {subscription.status}, "
        f"expected 'provisioning'"
      )

    # Get graph config from subscription metadata
    graph_config = subscription.subscription_metadata or {}

    return {
      "subscription_id": config.subscription_id,
      "user_id": config.user_id,
      "tier": config.tier,
      "graph_type": graph_config.get("graph_type", "generic"),
      "graph_name": graph_config.get("graph_name"),
      "description": graph_config.get("description"),
      "schema_extensions": graph_config.get("schema_extensions", []),
      "tags": graph_config.get("tags", []),
      "entity_name": graph_config.get("entity_name"),
      "entity_identifier": graph_config.get("entity_identifier"),
      "entity_identifier_type": graph_config.get("entity_identifier_type"),
      "create_entity": graph_config.get("create_entity", True),
    }


@op
def create_graph_database(
  context: OpExecutionContext,
  db: DatabaseResource,
  subscription_details: dict[str, Any],
) -> dict[str, Any]:
  """Create the graph database based on type."""
  from robosystems.operations.graph.entity_graph_service import EntityGraphService
  from robosystems.operations.graph.generic_graph_service import GenericGraphService

  graph_type = subscription_details["graph_type"]
  user_id = subscription_details["user_id"]
  tier = subscription_details["tier"]

  context.log.info(f"Creating {graph_type} graph for user {user_id}")

  with db.get_session() as session:
    has_entity = graph_type in ["entity", "company"] and (
      subscription_details.get("entity_name")
    )

    if has_entity:
      # Entity graph
      entity_service = EntityGraphService(session)
      result = entity_service.create_entity_graph(
        user_id=user_id,
        entity_name=subscription_details["entity_name"],
        entity_identifier=subscription_details.get("entity_identifier"),
        entity_identifier_type=subscription_details.get("entity_identifier_type"),
        graph_name=subscription_details.get("graph_name"),
        graph_description=subscription_details.get("description"),
        tier=tier,
        schema_extensions=subscription_details.get("schema_extensions", []),
        tags=subscription_details.get("tags", []),
        create_entity=subscription_details.get("create_entity", True),
        skip_billing=True,  # Billing handled by subscription
      )
    else:
      # Generic graph
      graph_service = GenericGraphService(session)
      result = graph_service.create_graph(
        user_id=user_id,
        graph_name=subscription_details.get("graph_name"),
        description=subscription_details.get("description"),
        tier=tier,
        schema_extensions=subscription_details.get("schema_extensions", []),
        tags=subscription_details.get("tags", []),
        skip_billing=True,
      )

    context.log.info(f"Created graph: {result.get('graph_id')}")

    return {
      **subscription_details,
      "graph_id": result.get("graph_id"),
      "graph_type": graph_type,
    }


@op
def activate_graph_subscription(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph_result: dict[str, Any],
) -> dict[str, Any]:
  """Activate the subscription with the created graph."""
  from robosystems.models.iam import BillingCustomer
  from robosystems.operations.graph.subscription_service import (
    generate_subscription_invoice,
  )

  subscription_id = graph_result["subscription_id"]
  graph_id = graph_result["graph_id"]
  user_id = graph_result["user_id"]

  with db.get_session() as session:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {subscription_id} not found")

    # Update subscription with graph ID and activate
    subscription.resource_id = graph_id
    subscription.activate(session)

    # Generate invoice for manual billing (non-Stripe) subscriptions
    if not subscription.stripe_subscription_id:
      customer = BillingCustomer.get_by_user_id(user_id, session)
      if customer and customer.invoice_billing_enabled:
        generate_subscription_invoice(
          subscription=subscription,
          customer=customer,
          description=f"Graph Database Subscription - {subscription.plan_name}",
          session=session,
        )
        context.log.info(f"Generated invoice for subscription {subscription_id}")

    context.log.info(f"Activated subscription {subscription_id} with graph {graph_id}")

    return {
      "subscription_id": subscription_id,
      "graph_id": graph_id,
      "user_id": user_id,
      "status": "activated",
    }


@op
def handle_provisioning_failure(
  context: OpExecutionContext,
  db: DatabaseResource,
  subscription_id: str,
  error: str,
) -> None:
  """Handle provisioning failure by updating subscription status."""
  with db.get_session() as session:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if subscription:
      subscription.status = "failed"
      if subscription.subscription_metadata:
        metadata = dict(subscription.subscription_metadata)  # type: ignore[arg-type]
        metadata["error"] = error
        subscription.subscription_metadata = metadata
      else:
        subscription.subscription_metadata = {"error": error}

      context.log.error(f"Marked subscription {subscription_id} as failed: {error}")


@job
def provision_graph_job():
  """Provision a graph database after payment confirmation."""
  details = get_subscription_details()
  graph = create_graph_database(details)
  activate_graph_subscription(graph)


# ============================================================================
# Repository Access Provisioning Job
# Replaces: robosystems.tasks.billing.provision_repository.provision_repository_access_task
# ============================================================================


@op
def get_repository_subscription(
  context: OpExecutionContext,
  db: DatabaseResource,
  config: ProvisionRepositoryConfig,
) -> dict[str, Any]:
  """Get repository subscription details."""
  from robosystems.models.iam import BillingCustomer

  with db.get_session() as session:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == config.subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {config.subscription_id} not found")

    customer = BillingCustomer.get_by_user_id(config.user_id, session)
    if not customer:
      raise Exception(f"Customer not found for user {config.user_id}")

    # Extract plan tier from plan name
    plan_tier = (
      subscription.plan_name.split("-")[-1]
      if "-" in subscription.plan_name
      else subscription.plan_name
    )

    return {
      "subscription_id": config.subscription_id,
      "user_id": config.user_id,
      "repository_name": config.repository_name,
      "plan_tier": plan_tier,
      "org_id": customer.org_id,
    }


@op
def grant_repository_access(
  context: OpExecutionContext,
  db: DatabaseResource,
  subscription_info: dict[str, Any],
) -> dict[str, Any]:
  """Grant access to the shared repository."""
  from robosystems.operations.graph.repository_subscription_service import (
    RepositorySubscriptionService,
  )

  repository_name = subscription_info["repository_name"]
  plan_tier = subscription_info["plan_tier"]
  user_id = subscription_info["user_id"]

  try:
    repository_type = RepositoryType(repository_name)
    repository_plan = RepositoryPlan(plan_tier)
  except ValueError as e:
    raise Exception(
      f"Invalid repository type '{repository_name}' or plan '{plan_tier}': {e}"
    )

  with db.get_session() as session:
    repo_service = RepositorySubscriptionService(session)

    access_granted = repo_service.grant_access(
      repository_type=repository_type,
      user_id=user_id,
      repository_plan=repository_plan,
    )

    credits_allocated = repo_service.allocate_credits(
      repository_type=repository_type,
      repository_plan=repository_plan,
      user_id=user_id,
    )

    context.log.info(
      f"Granted access to {repository_name} for user {user_id}, "
      f"allocated {credits_allocated} credits"
    )

    return {
      **subscription_info,
      "access_granted": access_granted,
      "credits_allocated": credits_allocated,
    }


@op
def activate_repository_subscription(
  context: OpExecutionContext,
  db: DatabaseResource,
  access_result: dict[str, Any],
) -> dict[str, Any]:
  """Activate the repository subscription."""
  from robosystems.models.iam import (
    BillingAuditLog,
    BillingCustomer,
    BillingEventType,
  )
  from robosystems.operations.graph.subscription_service import (
    generate_subscription_invoice,
  )

  subscription_id = access_result["subscription_id"]
  user_id = access_result["user_id"]
  repository_name = access_result["repository_name"]

  with db.get_session() as session:
    subscription = (
      session.query(BillingSubscription)
      .filter(BillingSubscription.id == subscription_id)
      .first()
    )

    if not subscription:
      raise Exception(f"Subscription {subscription_id} not found")

    customer = BillingCustomer.get_by_user_id(user_id, session)

    # Update and activate subscription
    subscription.resource_id = repository_name
    subscription.activate(session)

    # Log audit event
    BillingAuditLog.log_event(
      session=session,
      event_type=BillingEventType.SUBSCRIPTION_ACTIVATED,
      org_id=access_result["org_id"],
      subscription_id=subscription.id,
      description=f"Activated subscription for {repository_name} repository",
      actor_type="system",
      event_data={
        "current_period_start": subscription.current_period_start.isoformat(),
        "current_period_end": subscription.current_period_end.isoformat(),
        "credits_allocated": access_result["credits_allocated"],
      },
    )

    # Generate invoice
    if customer:
      generate_subscription_invoice(
        subscription=subscription,
        customer=customer,
        description=f"{repository_name.upper()} Repository Subscription - {subscription.plan_name}",
        session=session,
      )

    context.log.info(
      f"Activated repository subscription {subscription_id} for {repository_name}"
    )

    return {
      "subscription_id": subscription_id,
      "repository_name": repository_name,
      "status": "activated",
      "credits_allocated": access_result["credits_allocated"],
    }


@job
def provision_repository_job():
  """Provision repository access after payment confirmation."""
  info = get_repository_subscription()
  access = grant_repository_access(info)
  activate_repository_subscription(access)
