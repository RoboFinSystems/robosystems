"""Provisioning sensors for graph infrastructure.

These sensors watch for events that require infrastructure provisioning:
- New subscriptions needing graph databases
- Repository access provisioning
"""

from dagster import (
  DefaultSensorStatus,
  RunRequest,
  SensorEvaluationContext,
  SkipReason,
  sensor,
)

from robosystems.config import env
from robosystems.dagster.jobs.provisioning import (
  provision_graph_job,
  provision_repository_job,
)


@sensor(
  job=provision_graph_job,
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.RUNNING,
)
def pending_subscription_sensor(context: SensorEvaluationContext):
  """Watch for subscriptions in 'provisioning' status and trigger graph creation.

  This sensor replaces the Celery task that was triggered via .delay() calls
  when subscriptions were created. Instead, it polls for pending subscriptions
  and triggers the provisioning job.
  """
  # Skip in dev environment to avoid database connection issues
  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev environment")
    return

  from robosystems.database import session as SessionLocal
  from robosystems.models.iam import BillingSubscription

  session = SessionLocal()
  try:
    # Find subscriptions waiting for provisioning (graph type)
    pending = (
      session.query(BillingSubscription)
      .filter(
        BillingSubscription.status == "provisioning",
        BillingSubscription.product_type == "graph",
      )
      .all()
    )

    if not pending:
      return

    for sub in pending:
      # Use subscription ID as run key to prevent duplicate runs
      run_key = f"provision-graph-{sub.id}"

      context.log.info(
        f"Found pending graph subscription {sub.id} for user {sub.user_id}"
      )

      yield RunRequest(
        run_key=run_key,
        run_config={
          "ops": {
            "get_subscription_details": {
              "config": {
                "subscription_id": str(sub.id),
                "user_id": str(sub.user_id),
                "tier": sub.plan_name,
              }
            }
          }
        },
      )

  except Exception as e:
    context.log.error(f"Error checking pending subscriptions: {e}")
  finally:
    session.close()


@sensor(
  job=provision_repository_job,
  minimum_interval_seconds=60,
  default_status=DefaultSensorStatus.RUNNING,
)
def pending_repository_sensor(context: SensorEvaluationContext):
  """Watch for repository access subscriptions in 'provisioning' status.

  Triggers repository access provisioning for shared repositories
  (SEC, industry, economic data).
  """
  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev environment")
    return

  from robosystems.database import session as SessionLocal
  from robosystems.models.iam import BillingSubscription

  session = SessionLocal()
  try:
    # Find repository subscriptions waiting for provisioning
    pending = (
      session.query(BillingSubscription)
      .filter(
        BillingSubscription.status == "provisioning",
        BillingSubscription.product_type == "repository",
      )
      .all()
    )

    if not pending:
      return

    for sub in pending:
      run_key = f"provision-repo-{sub.id}"

      # Extract repository name from metadata
      metadata = sub.subscription_metadata or {}
      repository_name = metadata.get("repository_name", "sec")

      context.log.info(
        f"Found pending repository subscription {sub.id} "
        f"for user {sub.user_id}, repository {repository_name}"
      )

      yield RunRequest(
        run_key=run_key,
        run_config={
          "ops": {
            "get_repository_subscription": {
              "config": {
                "subscription_id": str(sub.id),
                "user_id": str(sub.user_id),
                "repository_name": repository_name,
              }
            }
          }
        },
      )

  except Exception as e:
    context.log.error(f"Error checking pending repository subscriptions: {e}")
  finally:
    session.close()
