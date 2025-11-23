"""
Celery Beat Schedule Configuration

This module defines all periodic tasks and their schedules.
Separated from celery.py for better maintainability and clarity.
"""

from celery.schedules import crontab
from robosystems.config import env
from robosystems.celery import QUEUE_DEFAULT

# Beat schedule configuration - start with empty dict
BEAT_SCHEDULE = {}

# Only add production tasks if not in dev environment
if env.ENVIRONMENT != "dev":
  BEAT_SCHEDULE.update(
    {
      # ========== BILLING TASKS ==========
      # Monthly shared repository credit allocation
      "allocate-monthly-shared-credits": {
        "task": "robosystems.tasks.billing.shared_credit_allocation.allocate_monthly_shared_credits",
        "schedule": crontab(
          hour=env.CREDIT_ALLOCATION_HOUR,
          minute=0,
          day_of_month=env.CREDIT_ALLOCATION_DAY,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 8,
        },
      },
      # Monthly credit reset and overage processing
      "monthly-credit-reset": {
        "task": "robosystems.tasks.billing.monthly_credit_reset.monthly_credit_reset",
        "schedule": crontab(
          hour=env.CREDIT_ALLOCATION_HOUR,
          minute=30,
          day_of_month=env.CREDIT_ALLOCATION_DAY,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 8,
        },
      },
      # Monthly usage report generation (2nd of month at 6 AM)
      "monthly-usage-report": {
        "task": "robosystems.tasks.billing.monthly_credit_reset.generate_monthly_usage_report",
        "schedule": crontab(
          hour=6,
          minute=0,
          day_of_month=2,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 5,
        },
      },
      # Daily storage billing
      "daily-storage-billing": {
        "task": "robosystems.tasks.billing.storage_billing.daily_storage_billing",
        "schedule": crontab(
          hour=2,
          minute=0,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 8,
        },
      },
      # Hourly storage usage collection
      "collect-storage-usage": {
        "task": "robosystems.tasks.billing.usage_collector.graph_usage_collector",
        "schedule": crontab(
          minute=5,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 6,
        },
      },
      # Monthly storage summary
      "monthly-storage-summary": {
        "task": "robosystems.tasks.billing.storage_billing.monthly_storage_summary",
        "schedule": crontab(
          hour=5,
          minute=0,
          day_of_month=2,
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 4,
        },
      },
      # ========== MONITORING TASKS ==========
      # ALL infrastructure monitoring is now handled by Lambda functions:
      # - Queue metrics and ECS task protection
      # - DLQ monitoring and health checks
      # These are scheduled via EventBridge (every 60 seconds), not Celery beat.
      # This provides better separation of concerns for infrastructure-level operations.
      # See worker-monitor Lambda in cloudformation/worker-infra.yaml1
      # ========== INFRASTRUCTURE TASKS ==========
      # Note: Instance monitoring tasks have been migrated to Lambda functions
      # and are now scheduled via EventBridge rules in graph-infra.yaml
      # ========== HEALTH CHECKS ==========
      # Weekly shared credit allocation health check
      "check-credit-allocation-health": {
        "task": "robosystems.tasks.billing.shared_credit_allocation.check_credit_allocation_health",
        "schedule": crontab(
          hour=3,
          minute=0,
          day_of_week=1,  # Monday
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 5,
        },
      },
      # Weekly graph credit health check
      "check-graph-credit-health": {
        "task": "robosystems.tasks.billing.credit_allocation.check_graph_credit_health",
        "schedule": crontab(
          hour=4,
          minute=0,
          day_of_week=1,  # Monday
        ),
        "options": {
          "queue": QUEUE_DEFAULT,
          "priority": 5,
        },
      },
    }
  )  # Close the update() for production tasks

# Essential tasks that should run in all environments (including dev)
# These are lightweight and don't require AWS services
BEAT_SCHEDULE.update(
  {
    # API key cleanup - runs against local PostgreSQL
    "cleanup-expired-api-keys": {
      "task": "robosystems.tasks.infrastructure.auth_cleanup.cleanup_expired_api_keys_task",
      "schedule": 3600.0,  # Every hour
      "options": {
        "queue": QUEUE_DEFAULT,
        "priority": 8,
      },
    },
  }
)  # Close the update() for essential tasks
