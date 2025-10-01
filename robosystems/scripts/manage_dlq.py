#!/usr/bin/env python3
# type: ignore
"""
Dead Letter Queue (DLQ) management CLI tool.

This script provides secure command-line access to DLQ operations
without exposing them through the public API.

Usage:
    python -m robosystems.scripts.manage_dlq stats
    python -m robosystems.scripts.manage_dlq list
    python -m robosystems.scripts.manage_dlq reprocess <task_id>
    python -m robosystems.scripts.manage_dlq purge --confirm
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Dict, Any

from robosystems.celery import celery_app
from robosystems.tasks.dlq import DLQ_NAME, get_dlq_stats
from robosystems.config import env
from robosystems.logger import logger


def get_stats() -> Dict[str, Any]:
  """Get DLQ statistics."""
  try:
    stats = get_dlq_stats.apply_async().get(timeout=5)
    return stats
  except Exception as e:
    logger.error(f"Failed to get DLQ stats: {e}")
    return {
      "error": str(e),
      "queue_name": DLQ_NAME,
      "message_count": -1,
      "status": "error",
    }


def list_messages(limit: int = 10) -> Dict[str, Any]:
  """
  List messages in the DLQ.

  Note: This is a basic implementation. In production, you would
  store DLQ messages in a database for easier querying.
  """
  try:
    with celery_app.connection_or_acquire() as conn:
      # Get a consumer for the DLQ
      with conn.Consumer(
        queues=[celery_app.conf.task_queues[-1]],  # DLQ is last queue
        callbacks=[lambda body, message: None],
        no_ack=True,  # Don't acknowledge (just peek)
      ) as consumer:
        messages = []
        for _ in range(limit):
          try:
            # Try to get a message without blocking
            message = consumer.fetch(block=False, timeout=1)
            if message:
              messages.append(
                {
                  "id": message.properties.get("message_id"),
                  "body": message.body,
                  "headers": message.headers,
                  "timestamp": message.properties.get("timestamp"),
                }
              )
          except Exception:
            break  # No more messages

        return {
          "queue_name": DLQ_NAME,
          "messages": messages,
          "count": len(messages),
          "limit": limit,
        }
  except Exception as e:
    logger.error(f"Failed to list DLQ messages: {e}")
    return {
      "error": str(e),
      "queue_name": DLQ_NAME,
      "messages": [],
    }


def reprocess_message(task_id: str) -> Dict[str, Any]:
  """
  Reprocess a specific message from the DLQ.

  In production, you would fetch the message from a database
  by task_id and reprocess it.
  """
  # This is a placeholder - in reality you'd fetch from DB
  logger.warning(
    "DLQ reprocessing requires database storage of failed tasks. "
    "This is a placeholder implementation."
  )

  # Example of what you would do:
  # 1. Query database for the failed task by ID
  # 2. Get the original task name, args, and kwargs
  # 3. Resubmit the task

  return {
    "status": "not_implemented",
    "message": "DLQ reprocessing requires database storage",
    "task_id": task_id,
  }


def purge_queue(confirm: bool = False) -> Dict[str, Any]:
  """Purge all messages from the DLQ."""
  if not confirm:
    return {
      "error": "Purge requires --confirm flag",
      "queue_name": DLQ_NAME,
    }

  try:
    with celery_app.connection_or_acquire() as conn:
      channel = conn.default_channel
      deleted = channel.queue_purge(DLQ_NAME)

    logger.warning(f"Purged {deleted} messages from DLQ")

    return {
      "status": "success",
      "queue_name": DLQ_NAME,
      "messages_deleted": deleted,
      "purged_at": datetime.now(timezone.utc).isoformat(),
    }
  except Exception as e:
    logger.error(f"Failed to purge DLQ: {e}")
    return {
      "error": str(e),
      "queue_name": DLQ_NAME,
    }


def main():
  """Main CLI entry point."""
  parser = argparse.ArgumentParser(
    description="Manage Dead Letter Queue (DLQ) for failed Celery tasks"
  )

  subparsers = parser.add_subparsers(dest="command", help="Command to run")

  # Stats command
  subparsers.add_parser("stats", help="Get DLQ statistics")

  # List command
  list_parser = subparsers.add_parser("list", help="List messages in DLQ")
  list_parser.add_argument(
    "--limit",
    type=int,
    default=10,
    help="Maximum number of messages to list",
  )

  # Reprocess command
  reprocess_parser = subparsers.add_parser(
    "reprocess",
    help="Reprocess a failed task",
  )
  reprocess_parser.add_argument(
    "task_id",
    help="ID of the task to reprocess",
  )

  # Purge command
  purge_parser = subparsers.add_parser("purge", help="Purge all messages from DLQ")
  purge_parser.add_argument(
    "--confirm",
    action="store_true",
    help="Confirm purge operation",
  )

  # Health command
  subparsers.add_parser("health", help="Check DLQ health")

  args = parser.parse_args()

  if not args.command:
    parser.print_help()
    sys.exit(1)

  # Ensure we're in production or have explicit permission
  if env.is_development():
    logger.warning("Running DLQ management in development environment")

  # Execute command
  result = {}

  if args.command == "stats":
    result = get_stats()

  elif args.command == "list":
    result = list_messages(limit=args.limit)

  elif args.command == "reprocess":
    result = reprocess_message(args.task_id)

  elif args.command == "purge":
    result = purge_queue(confirm=args.confirm)

  elif args.command == "health":
    stats = get_stats()
    message_count = stats.get("message_count", -1)

    if message_count < 0:
      health_status = "error"
      health_message = "Cannot determine DLQ status"
    elif message_count == 0:
      health_status = "healthy"
      health_message = "No failed tasks"
    elif message_count < 50:
      health_status = "warning"
      health_message = f"{message_count} failed tasks in queue"
    else:
      health_status = "critical"
      health_message = f"{message_count} failed tasks - investigation needed!"

    result = {
      "status": health_status,
      "message": health_message,
      "message_count": message_count,
      "queue_name": DLQ_NAME,
    }

  # Output result
  print(json.dumps(result, indent=2))

  # Exit with appropriate code
  if "error" in result:
    sys.exit(1)
  elif result.get("status") == "critical":
    sys.exit(2)

  sys.exit(0)


if __name__ == "__main__":
  main()
