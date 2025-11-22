"""
Worker Autoscaling Monitor Lambda Function

This Lambda handles infrastructure-level monitoring for worker autoscaling:
- Publishes queue metrics to CloudWatch for autoscaling
- Manages ECS task protection for long-running tasks
- Monitors worker health and performance
- Handles all infrastructure-level operations that require AWS account context

This replaces the application-level queue monitor to maintain proper separation
of concerns between application logic and infrastructure management.
"""

import boto3
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from time import sleep
import redis
import ssl
from urllib.parse import quote

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ecs = boto3.client("ecs")
cloudwatch = boto3.client("cloudwatch")
elasticache = boto3.client("elasticache")
sns = boto3.client("sns")  # Added for DLQ alerting
secretsmanager = boto3.client("secretsmanager")

# Environment variables
ENVIRONMENT = os.environ.get("ENVIRONMENT", "staging")
VALKEY_CLUSTER_ID = os.environ.get("VALKEY_CLUSTER_ID")
CLOUDWATCH_NAMESPACE = "RoboSystems/Worker"

# Configurable DLQ threshold
DLQ_CRITICAL_THRESHOLD = int(os.environ.get("DLQ_CRITICAL_THRESHOLD", "20"))

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Queue configurations matching production deployment
WORKER_QUEUE_CONFIGS = {
  "default": {
    "queues": ["default"],
    "metric_dimensions": {"WorkerType": "default"},
    "cluster_name": f"robosystems-worker-default-{ENVIRONMENT}-cluster",
    "service_name": f"robosystems-worker-default-{ENVIRONMENT}-service",
  },
  "critical": {
    "queues": ["critical"],
    "metric_dimensions": {"WorkerType": "critical"},
    "cluster_name": f"robosystems-worker-critical-{ENVIRONMENT}-cluster",
    "service_name": f"robosystems-worker-critical-{ENVIRONMENT}-service",
  },
  "shared-extraction": {
    "queues": ["shared-extraction"],
    "metric_dimensions": {"WorkerType": "shared-extraction"},
    "cluster_name": f"robosystems-worker-shared-extraction-{ENVIRONMENT}-cluster",
    "service_name": f"robosystems-worker-shared-extraction-{ENVIRONMENT}-service",  # Fixed naming
  },
  "shared-processing": {
    "queues": ["shared-processing"],
    "metric_dimensions": {"WorkerType": "shared-processing"},
    "cluster_name": f"robosystems-worker-shared-processing-{ENVIRONMENT}-cluster",
    "service_name": f"robosystems-worker-shared-processing-{ENVIRONMENT}-service",  # Fixed naming
  },
  "shared-ingestion": {
    "queues": ["shared-ingestion"],
    "metric_dimensions": {"WorkerType": "shared-ingestion"},
    "cluster_name": f"robosystems-worker-shared-ingestion-{ENVIRONMENT}-cluster",
    "service_name": f"robosystems-worker-shared-ingestion-{ENVIRONMENT}-service",  # Fixed naming
  },
}

# Task protection configuration
IDLE_THRESHOLD_MINUTES = 5
PROTECTION_GRACE_PERIOD_MINUTES = 2
PROTECTION_EXPIRY_MINUTES = 60

# Connection pool for Redis - reused across functions
_redis_connections = {}
_valkey_auth_token = None
_token_cache_time = None

# Token cache TTL (1 hour) - allows rotation without Lambda restart
TOKEN_CACHE_TTL = 3600


def get_valkey_auth_token() -> Optional[str]:
  """Get Valkey auth token from Secrets Manager with TTL-based caching."""
  global _valkey_auth_token, _token_cache_time

  import time

  # Check if cached token is still valid
  if _valkey_auth_token and _token_cache_time:
    if time.time() - _token_cache_time < TOKEN_CACHE_TTL:
      return _valkey_auth_token

  secret_name = f"robosystems/{ENVIRONMENT}/valkey"

  try:
    response = secretsmanager.get_secret_value(SecretId=secret_name)
    secret_data = json.loads(response["SecretString"])
    _valkey_auth_token = secret_data.get("VALKEY_AUTH_TOKEN")

    if not _valkey_auth_token:
      logger.error(f"VALKEY_AUTH_TOKEN key not found in secret {secret_name}")
      return None

    _token_cache_time = time.time()
    logger.info(
      f"Retrieved Valkey auth token from {secret_name} (cached for {TOKEN_CACHE_TTL}s)"
    )
    return _valkey_auth_token
  except Exception as e:
    logger.error(f"Failed to get Valkey auth token from {secret_name}: {e}")
    return None


def get_redis_connection(database: int = 0) -> redis.Redis:
  """Get Redis connection with proper database selection and pooling.

  Database mapping (from Valkey registry):
  - 0: Celery broker
  - 1: Celery results
  - 2: Auth cache
  - 3: SSE events
  - 4: Distributed locks
  - 5: Pipeline tracking
  - 6: Credits cache
  - 7: Rate limiting
  - 8: LadybugDB cache
  """
  global _redis_connections

  # Check if we already have a connection for this database
  if database in _redis_connections:
    try:
      # Test if connection is still alive
      _redis_connections[database].ping()
      return _redis_connections[database]
    except Exception:
      # Connection is dead, remove it
      del _redis_connections[database]

  redis_endpoint = get_valkey_endpoint()
  if not redis_endpoint:
    raise RuntimeError("Failed to get Valkey endpoint")

  # Get auth token
  auth_token = get_valkey_auth_token()
  if not auth_token:
    secret_name = f"robosystems/{ENVIRONMENT}/valkey"
    raise RuntimeError(
      f"Failed to retrieve Valkey auth token from Secrets Manager. "
      f"Check that secret '{secret_name}' exists and contains 'VALKEY_AUTH_TOKEN' key."
    )

  # Build URL with auth token and database number
  # Format: rediss://default:{password}@{host}:{port}/{db}
  # Use rediss:// for TLS connections (matches application pattern)
  # URL-encode the auth token to handle special characters (use quote, not quote_plus)
  encoded_token = quote(auth_token, safe="")
  redis_url = f"rediss://default:{encoded_token}@{redis_endpoint}/{database}"

  # Connection parameters matching application configuration
  # SECURITY NOTE: ElastiCache uses self-signed certificates that cannot be validated
  # against a CA. This is AWS's design for ElastiCache. The connection is still
  # encrypted with TLS, but we cannot verify the certificate authenticity.
  conn = redis.from_url(
    redis_url,
    socket_connect_timeout=2,
    socket_timeout=2,
    retry_on_timeout=False,
    decode_responses=False,  # Keep as bytes for proper queue detection
    ssl_cert_reqs=ssl.CERT_NONE,  # Don't verify certificate (ElastiCache uses self-signed)
    ssl_check_hostname=False,  # Don't check hostname
    ssl_ca_certs=None,  # No CA certificate validation
  )

  # Store connection for reuse
  _redis_connections[database] = conn
  return conn


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """Main Lambda handler"""

  # Default action is to do all monitoring tasks
  action = event.get("action", "all")

  logger.info(f"Executing action: {action}")

  results = {}

  try:
    # Initialize Redis connection with retry logic
    redis_client = None
    for attempt in range(MAX_RETRIES):
      try:
        redis_client = get_redis_connection(0)  # Celery broker
        break
      except Exception as e:
        if attempt < MAX_RETRIES - 1:
          logger.warning(f"Attempt {attempt + 1} failed to connect to Redis: {e}")
          sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
        else:
          logger.error(f"All attempts to connect to Redis failed: {e}")
          # Graceful degradation - return partial results
          return {
            "statusCode": 200,
            "body": json.dumps(
              {
                "status": "partial",
                "error": "Redis connection failed - metrics may be incomplete",
                "results": results,
                "timestamp": datetime.now(timezone.utc).isoformat(),
              }
            ),
          }

    if action in ["all", "queue_metrics"]:
      results["queue_metrics"] = publish_queue_metrics(redis_client)

    if action in ["all", "task_protection"]:
      results["task_protection"] = (
        manage_task_protection()
      )  # Will get its own connections

    if action in ["all", "dlq_monitor"]:
      results["dlq_monitor"] = monitor_dlq(redis_client)

    # Note: Connection pooling handles cleanup automatically

    return {
      "statusCode": 200,
      "body": json.dumps(
        {
          "status": "success",
          "results": results,
          "timestamp": datetime.now(timezone.utc).isoformat(),
        }
      ),
    }

  except Exception as e:
    logger.error(f"Lambda execution failed: {e}", exc_info=True)
    return {
      "statusCode": 500,
      "body": json.dumps(
        {
          "status": "error",
          "error": str(e),
          "timestamp": datetime.now(timezone.utc).isoformat(),
        }
      ),
    }


def get_valkey_endpoint() -> Optional[str]:
  """Get Valkey/ElastiCache endpoint with retry logic using replication groups."""
  for attempt in range(MAX_RETRIES):
    try:
      if not VALKEY_CLUSTER_ID:
        logger.error("VALKEY_CLUSTER_ID not configured")
        return None

      # Try as a replication group first (preferred for multi-node setups)
      try:
        response = elasticache.describe_replication_groups(
          ReplicationGroupId=VALKEY_CLUSTER_ID
        )
        groups = response.get("ReplicationGroups", [])

        if groups:
          group = groups[0]
          # Use the primary endpoint for the replication group
          # This automatically handles failover and doesn't require node suffixes
          endpoint_address = group.get("ConfigurationEndpoint", {}).get("Address")
          port = 6379  # Default port
          if not endpoint_address:
            # Fallback to primary endpoint if no configuration endpoint
            node_groups = group.get("NodeGroups", [])
            if node_groups:
              primary_endpoint = node_groups[0].get("PrimaryEndpoint", {})
              endpoint_address = primary_endpoint.get("Address")
              port = primary_endpoint.get("Port", 6379)
          else:
            port = group.get("ConfigurationEndpoint", {}).get("Port", 6379)

          if endpoint_address:
            redis_endpoint = f"{endpoint_address}:{port}"
            logger.info(f"Using Valkey replication group endpoint: {redis_endpoint}")
            return redis_endpoint
      except Exception as e:
        if "ReplicationGroupNotFoundFault" not in str(e):
          raise
        # Not a replication group, fall back to cache cluster lookup
        logger.info(
          f"{VALKEY_CLUSTER_ID} is not a replication group, trying as cache cluster..."
        )

      # Fallback: try as a standalone cache cluster
      response = elasticache.describe_cache_clusters(
        CacheClusterId=VALKEY_CLUSTER_ID,
        ShowCacheNodeInfo=True,
      )
      clusters = response.get("CacheClusters", [])

      if not clusters:
        logger.error(f"No cache cluster found for {VALKEY_CLUSTER_ID}")
        return None

      cluster = clusters[0]
      nodes = cluster.get("CacheNodes", [])
      if not nodes:
        logger.error(f"No cache nodes found for {VALKEY_CLUSTER_ID}")
        return None

      endpoint = nodes[0].get("Endpoint", {})
      address = endpoint.get("Address")
      port = endpoint.get("Port", 6379)

      if not address:
        logger.error("No endpoint address found")
        return None

      redis_endpoint = f"{address}:{port}"
      logger.info(f"Using Valkey cache cluster endpoint: {redis_endpoint}")
      return redis_endpoint

    except Exception as e:
      if attempt < MAX_RETRIES - 1:
        logger.warning(f"Attempt {attempt + 1} failed to get Valkey endpoint: {e}")
        sleep(RETRY_DELAY * (attempt + 1))
      else:
        logger.error(f"All attempts to get Valkey endpoint failed: {e}")
        return None

  return None


def publish_queue_metrics(redis_client) -> Dict[str, Any]:
  """Publish queue metrics to CloudWatch for autoscaling"""

  queue_metrics = {}
  worker_pool_metrics = {}
  metrics_published = 0
  error_count = 0
  max_consecutive_errors = 3

  try:
    # Collect queue sizes
    for pool_name, config in WORKER_QUEUE_CONFIGS.items():
      pool_total = 0
      for queue_name in config["queues"]:
        queue_size = get_queue_size(redis_client, queue_name)
        if queue_size >= 0:
          queue_metrics[queue_name] = queue_size
          pool_total += queue_size
          error_count = 0
        else:
          error_count += 1
          if error_count >= max_consecutive_errors:
            logger.error(
              f"Hit {max_consecutive_errors} consecutive errors, stopping metric collection early"
            )
            break

      if error_count >= max_consecutive_errors:
        break

      worker_pool_metrics[pool_name] = pool_total

    # Check dead letter queue only if we haven't hit too many errors
    if error_count < max_consecutive_errors:
      dlq_size = get_queue_size(redis_client, "dead_letter")
      if dlq_size > 0:
        queue_metrics["dead_letter"] = dlq_size
        logger.warning(f"Dead letter queue has {dlq_size} messages")

    # Publish to CloudWatch
    metric_data = []
    timestamp = datetime.now(timezone.utc)

    # Individual queue metrics
    for queue_name, queue_size in queue_metrics.items():
      metric_data.append(
        {
          "MetricName": "QueueSize",
          "Value": queue_size,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Queue", "Value": queue_name},
            {"Name": "Environment", "Value": ENVIRONMENT},
          ],
          "Timestamp": timestamp,
        }
      )

    # Worker pool aggregate metrics (for autoscaling)
    for pool_name, pool_size in worker_pool_metrics.items():
      metric_data.append(
        {
          "MetricName": "WorkerPoolQueueSize",
          "Value": pool_size,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "WorkerPool", "Value": pool_name},
            {"Name": "Environment", "Value": ENVIRONMENT},
          ],
          "Timestamp": timestamp,
        }
      )

    # Publish in batches (CloudWatch limit is 20 metrics per call)
    for i in range(0, len(metric_data), 20):
      batch = metric_data[i : i + 20]
      cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=batch,
      )
      metrics_published += len(batch)

    logger.info(
      f"Published {metrics_published} metrics. "
      f"Queue sizes: {queue_metrics}, Pool sizes: {worker_pool_metrics}"
    )

    return {
      "metrics_published": metrics_published,
      "queue_sizes": queue_metrics,
      "worker_pool_sizes": worker_pool_metrics,
    }

  except Exception as e:
    logger.error(f"Failed to publish queue metrics: {e}")
    return {"error": str(e)}


def get_queue_size(redis_client, queue_name: str) -> int:
  """Get queue size, handling Kombu priority queue naming"""
  try:
    # First try the simple queue name
    queue_size = redis_client.llen(queue_name)

    # Log if we found messages in the simple queue
    if queue_size > 0:
      logger.debug(f"Found {queue_size} messages in queue '{queue_name}'")
      return queue_size

    # If empty, check for priority-encoded versions
    # Kombu adds priority markers like \x06\x166
    pattern = f"{queue_name}*"
    keys = redis_client.keys(pattern)

    if keys:
      logger.debug(f"Checking {len(keys)} keys matching pattern '{pattern}'")

    for key in keys:
      # Handle both bytes and string keys
      key_str = key.decode("utf-8", errors="ignore") if isinstance(key, bytes) else key

      # Check if it's a list type - handle both bytes and string responses
      key_type = redis_client.type(key)
      is_list = (
        key_type == b"list" if isinstance(key_type, bytes) else key_type == "list"
      )

      if is_list and key_str.startswith(queue_name):
        size = redis_client.llen(key)
        if size > 0:
          logger.info(
            f"Found priority queue variant '{repr(key_str)}' with {size} messages"
          )
          queue_size = max(queue_size, size)

    return queue_size

  except redis.TimeoutError:
    logger.error(f"Timeout getting queue size for '{queue_name}'")
    return -1
  except redis.ConnectionError:
    logger.error(f"Connection error getting queue size for '{queue_name}'")
    return -1
  except Exception as e:
    logger.error(f"Failed to get queue size for '{queue_name}': {e}")
    return -1


def check_task_processing_status(task_id: str, redis_locks) -> bool:
  """Check if a task is actively processing."""
  sec_task_key = f"sec:task:active:{task_id}"
  return redis_locks.exists(sec_task_key)


def update_task_protection(
  cluster_name: str,
  task_arn: str,
  enable: bool,
  expiry_minutes: int = PROTECTION_EXPIRY_MINUTES,
) -> bool:
  """Enable or disable protection for a single task."""
  try:
    if enable:
      ecs.update_task_protection(
        cluster=cluster_name,
        tasks=[task_arn],
        protectionEnabled=True,
        expiresInMinutes=expiry_minutes,
      )
      logger.info(f"Enabled protection for task {task_arn.split('/')[-1]}")
    else:
      ecs.update_task_protection(
        cluster=cluster_name,
        tasks=[task_arn],
        protectionEnabled=False,
      )
      logger.info(f"Disabled protection for task {task_arn.split('/')[-1]}")
    return True
  except Exception as e:
    logger.error(f"Failed to update protection for {task_arn}: {e}")
    return False


def check_task_idle_time(task_id: str, redis_locks) -> int:
  """Get or update the idle time counter for a task."""
  idle_key = f"ecs:idle:counter:{task_id}"
  return int(redis_locks.get(idle_key) or 0)


def increment_task_idle_time(task_id: str, redis_locks) -> None:
  """Increment and expire the idle time counter for a task."""
  idle_key = f"ecs:idle:counter:{task_id}"
  redis_locks.incr(idle_key)
  redis_locks.expire(idle_key, 600)  # 10 minute expiry


def clear_task_idle_time(task_id: str, redis_locks) -> None:
  """Clear the idle time counter for a task."""
  idle_key = f"ecs:idle:counter:{task_id}"
  redis_locks.delete(idle_key)


def process_worker_tasks(worker_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
  """Process task protection for a single worker type."""
  cluster_name = config["cluster_name"]
  service_name = config["service_name"]

  result = {
    "worker_type": worker_type,
    "tasks": [],
    "protection_enabled": 0,
    "protection_disabled": 0,
    "error": None,
  }

  try:
    # List running tasks
    list_response = ecs.list_tasks(
      cluster=cluster_name,
      serviceName=service_name,
      desiredStatus="RUNNING",
    )

    task_arns = list_response.get("taskArns", [])
    if not task_arns:
      return result

    # Get protection status
    protection_response = ecs.get_task_protection(
      cluster=cluster_name,
      tasks=task_arns,
    )

    # Get Redis connection for distributed locks
    try:
      redis_locks = get_redis_connection(4)
    except Exception as redis_error:
      logger.error(f"Failed to connect to Redis for task protection: {redis_error}")
      result["error"] = f"Redis connection failed: {str(redis_error)}"
      return result

    for task_info in protection_response.get("protectedTasks", []):
      task_arn = task_info["taskArn"]
      task_id = task_arn.split("/")[-1] if "/" in task_arn else task_arn.split(":")[-1]
      protection_enabled = task_info.get("protectionEnabled", False)

      # Check if task is actively processing (with timeout handling)
      try:
        is_processing = check_task_processing_status(task_id, redis_locks)
      except (redis.TimeoutError, redis.ConnectionError) as e:
        logger.warning(
          f"Redis timeout checking task {task_id}, assuming not processing: {e}"
        )
        is_processing = False

      task_data = {
        "task_id": task_id,
        "protected": protection_enabled,
        "processing": is_processing,
      }
      result["tasks"].append(task_data)

      # Enable protection if needed
      if is_processing and not protection_enabled:
        if update_task_protection(cluster_name, task_arn, True):
          result["protection_enabled"] += 1

      # Disable protection if idle
      elif not is_processing and protection_enabled:
        try:
          idle_minutes = check_task_idle_time(task_id, redis_locks)

          if idle_minutes >= IDLE_THRESHOLD_MINUTES:
            if update_task_protection(cluster_name, task_arn, False):
              result["protection_disabled"] += 1
              clear_task_idle_time(task_id, redis_locks)
          else:
            increment_task_idle_time(task_id, redis_locks)
        except (redis.TimeoutError, redis.ConnectionError) as e:
          logger.warning(f"Redis timeout managing idle time for task {task_id}: {e}")

  except Exception as e:
    logger.error(f"Failed to process tasks for {worker_type}: {e}")
    result["error"] = str(e)

  return result


def manage_task_protection() -> Dict[str, Any]:
  """Manage ECS task protection for long-running tasks"""

  all_results = []
  total_protected = 0
  total_enabled = 0
  total_disabled = 0

  try:
    # Check each worker type that needs protection
    protected_worker_types = [
      "shared-processing",
      "shared-ingestion",
      "shared-extraction",
    ]

    for worker_type in protected_worker_types:
      config = WORKER_QUEUE_CONFIGS.get(worker_type)
      if not config:
        continue

      # Process this worker type
      result = process_worker_tasks(worker_type, config)
      all_results.append(result)

      # Aggregate counts
      total_protected += len([t for t in result["tasks"] if t["protected"]])
      total_enabled += result["protection_enabled"]
      total_disabled += result["protection_disabled"]

    # Collect all tasks for metrics
    all_tasks = []
    for result in all_results:
      all_tasks.extend(result["tasks"])

    # Publish protection metrics
    try:
      timestamp = datetime.now(timezone.utc)
      metric_data = [
        {
          "MetricName": "ProtectedTaskCount",
          "Value": total_protected,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": ENVIRONMENT},
          ],
          "Timestamp": timestamp,
        },
        {
          "MetricName": "ProcessingTaskCount",
          "Value": len([t for t in all_tasks if t["processing"]]),
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": ENVIRONMENT},
          ],
          "Timestamp": timestamp,
        },
      ]

      cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=metric_data,
      )
    except Exception as e:
      logger.error(f"Failed to publish protection metrics: {e}")

    logger.info(
      f"Task protection summary: {len(all_tasks)} total, "
      f"{total_enabled} enabled, {total_disabled} disabled"
    )

    return {
      "total_tasks": len(all_tasks),
      "protection_enabled": total_enabled,
      "protection_disabled": total_disabled,
      "worker_results": all_results,
    }

  except Exception as e:
    logger.error(f"Failed to manage task protection: {e}")
    return {"error": str(e)}


def monitor_dlq(redis_client) -> Dict[str, Any]:
  """Monitor Dead Letter Queue and publish metrics"""

  DLQ_NAME = "dead_letter"
  metrics_published = 0

  try:
    # Get DLQ size with timeout handling
    try:
      dlq_size = redis_client.llen(DLQ_NAME)
    except (redis.TimeoutError, redis.ConnectionError) as e:
      logger.error(f"Redis timeout getting DLQ size: {e}")
      return {"error": f"Redis timeout: {str(e)}"}

    # Determine health status using configurable threshold
    if dlq_size == 0:
      status_value = 0  # Healthy
      health_status = "healthy"
      health_message = "No failed tasks in DLQ"
    elif dlq_size < 10:
      status_value = 1  # Warning
      health_status = "warning"
      health_message = f"{dlq_size} failed tasks in DLQ"
    elif dlq_size < DLQ_CRITICAL_THRESHOLD:
      status_value = 1  # Warning
      health_status = "warning"
      health_message = f"{dlq_size} failed tasks in DLQ - investigation recommended"
    else:
      status_value = 2  # Critical
      health_status = "critical"
      health_message = f"{dlq_size} failed tasks in DLQ - immediate attention required!"

    logger.info(f"DLQ Health Check: {health_message}")

    # Publish to CloudWatch
    metric_data = []
    timestamp = datetime.now(timezone.utc)

    # DLQ size metric
    metric_data.append(
      {
        "MetricName": "DLQSize",
        "Value": dlq_size,
        "Unit": "Count",
        "Dimensions": [
          {"Name": "Queue", "Value": DLQ_NAME},
          {"Name": "Environment", "Value": ENVIRONMENT},
        ],
        "Timestamp": timestamp,
      }
    )

    # DLQ status metric
    metric_data.append(
      {
        "MetricName": "DLQStatus",
        "Value": status_value,
        "Unit": "None",
        "Dimensions": [
          {"Name": "Queue", "Value": DLQ_NAME},
          {"Name": "Environment", "Value": ENVIRONMENT},
        ],
        "Timestamp": timestamp,
      }
    )

    # Publish metrics
    cloudwatch.put_metric_data(
      Namespace=CLOUDWATCH_NAMESPACE,
      MetricData=metric_data,
    )
    metrics_published = len(metric_data)

    logger.info(f"Published DLQ metrics: size={dlq_size}, status={status_value}")

    # TODO: If critical, could trigger SNS alert

    return {
      "metrics_published": metrics_published,
      "dlq_size": dlq_size,
      "health_status": health_status,
      "health_message": health_message,
    }

  except Exception as e:
    logger.error(f"Failed to monitor DLQ: {e}")
    return {"error": str(e)}
