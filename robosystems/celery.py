from celery import Celery
from celery.signals import worker_ready
from kombu import Exchange, Queue
from robosystems.config import env
from robosystems.config.validation import EnvValidator
from robosystems.logger import logger


# Queue names - using environment variables for backward compatibility
# These will map to the new queue names in production
QUEUE_DEFAULT = env.QUEUE_DEFAULT
QUEUE_CRITICAL = env.QUEUE_CRITICAL
QUEUE_SHARED_EXTRACTION = (
  env.QUEUE_SHARED_EXTRACTION
)  # Limited to 2 concurrent workers for SEC downloads
QUEUE_SHARED_PROCESSING = env.QUEUE_SHARED_PROCESSING
QUEUE_SHARED_INGESTION = env.QUEUE_SHARED_INGESTION  # Limited to 2-3 concurrent workers

# Future queues - currently route to default queue
QUEUE_DATA_SYNC = env.QUEUE_DATA_SYNC  # Routes to default for now
QUEUE_ANALYTICS = env.QUEUE_ANALYTICS  # Routes to default for now


# Get Celery configuration with authenticated URLs for prod/staging
celery_config = env.get_celery_config()

celery_app = Celery(
  "tasks",
  broker=celery_config["broker_url"],
  result_backend=celery_config["result_backend"],
  include=[
    # Data synchronization
    "robosystems.tasks.data_sync.qb",
    "robosystems.tasks.data_sync.plaid",
    # SEC XBRL processing
    "robosystems.tasks.sec_xbrl.orchestration",
    "robosystems.tasks.sec_xbrl.ingestion",
    "robosystems.tasks.sec_xbrl.consolidation",
    "robosystems.tasks.sec_xbrl.maintenance",
    "robosystems.tasks.sec_xbrl.duckdb_ingestion",
    # Graph operations
    "robosystems.tasks.graph_operations.backup",
    "robosystems.tasks.graph_operations.create_entity_graph",
    "robosystems.tasks.graph_operations.create_graph",
    "robosystems.tasks.graph_operations.create_subgraph",
    # Agent operations
    "robosystems.tasks.agents.analyze",
    # Billing and credits
    "robosystems.tasks.billing.credit_allocation",
    "robosystems.tasks.billing.shared_credit_allocation",
    "robosystems.tasks.billing.storage_billing",
    "robosystems.tasks.billing.usage_collector",
    # Infrastructure
    "robosystems.tasks.infrastructure.auth_cleanup",
  ],
)

# Get worker autoscale from env
WORKER_AUTOSCALE = env.WORKER_AUTOSCALE

celery_app.conf.update(
  # Timezone settings
  enable_utc=True,  # Use UTC timezone
  timezone="UTC",  # Set timezone to UTC
  # Broker settings
  broker_connection_retry_on_startup=True,  # Retry connection on startup
  broker_transport_options={
    "confirm_publish": True,  # Confirm publish
    "consumer_timeout": 7200000,  # 2 hours in milliseconds
    "queue_order_strategy": "priority",  # Enable Redis priority queue support
    "priority_steps": list(range(11)),  # Priority levels 0-10 (0 is highest)
    "retry_on_timeout": True,  # Retry operations on timeout
    "socket_connect_timeout": 30,  # Connection timeout in seconds
    "visibility_timeout": 3600,  # 1 hour visibility timeout
    "fanout_prefix": True,
    "fanout_patterns": True,
    "socket_keepalive": True,  # Enable TCP keepalive to detect dead connections
    "master_name": None,  # Not using Redis Sentinel
  },
  # Task settings
  task_acks_late=True,  # Acknowledge tasks late
  task_time_limit=env.CELERY_TASK_TIME_LIMIT,
  task_soft_time_limit=env.CELERY_TASK_SOFT_TIME_LIMIT,
  task_default_queue=QUEUE_DEFAULT,  # Default queue
  task_default_priority=10,  # Default priority
  task_queue_max_priority=10,  # Maximum priority
  # Retry settings
  task_default_retry_delay=env.CELERY_TASK_RETRY_DELAY,
  task_max_retries=env.CELERY_TASK_MAX_RETRIES,
  # Worker settings - Use appropriate pool based on concurrency to avoid forking
  worker_pool="solo"
  if WORKER_AUTOSCALE == 1
  else "threads",  # Solo for single task, threads for multiple
  worker_concurrency=WORKER_AUTOSCALE,  # Use environment variable for consistency
  # worker_max_tasks_per_child not needed - solo mode has no child processes, threads manage memory well
  worker_prefetch_multiplier=env.CELERY_WORKER_PREFETCH_MULTIPLIER,  # 0 = no prefetch for queue-based scaling
  worker_soft_shutdown_timeout=env.CELERY_WORKER_SOFT_SHUTDOWN_TIMEOUT,
  # Connection loss handling - important for auto-scaling environments
  worker_cancel_long_running_tasks_on_connection_loss=True,  # Cancel tasks on connection loss (Celery 5.1+)
  broker_connection_retry=True,  # Automatically retry broker connections
  broker_connection_max_retries=10,  # Max retries for broker connection
  # Connection pooling to reduce connection count with 30+ workers
  broker_pool_limit=10,  # Reuse connections in pool (default is 10)
  redis_max_connections=50,  # Max connections in Redis pool per worker
  # Serialization settings (more secure than default pickle)
  task_serializer="json",  # Use JSON serializer
  result_serializer="json",  # Use JSON serializer
  accept_content=["json"],  # Accept JSON content
  # Memory optimization settings
  result_expires=env.CELERY_RESULT_EXPIRES,
  task_compression="gzip",  # Compress task messages
  result_compression="gzip",  # Compress result messages
  # Error handling
  task_acks_on_failure_or_timeout=True,  # Acknowledge tasks on failure or timeout
  task_reject_on_worker_lost=True,  # Tasks are rejected if worker crashes
  task_remote_tracebacks=True,  # Include remote tracebacks in errors
  # Redis backend options for result storage
  redis_backend_use_ssl=False,
  redis_socket_keepalive=True,
)

# Configure task queues
celery_app.conf.task_queues = [
  # Default queue for general tasks (includes data-sync and analytics for now)
  Queue(
    QUEUE_DEFAULT,
    Exchange(QUEUE_DEFAULT),
    routing_key=QUEUE_DEFAULT,
    queue_arguments={"x-max-priority": 10},
  ),
  # Critical queue for high-priority, fast tasks
  Queue(
    QUEUE_CRITICAL,
    Exchange(QUEUE_CRITICAL),
    routing_key=QUEUE_CRITICAL,
    queue_arguments={"x-max-priority": 10},
  ),
  # Dedicated shared extraction queue for downloading SEC XBRL files
  # Limited to 2 concurrent workers to avoid overwhelming SEC API
  Queue(
    QUEUE_SHARED_EXTRACTION,
    Exchange(QUEUE_SHARED_EXTRACTION),
    routing_key=QUEUE_SHARED_EXTRACTION,
    queue_arguments={
      "x-max-priority": 10,
      "x-message-ttl": 7200000,  # 2 hour TTL for extraction tasks
    },
  ),
  # Shared processing queue for SEC and other shared repository data
  Queue(
    QUEUE_SHARED_PROCESSING,
    Exchange(QUEUE_SHARED_PROCESSING),
    routing_key=QUEUE_SHARED_PROCESSING,
    queue_arguments={"x-max-priority": 10},
  ),
  # Dedicated shared ingestion queue with limited concurrency
  # This prevents overwhelming shared databases with too many concurrent ingestions
  Queue(
    QUEUE_SHARED_INGESTION,
    Exchange(QUEUE_SHARED_INGESTION),
    routing_key=QUEUE_SHARED_INGESTION,
    queue_arguments={
      "x-max-priority": 10,
      # Lower priority by default to process after data preparation
      "x-message-ttl": 3600000,  # 1 hour TTL for ingestion tasks
    },
  ),
]


# Configure Celery Beat Schedule
# Import and set the schedule directly so it's available for beat
from robosystems.tasks.schedule import BEAT_SCHEDULE  # noqa: E402

celery_app.conf.beat_schedule = BEAT_SCHEDULE


# Worker startup validation
@worker_ready.connect
def validate_worker_config(sender=None, **kwargs):
  """Validate configuration when worker starts."""
  logger.info("Starting Celery worker...")

  try:
    EnvValidator.validate_required_vars(env)
    config_summary = EnvValidator.get_config_summary(env)
    logger.info(f"Worker configuration validated successfully: {config_summary}")
  except Exception as e:
    logger.error(f"Worker configuration validation failed: {e}")
    if env.ENVIRONMENT == "prod":
      # In production, fail fast on invalid configuration
      raise
    else:
      # In development, log warning but continue
      logger.warning("Continuing with invalid configuration (development mode)")

  logger.info("Celery worker startup complete")
