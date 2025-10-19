"""
SEC Orchestration - Phase-Based Processing

Architecture:
- Phase 1: Download (rate-limited, shared-extraction queue)
- Phase 2: Process (unlimited parallelism, shared-processing queue)
- Phase 3: Consolidate (combine small parquet files into larger ones)
- Phase 4: Ingest (shared-ingestion queue, supports both Kuzu and Neo4j backends)

All state managed in Redis for distributed coordination.
"""

from typing import Dict, List, Optional
from datetime import datetime
import json

from celery import group
from robosystems.celery import celery_app
from robosystems.config import env
from robosystems.logger import logger
from robosystems.config.valkey_registry import ValkeyDatabase


class SECOrchestrator:
  """Orchestrator for phase-based SEC processing with checkpoint/resume support."""

  def __init__(self):
    """Initialize with Redis connection."""
    self.state_key = "sec:orchestrator:state"
    self.checkpoint_key = "sec:orchestrator:checkpoint"
    self.failed_companies_key = "sec:orchestrator:failed_companies"

    # Use factory method to handle SSL params correctly
    from robosystems.config.valkey_registry import create_redis_client

    self.redis_client = create_redis_client(
      ValkeyDatabase.PIPELINE_TRACKING, decode_responses=True
    )

  def _load_state(self) -> Dict:
    """Load state from Redis."""
    try:
      state_json = self.redis_client.get(self.state_key)
      if state_json:
        return json.loads(state_json)
    except Exception as e:
      logger.warning(f"Failed to load state: {e}")

    return {
      "phases": {
        "download": {"status": "pending", "progress": {}},
        "process": {"status": "pending", "progress": {}},
        "ingest": {"status": "pending", "progress": {}},
      },
      "companies": [],
      "years": [],
      "config": {},
      "stats": {},
      "last_updated": None,
    }

  def _save_state(self, state: Dict):
    """Save state to Redis."""
    state["last_updated"] = datetime.now().isoformat()

    try:
      state_json = json.dumps(state)
      self.redis_client.setex(
        self.state_key,
        30 * 86400,  # 30 days
        state_json,
      )
      logger.debug(f"Saved state to Redis: {len(state_json)} bytes")
    except Exception as e:
      logger.error(f"Failed to save state: {e}")
      raise

  def save_checkpoint(self, phase: str, completed_items: List[str]):
    """Save checkpoint for resumption after failure."""
    checkpoint = {
      "phase": phase,
      "completed_items": completed_items,
      "timestamp": datetime.now().isoformat(),
    }

    try:
      checkpoint_json = json.dumps(checkpoint)
      self.redis_client.setex(
        f"{self.checkpoint_key}:{phase}",
        7 * 86400,  # 7 days
        checkpoint_json,
      )
      logger.debug(f"Saved checkpoint for {phase}: {len(completed_items)} items")
    except Exception as e:
      logger.error(f"Failed to save checkpoint: {e}")

  def get_checkpoint(self, phase: str) -> Optional[Dict]:
    """Get checkpoint for a phase if it exists."""
    try:
      checkpoint_json = self.redis_client.get(f"{self.checkpoint_key}:{phase}")
      if checkpoint_json:
        return json.loads(checkpoint_json)
    except Exception as e:
      logger.warning(f"Failed to load checkpoint: {e}")
    return None

  def mark_company_failed(self, cik: str, phase: str, error: str):
    """Track failed companies for retry with error classification."""
    # Classify the error type for smart retry
    error_type = self._classify_error(error)

    failed_record = {
      "cik": cik,
      "phase": phase,
      "error": error,
      "error_type": error_type,
      "timestamp": datetime.now().isoformat(),
      "retry_count": 0,  # Track retry attempts
    }

    try:
      # Check if already failed and increment retry count
      existing_key = f"{phase}:{cik}"
      existing = self.redis_client.hget(self.failed_companies_key, existing_key)
      if existing:
        existing_record = json.loads(existing)
        failed_record["retry_count"] = existing_record.get("retry_count", 0) + 1

      self.redis_client.hset(
        self.failed_companies_key,
        existing_key,
        json.dumps(failed_record),
      )
    except Exception as e:
      logger.error(f"Failed to mark company as failed: {e}")

  def _classify_error(self, error_msg: str) -> str:
    """Classify error type for intelligent retry strategy."""
    error_lower = str(error_msg).lower()

    if (
      "rate limit" in error_lower
      or "429" in error_lower
      or "too many requests" in error_lower
    ):
      return "rate_limit"
    elif "timeout" in error_lower or "timed out" in error_lower:
      return "timeout"
    elif "connection" in error_lower or "network" in error_lower:
      return "network"
    elif "memory" in error_lower or "oom" in error_lower:
      return "memory"
    elif "not found" in error_lower or "404" in error_lower:
      return "not_found"
    elif "parse" in error_lower or "xbrl" in error_lower or "invalid" in error_lower:
      return "data_error"
    else:
      return "unknown"

  def get_failed_companies(self, phase: str = None) -> List[Dict]:
    """Get list of failed companies, optionally filtered by phase."""
    try:
      all_failed = self.redis_client.hgetall(self.failed_companies_key)
      failed_list = []

      for key, value in all_failed.items():
        if phase is None or key.startswith(f"{phase}:"):
          failed_list.append(json.loads(value))

      return failed_list
    except Exception as e:
      logger.error(f"Failed to get failed companies: {e}")
      return []


# ============================================================================
# PHASE 1: DOWNLOAD (Rate-Limited)
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,  # Rate-limited queue
  name="sec_xbrl.download_company_filings",
  max_retries=3,
  default_retry_delay=60,  # Wait 1 minute between retries (rate limiting)
)
def download_company_filings(
  cik: str,
  years: List[int],
  pipeline_id: str = None,
  skip_if_exists: bool = True,  # Skip if already downloaded
) -> Dict:
  """
  Download SEC filings for a single company (rate-limited).
  Checks S3 cache before downloading from SEC.

  This runs on the shared-extraction queue with limited workers
  to respect SEC rate limits.
  """
  from robosystems.operations.pipelines.sec_xbrl_filings import SECXBRLPipeline

  pipeline_id = pipeline_id or f"download_{cik}_{datetime.now().timestamp()}"
  pipeline = SECXBRLPipeline(pipeline_id)

  results = {
    "cik": cik,
    "status": "success",
    "years": {},
    "total_downloaded": 0,
    "total_cached": 0,
  }

  try:
    for year in years:
      logger.info(f"Downloading filings for CIK {cik} year {year}")

      # Discover and collect all filings for the year
      filings = pipeline._discover_entity_filings_by_year(
        cik,
        year,
        None,  # Get all filings for the year
      )

      downloaded = 0
      cached = 0

      for filing in filings:
        # Check if already in S3
        accession_number = filing.get("accessionNumber", "").replace("-", "")
        raw_s3_key = f"raw/year={year}/{cik}/{accession_number}.zip"

        if skip_if_exists:
          try:
            # Check if exists in S3
            pipeline.s3_client.head_object(Bucket=pipeline.raw_bucket, Key=raw_s3_key)
            cached += 1
            logger.debug(f"Found cached: {raw_s3_key}")
            continue  # Skip download
          except Exception:
            pass  # Not cached, proceed with download

        # Download from SEC
        try:
          raw_file = pipeline._collect_raw_filing(cik, filing, year, "STANDARD")
          if raw_file:
            downloaded += 1
            logger.info(f"Downloaded: {raw_s3_key}")
        except Exception as e:
          logger.warning(f"Failed to download {cik}/{accession_number}: {e}")

      results["years"][year] = {
        "filings_found": len(filings),
        "downloaded": downloaded,
        "cached": cached,
      }
      results["total_downloaded"] += downloaded
      results["total_cached"] += cached

  except Exception as e:
    logger.error(f"Failed to download filings for {cik}: {e}")
    results["status"] = "failed"
    results["error"] = str(e)

  return results


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.orchestrate_download_phase",
  max_retries=1,
)
def orchestrate_download_phase(
  companies: List[str],
  years: List[int],
  pipeline_id: str = None,
) -> Dict:
  """
  Orchestrate download phase for all companies with checkpoint support.
  Creates individual download tasks for each company.
  """
  pipeline_id = pipeline_id or f"download_phase_{datetime.now().timestamp()}"
  start_time = datetime.now()

  logger.info(f"Starting download phase for {len(companies)} companies, years {years}")

  # Create download tasks for each company
  download_tasks = []
  for cik in companies:
    task = download_company_filings.s(  # type: ignore[attr-defined]
      cik=cik,
      years=years,
      pipeline_id=pipeline_id,
      skip_if_exists=True,  # Enable caching
    )
    download_tasks.append(task)

  # Schedule phase completion handler using chord
  from celery import chord

  # Use a chord to execute all tasks and then run completion handler
  completion_callback = handle_phase_completion.s(  # type: ignore[attr-defined]
    phase="download",
    start_time=start_time.isoformat(),
    total_companies=len(companies),
  )

  # Create chord with callback support (this executes the tasks)
  download_chord = chord(download_tasks)(completion_callback)

  # Store job ID for monitoring
  orchestrator = SECOrchestrator()
  state = orchestrator._load_state()
  state["phases"]["download"]["job_id"] = download_chord.id
  state["phases"]["download"]["status"] = "running"
  state["phases"]["download"]["started_at"] = start_time.isoformat()
  orchestrator._save_state(state)

  return {
    "status": "started",
    "job_id": download_chord.id,
    "companies": len(companies),
    "pipeline_id": pipeline_id,
  }


# ============================================================================
# PHASE 2: PROCESS (Unlimited Parallelism)
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_PROCESSING,  # High parallelism queue
  name="sec_xbrl.process_company_filings",
  max_retries=3,
)
def process_company_filings(
  cik: str,
  years: List[int],
  pipeline_id: str = None,
) -> Dict:
  """
  Process already-downloaded filings for a company.
  This can run with unlimited parallelism since it only reads from S3.
  """
  from robosystems.operations.pipelines.sec_xbrl_filings import SECXBRLPipeline

  pipeline_id = pipeline_id or f"process_{cik}_{datetime.now().timestamp()}"
  pipeline = SECXBRLPipeline(pipeline_id)

  results = {
    "cik": cik,
    "status": "success",
    "years": {},
    "total_processed": 0,
  }

  try:
    for year in years:
      # Get list of raw files from S3
      prefix = f"raw/year={year}/{cik}/"

      raw_files = []
      paginator = pipeline.s3_client.get_paginator("list_objects_v2")
      for page in paginator.paginate(Bucket=pipeline.raw_bucket, Prefix=prefix):
        if "Contents" in page:
          for obj in page["Contents"]:
            if obj["Key"].endswith(".zip"):
              raw_files.append(obj["Key"])

      processed = 0
      for raw_file_key in raw_files:
        try:
          # Process the raw file to parquet
          result = pipeline._process_single_raw_file(
            raw_file_key, year, pipeline, refresh=False
          )
          if result and result["status"] == "completed":
            processed += 1
        except Exception as e:
          logger.warning(f"Failed to process {raw_file_key}: {e}")

      results["years"][year] = {
        "raw_files": len(raw_files),
        "processed": processed,
      }
      results["total_processed"] += processed

  except Exception as e:
    logger.error(f"Failed to process filings for {cik}: {e}")
    results["status"] = "failed"
    results["error"] = str(e)

  return results


@celery_app.task(
  queue=env.QUEUE_SHARED_PROCESSING,
  name="sec_xbrl.orchestrate_process_phase",
  max_retries=1,
)
def orchestrate_process_phase(
  companies: List[str],
  years: List[int],
  batch_size: int = 50,  # For monitoring, not parallelism control
  pipeline_id: str = None,
) -> Dict:
  """
  Orchestrate processing phase with unlimited parallelism.
  """
  pipeline_id = pipeline_id or f"process_phase_{datetime.now().timestamp()}"

  logger.info(
    f"Starting process phase for {len(companies)} companies with unlimited parallelism"
  )

  # Create process tasks for each company
  process_tasks = []
  for cik in companies:
    task = process_company_filings.s(  # type: ignore[attr-defined]
      cik=cik,
      years=years,
      pipeline_id=pipeline_id,
    )
    process_tasks.append(task)

  # Execute all processing with maximum parallelism
  job = group(process_tasks).apply_async()  # type: ignore[attr-defined]

  # Store job ID for monitoring
  orchestrator = SECOrchestrator()
  state = orchestrator._load_state()
  state["phases"]["process"]["job_id"] = job.id
  state["phases"]["process"]["status"] = "running"
  state["phases"]["process"]["started_at"] = datetime.now().isoformat()
  state["phases"]["process"]["batch_size"] = batch_size  # For monitoring only
  orchestrator._save_state(state)

  return {
    "status": "started",
    "job_id": job.id,
    "companies": len(companies),
    "pipeline_id": pipeline_id,
  }


# ============================================================================
# PHASE COMPLETION HANDLING
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.handle_phase_completion",
  max_retries=1,
)
def handle_phase_completion(
  results: List[Dict],
  phase: str,
  start_time: str,
  total_companies: int,
) -> Dict:
  """
  Handle phase completion: publish metrics, cleanup connections, handle failures.

  This runs automatically after all phase tasks complete via Celery chord.
  """
  try:
    start_dt = datetime.fromisoformat(start_time)
    duration = (datetime.now() - start_dt).total_seconds()

    # Analyze results
    successful = 0
    failed_companies = []
    total_records = 0
    total_files = 0
    errors_by_type = {}

    orchestrator = SECOrchestrator()

    for result in results:
      if result.get("status") == "success":
        successful += 1
        total_records += result.get("total_records", 0)
        total_files += result.get("total_downloaded", 0) + result.get("total_cached", 0)
      else:
        # Track failure
        cik = result.get("cik", "unknown")
        error = result.get("error", "Unknown error")
        failed_companies.append(cik)

        # Mark company as failed with error classification
        orchestrator.mark_company_failed(cik, phase, error)

        # Track error types
        error_type = orchestrator._classify_error(error)
        errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1

    failed = len(failed_companies)

    # Calculate and publish metrics
    metrics = {
      "duration_seconds": duration,
      "success_rate": (successful / total_companies * 100) if total_companies else 0,
      "total_records": total_records,
      "total_files": total_files,
      "companies_processed": successful,
      "companies_failed": failed,
      "error_breakdown": errors_by_type,
    }

    publish_phase_metrics(phase, metrics)

    # Update orchestrator state
    state = orchestrator._load_state()
    state["phases"][phase]["status"] = "completed"
    state["phases"][phase]["completed_at"] = datetime.now().isoformat()
    state["phases"][phase]["stats"] = metrics
    orchestrator._save_state(state)

    # Log summary
    logger.info(
      f"Phase {phase} completed: {successful}/{total_companies} successful, "
      f"{failed} failed, {duration:.1f}s duration"
    )

    # Clean up connections
    cleanup_task = cleanup_phase_connections.apply_async(  # type: ignore[attr-defined]
      kwargs={"phase": phase},
      countdown=5,  # Wait 5 seconds for things to settle
    )

    # If there are failures, suggest retry
    if failed_companies:
      logger.warning(
        f"Phase {phase} had {failed} failures. "
        f"Run smart_retry_failed_companies('{phase}') to retry."
      )

      # Auto-retry rate limit errors after delay
      if errors_by_type.get("rate_limit", 0) > 0:
        smart_retry_failed_companies.apply_async(  # type: ignore[attr-defined]
          kwargs={"phase": phase},
          countdown=300,  # Wait 5 minutes for rate limits
        )

    return {
      "status": "completed",
      "phase": phase,
      "metrics": metrics,
      "cleanup_task_id": cleanup_task.id,
    }

  except Exception as e:
    logger.error(f"Failed to handle {phase} phase completion: {e}")
    return {
      "status": "failed",
      "phase": phase,
      "error": str(e),
    }


# ============================================================================
# CONNECTION MANAGEMENT
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.cleanup_phase_connections",
  max_retries=1,
)
def cleanup_phase_connections(phase: str) -> Dict:
  """
  Clean up connections and resources after phase completion.

  This addresses the connection issues that require Docker restarts
  after heavy ingestion operations.

  Args:
      phase: Phase that just completed
  """
  import gc

  try:
    logger.info(f"Starting connection cleanup after {phase} phase")

    # Clear Kuzu client factory connection pools
    try:
      from robosystems.graph_api.client.factory import GraphClientFactory

      if hasattr(GraphClientFactory, "_connection_pools"):
        pool_count = len(GraphClientFactory._connection_pools)
        GraphClientFactory._connection_pools.clear()
        logger.info(f"Cleared {pool_count} Kuzu connection pools")

      if hasattr(GraphClientFactory, "_pool_stats"):
        GraphClientFactory._pool_stats.clear()
        logger.info("Cleared Kuzu pool statistics")

    except ImportError:
      logger.debug("Kuzu client factory not available")
    except Exception as e:
      logger.warning(f"Failed to clear Kuzu pools: {e}")

    # Clear any cached S3 clients
    try:
      from robosystems.adapters.s3 import S3AdapterMixin

      # If S3 adapter has any class-level caches, clear them
      if hasattr(S3AdapterMixin, "_s3_client"):
        S3AdapterMixin._s3_client = None
        logger.info("Cleared cached S3 client")

    except ImportError:
      logger.debug("S3 adapter not available")
    except Exception as e:
      logger.warning(f"Failed to clear S3 caches: {e}")

    # Force garbage collection to release memory
    collected = gc.collect()
    logger.info(f"Garbage collection freed {collected} objects")

    # If this is after ingestion, also clear Redis connection pools
    if phase == "ingest":
      try:
        # Redis connection pools are managed per-process, so we can't clear
        # them directly, but we can close idle connections
        # Use factory method to handle SSL params correctly
        from robosystems.config.valkey_registry import create_redis_client

        temp_client = create_redis_client(
          ValkeyDatabase.PIPELINE_TRACKING, decode_responses=False
        )
        temp_client.connection_pool.disconnect()
        logger.info("Disconnected Redis connection pool")
      except Exception as e:
        logger.warning(f"Failed to disconnect Redis pool: {e}")

    # Add a small delay to allow connections to fully close
    import time

    time.sleep(2)

    return {
      "status": "success",
      "phase": phase,
      "message": f"Cleaned up connections after {phase} phase",
    }

  except Exception as e:
    logger.error(f"Failed to cleanup connections after {phase}: {e}")
    return {
      "status": "failed",
      "phase": phase,
      "error": str(e),
    }


# ============================================================================
# MONITORING & OBSERVABILITY
# ============================================================================


def publish_phase_metrics(phase: str, stats: Dict):
  """
  Publish phase metrics for monitoring (internal tracking).

  Instead of CloudWatch, we'll use Redis for metrics that can be
  consumed by external monitoring systems if needed.
  """
  try:
    # Use factory method to handle SSL params correctly
    from robosystems.config.valkey_registry import create_redis_client

    redis_client = create_redis_client(
      ValkeyDatabase.PIPELINE_TRACKING, decode_responses=True
    )

    # Store metrics with TTL for dashboard consumption
    metrics_key = f"sec:metrics:{phase}:{datetime.now().strftime('%Y%m%d')}"

    metrics = {
      "phase": phase,
      "timestamp": datetime.now().isoformat(),
      "duration_seconds": stats.get("duration_seconds", 0),
      "success_rate": stats.get("success_rate", 0),
      "total_records": stats.get("total_records", 0),
      "total_files": stats.get("total_files", 0),
      "companies_processed": stats.get("companies_processed", 0),
      "companies_failed": stats.get("companies_failed", 0),
      "error_breakdown": stats.get("error_breakdown", {}),
    }

    # Store metrics
    redis_client.hset(metrics_key, datetime.now().timestamp(), json.dumps(metrics))

    # Set expiry for 30 days
    redis_client.expire(metrics_key, 30 * 86400)

    # Also update summary metrics for quick access
    summary_key = f"sec:metrics:summary:{phase}"
    redis_client.hset(summary_key, "last_run", json.dumps(metrics))

    # Track phase completion for rate calculation
    if phase == "download":
      # Track download rate (filings per minute)
      if stats.get("duration_seconds", 0) > 0:
        rate = (stats.get("total_files", 0) / stats["duration_seconds"]) * 60
        redis_client.hset(summary_key, "download_rate_per_min", rate)

    elif phase == "process":
      # Track processing throughput (records per second)
      if stats.get("duration_seconds", 0) > 0:
        throughput = stats.get("total_records", 0) / stats["duration_seconds"]
        redis_client.hset(summary_key, "process_throughput_per_sec", throughput)

    elif phase == "ingest":
      # Track ingestion speed (records per second)
      if stats.get("duration_seconds", 0) > 0:
        speed = stats.get("total_records", 0) / stats["duration_seconds"]
        redis_client.hset(summary_key, "ingest_speed_per_sec", speed)

    logger.info(f"Published metrics for {phase}: {metrics}")

  except Exception as e:
    logger.warning(f"Failed to publish metrics for {phase}: {e}")


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.get_pipeline_metrics",
  max_retries=1,
)
def get_pipeline_metrics(days: int = 7) -> Dict:
  """
  Retrieve pipeline metrics for monitoring dashboard.

  Args:
      days: Number of days of history to retrieve
  """
  try:
    # Use factory method to handle SSL params correctly
    from robosystems.config.valkey_registry import create_redis_client

    redis_client = create_redis_client(
      ValkeyDatabase.PIPELINE_TRACKING, decode_responses=True
    )

    metrics = {
      "phases": {},
      "trends": {},
      "current_state": {},
    }

    # Get summary metrics for each phase
    for phase in ["download", "process", "ingest"]:
      summary_key = f"sec:metrics:summary:{phase}"
      phase_metrics = redis_client.hgetall(summary_key)

      if phase_metrics:
        # Parse the last run data
        if "last_run" in phase_metrics:  # type: ignore[operator]
          metrics["phases"][phase] = json.loads(phase_metrics["last_run"])  # type: ignore[index]

        # Add rate/throughput metrics
        if "download_rate_per_min" in phase_metrics:  # type: ignore[operator]
          metrics["phases"][phase]["rate_per_min"] = float(
            phase_metrics["download_rate_per_min"]  # type: ignore[index]
          )
        if "process_throughput_per_sec" in phase_metrics:  # type: ignore[operator]
          metrics["phases"][phase]["throughput_per_sec"] = float(
            phase_metrics["process_throughput_per_sec"]  # type: ignore[index]
          )
        if "ingest_speed_per_sec" in phase_metrics:  # type: ignore[operator]
          metrics["phases"][phase]["speed_per_sec"] = float(
            phase_metrics["ingest_speed_per_sec"]  # type: ignore[index]
          )

    # Get current orchestrator state
    orchestrator = SECOrchestrator()
    state = orchestrator._load_state()
    metrics["current_state"] = {
      "companies": len(state.get("companies", [])),
      "years": state.get("years", []),
      "phases": state.get("phases", {}),
    }

    # Calculate trends if we have history
    # (This could be enhanced to track daily metrics)

    return metrics

  except Exception as e:
    logger.error(f"Failed to retrieve pipeline metrics: {e}")
    return {"error": str(e)}


# ============================================================================
# ENHANCED ERROR RECOVERY & RETRY
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.smart_retry_failed",
  max_retries=1,
)
def smart_retry_failed_companies(
  phase: str, max_attempts: int = 3, force: bool = False
) -> Dict:
  """
  Intelligently retry failed companies based on error type.

  Args:
      phase: Phase to retry (download, process, ingest)
      max_attempts: Maximum retry attempts per company
      force: Force retry even if max attempts reached
  """
  orchestrator = SECOrchestrator()
  failed = orchestrator.get_failed_companies(phase)

  if not failed:
    return {
      "status": "success",
      "message": f"No failed companies found for {phase}",
    }

  # Group by error type for different retry strategies
  error_groups = {}
  skipped = []

  for company in failed:
    retry_count = company.get("retry_count", 0)
    error_type = company.get("error_type", "unknown")

    # Skip if max attempts reached (unless forced)
    if retry_count >= max_attempts and not force:
      skipped.append(company["cik"])
      logger.warning(f"Skipping {company['cik']}: max attempts ({retry_count}) reached")
      continue

    error_groups.setdefault(error_type, []).append(company)

  # Apply different retry strategies per error type
  retry_tasks = []
  retry_config = {
    "rate_limit": {"delay": 300, "batch_size": 1},  # 5 min delay, one at a time
    "timeout": {"delay": 60, "batch_size": 5},  # 1 min delay, smaller batches
    "network": {"delay": 30, "batch_size": 10},  # 30 sec delay, medium batches
    "memory": {"delay": 120, "batch_size": 2},  # 2 min delay, tiny batches
    "unknown": {"delay": 60, "batch_size": 5},  # Default strategy
  }

  for error_type, companies in error_groups.items():
    if error_type in ["not_found", "data_error"]:
      # Don't auto-retry data errors
      logger.error(
        f"Skipping {len(companies)} companies with {error_type} errors - manual review required"
      )
      for company in companies:
        skipped.append(company["cik"])
      continue

    config = retry_config.get(error_type, retry_config["unknown"])
    ciks = [c["cik"] for c in companies]

    # Create retry tasks with appropriate delays
    for i in range(0, len(ciks), config["batch_size"]):
      batch = ciks[i : i + config["batch_size"]]

      if phase == "download":
        from robosystems.tasks.sec_xbrl.orchestration import download_company_filings

        for cik in batch:
          task = download_company_filings.apply_async(  # type: ignore[attr-defined]
            kwargs={"cik": cik, "years": [2024]},  # TODO: Get years from state
            countdown=config["delay"],
          )
          retry_tasks.append(task)

      elif phase == "process":
        from robosystems.tasks.sec_xbrl.orchestration import process_company_filings

        for cik in batch:
          task = process_company_filings.apply_async(  # type: ignore[attr-defined]
            kwargs={"cik": cik, "years": [2024]},
            countdown=config["delay"],
          )
          retry_tasks.append(task)

    logger.info(
      f"Retrying {len(ciks)} companies with {error_type} errors "
      f"(delay={config['delay']}s, batch_size={config['batch_size']})"
    )

  return {
    "status": "retry_started",
    "retried": len(retry_tasks),
    "skipped": len(skipped),
    "error_breakdown": {k: len(v) for k, v in error_groups.items()},
  }


# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.plan_phased_processing",
  max_retries=1,
)
def plan_phased_processing(
  start_year: int,
  end_year: int,
  max_companies: Optional[int] = None,  # Limit companies for testing
  companies_per_batch: int = 50,  # For monitoring/checkpointing only
  cik_filter: Optional[str] = None,  # Filter to specific CIK for local testing
  backend: str = "kuzu",  # Backend type for ingestion ("kuzu" or "neo4j")
) -> Dict:
  """
  Plan phased SEC processing with optional company limit for testing.

  Args:
      start_year: Starting year
      end_year: Ending year
      max_companies: Maximum companies to process (None = all)
      companies_per_batch: Batch size for monitoring (not parallelism)
      cik_filter: Optional CIK to filter to a single company
      backend: Backend type for ingestion ("kuzu" or "neo4j")
  """
  from robosystems.adapters.sec import SECClient

  logger.info(
    f"Planning phased processing ({backend} backend): years {start_year}-{end_year}, "
    f"max_companies={max_companies}, batch_size={companies_per_batch}, "
    f"cik_filter={cik_filter}"
  )

  # Get companies
  sec_client = SECClient()
  companies_df = sec_client.get_companies_df()

  # Filter to specific CIK if provided (for local testing)
  if cik_filter:
    # Ensure CIK is properly formatted (10 digits with leading zeros)
    cik_filter = str(cik_filter).zfill(10)
    companies_df = companies_df[companies_df["cik_str"] == int(cik_filter)]
    if companies_df.empty:
      logger.error(f"No company found with CIK: {cik_filter}")
      return {"status": "failed", "error": f"Company not found: CIK {cik_filter}"}
    logger.info(f"Filtered to single company: CIK {cik_filter}")
  # Limit companies if specified (for testing)
  elif max_companies:
    companies_df = companies_df.head(max_companies)
    logger.info(f"Limited to {max_companies} companies for testing")

  companies = companies_df["cik_str"].tolist()
  years = list(range(start_year, end_year + 1))

  # Initialize state
  orchestrator = SECOrchestrator()
  state = {
    "companies": companies,
    "years": years,
    "config": {
      "max_companies": max_companies,
      "companies_per_batch": companies_per_batch,
      "backend": backend,
    },
    "phases": {
      "download": {"status": "pending"},
      "process": {"status": "pending"},
      "ingest": {"status": "pending"},
    },
    "stats": {
      "total_companies": len(companies),
      "total_years": len(years),
    },
    "created_at": datetime.now().isoformat(),
  }

  orchestrator._save_state(state)

  return {
    "status": "success",
    "companies": len(companies),
    "years": years,
    "max_companies": max_companies,
    "backend": backend,
    "phases": ["download", "process", "consolidate", "ingest"],
    "message": f"Ready to process {len(companies)} companies for {len(years)} years ({backend} backend)",
  }


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.start_phase",
  max_retries=1,
)
def start_phase(
  phase: str, resume: bool = False, retry_failed: bool = False, backend: str = "kuzu"
) -> Dict:
  """
  Start a specific processing phase with optional resume support.

  Args:
      phase: Phase to start (download, process, consolidate, ingest)
      resume: Resume from last checkpoint if available
      retry_failed: Include previously failed companies
      backend: Backend type for ingestion ("kuzu" or "neo4j")
  """
  orchestrator = SECOrchestrator()
  state = orchestrator._load_state()

  if not state.get("companies"):
    return {
      "status": "failed",
      "error": "No plan found. Run plan_phased_processing first.",
    }

  companies = state["companies"]
  years = state["years"]
  config = state["config"]

  logger.info(f"Starting phase '{phase}' with backend: {backend}")

  # Check for checkpoint if resuming
  if resume:
    checkpoint = orchestrator.get_checkpoint(phase)
    if checkpoint:
      completed = set(checkpoint["completed_items"])
      # Filter out completed companies
      companies = [c for c in companies if c not in completed]
      logger.info(
        f"Resuming {phase} from checkpoint: {len(completed)} already completed, "
        f"{len(companies)} remaining"
      )

  # Add failed companies if retrying
  if retry_failed:
    failed = orchestrator.get_failed_companies(phase)
    failed_ciks = [f["cik"] for f in failed]
    # Add failed companies back to processing list (deduplicated)
    companies = list(set(companies + failed_ciks))
    logger.info(f"Added {len(failed_ciks)} failed companies to retry")

  if phase == "download":
    # Start download phase (rate-limited)
    result = orchestrate_download_phase.apply_async(  # type: ignore[attr-defined]
      kwargs={
        "companies": companies,
        "years": years,
      }
    )
    return {
      "status": "started",
      "phase": "download",
      "task_id": result.id,
      "companies": len(companies),
    }

  elif phase == "process":
    # Start processing phase (unlimited parallelism)
    result = orchestrate_process_phase.apply_async(  # type: ignore[attr-defined]
      kwargs={
        "companies": companies,
        "years": years,
        "batch_size": config["companies_per_batch"],
      }
    )
    return {
      "status": "started",
      "phase": "process",
      "task_id": result.id,
      "companies": len(companies),
    }

  elif phase == "consolidate":
    # Start consolidation phase
    from robosystems.tasks.sec_xbrl.consolidation import orchestrate_consolidation_phase

    result = orchestrate_consolidation_phase.apply_async(  # type: ignore[attr-defined]
      kwargs={
        "years": years,
        "bucket": env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed",
        "pipeline_id": f"orchestrator_{datetime.now().timestamp()}",
      }
    )
    return {
      "status": "started",
      "phase": "consolidate",
      "task_id": result.id,
      "years": years,
    }

  elif phase == "ingest":
    # Use existing ingestion - will now use consolidated files
    from robosystems.tasks.sec_xbrl.ingestion import ingest_sec_data

    logger.info(f"Starting ingestion phase with {backend} backend for years {years}")

    tasks = []
    for year in years:
      task = ingest_sec_data.apply_async(  # type: ignore[attr-defined]
        kwargs={
          "pipeline_run_id": f"orchestrator_ingest_{year}_{datetime.now().timestamp()}",
          "year": year,
          "db_name": "sec",
          "graph_id": "sec",
          "bucket": env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed",
          "use_consolidated": True,  # Use consolidated files
          "backend": backend,  # Pass backend parameter
        }
      )
      tasks.append(task)

    return {
      "status": "started",
      "phase": "ingest",
      "backend": backend,
      "years": years,
      "tasks": len(tasks),
    }

  else:
    return {
      "status": "failed",
      "error": f"Unknown phase: {phase}",
    }


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.get_phase_status",
  max_retries=1,
)
def get_phase_status(include_failed: bool = False) -> Dict:
  """
  Get detailed status of all phases including checkpoints and failures.

  Args:
      include_failed: Include list of failed companies
  """
  orchestrator = SECOrchestrator()
  state = orchestrator._load_state()

  if not state.get("companies"):
    return {
      "status": "no_plan",
      "message": "No processing plan found. Run plan_phased_processing first.",
    }

  # Enhance phase status with checkpoint info
  phases_status = state.get("phases", {})

  for phase_name in phases_status:
    # Add checkpoint info
    checkpoint = orchestrator.get_checkpoint(phase_name)
    if checkpoint:
      phases_status[phase_name]["checkpoint"] = {
        "completed_count": len(checkpoint["completed_items"]),
        "last_updated": checkpoint["timestamp"],
      }

    # Add failed companies count
    failed = orchestrator.get_failed_companies(phase_name)
    if failed:
      phases_status[phase_name]["failed_count"] = len(failed)
      if include_failed:
        phases_status[phase_name]["failed_companies"] = failed

  return {
    "status": "success",
    "config": state.get("config", {}),
    "stats": state.get("stats", {}),
    "phases": phases_status,
    "last_updated": state.get("last_updated"),
    "can_resume": any(
      orchestrator.get_checkpoint(p) is not None
      for p in ["download", "process", "ingest"]
    ),
  }
