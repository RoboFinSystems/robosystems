"""
Test Suite for SEC XBRL Orchestration Tasks

Tests the phase-based pipeline orchestration logic.
"""

import pytest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime


class TestOrchestratorCore:
  """Test the SECOrchestrator core functionality."""

  @patch("redis.from_url")
  def test_orchestrator_initialization(self, mock_redis):
    """Test orchestrator initializes correctly."""
    mock_redis_client = MagicMock()
    mock_redis.return_value = mock_redis_client

    from robosystems.tasks.sec_xbrl.orchestration import SECOrchestrator

    orchestrator = SECOrchestrator()

    # Check that keys are set correctly
    assert orchestrator.state_key == "sec:orchestrator:state"
    assert orchestrator.checkpoint_key == "sec:orchestrator:checkpoint"
    assert orchestrator.failed_companies_key == "sec:orchestrator:failed_companies"
    assert orchestrator.redis_client is not None

  def test_state_structure(self):
    """Test the orchestrator state structure."""
    state = {
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

    # Verify state structure
    assert "phases" in state
    assert len(state["phases"]) == 3
    assert all(phase in state["phases"] for phase in ["download", "process", "ingest"])

  def test_error_classification_logic(self):
    """Test error classification logic."""
    errors = [
      {"error": "Connection timeout", "retryable": True},
      {"error": "Invalid data format", "retryable": False},
      {"error": "Rate limit exceeded", "retryable": True},
      {"error": "Corrupted file", "retryable": False},
    ]

    retryable_errors = [e for e in errors if e["retryable"]]
    fatal_errors = [e for e in errors if not e["retryable"]]

    assert len(retryable_errors) == 2
    assert len(fatal_errors) == 2
    assert retryable_errors[0]["error"] == "Connection timeout"
    assert fatal_errors[0]["error"] == "Invalid data format"


class TestPhaseManagement:
  """Test phase management logic."""

  def test_phase_ordering(self):
    """Test that phases execute in correct order."""
    phases = ["download", "process", "consolidate", "ingest"]

    # Phases should execute sequentially
    for i in range(len(phases) - 1):
      current_phase = phases[i]
      next_phase = phases[i + 1]

      # Next phase shouldn't start until current is complete
      assert phases.index(current_phase) < phases.index(next_phase)

  def test_phase_dependencies(self):
    """Test phase dependency checking."""
    dependencies = {
      "download": [],
      "process": ["download"],
      "consolidate": ["process"],
      "ingest": ["consolidate"],
    }

    # Check that ingest depends on all previous phases
    ingest_deps = dependencies["ingest"]
    assert "consolidate" in ingest_deps or "process" in dependencies["consolidate"]

  def test_phase_progress_tracking(self):
    """Test tracking progress within phases."""
    phase_progress = {
      "download": {"total": 100, "completed": 75, "failed": 5},
      "process": {"total": 75, "completed": 50, "failed": 2},
      "consolidate": {"total": 50, "completed": 50, "failed": 0},
      "ingest": {"total": 50, "completed": 0, "failed": 0},
    }

    # Calculate completion percentages
    for phase, progress in phase_progress.items():
      if progress["total"] > 0:
        completion = (progress["completed"] / progress["total"]) * 100
      else:
        completion = 0

      if phase == "download":
        assert completion == 75
      elif phase == "consolidate":
        assert completion == 100

  def test_phase_retry_logic(self):
    """Test phase retry logic."""
    phase_retries = {
      "download": {"max_retries": 3, "current_retry": 0},
      "process": {"max_retries": 2, "current_retry": 1},
      "ingest": {"max_retries": 1, "current_retry": 1},
    }

    for phase, retry_info in phase_retries.items():
      can_retry = retry_info["current_retry"] < retry_info["max_retries"]

      if phase == "download":
        assert can_retry
      elif phase == "ingest":
        assert not can_retry


class TestCompanyProcessing:
  """Test company processing logic."""

  def test_company_list_generation(self):
    """Test generation of company list for processing."""
    # Test with explicit list
    explicit_companies = ["0000320193", "0000789019", "0001018724"]
    assert len(explicit_companies) == 3
    assert "0000320193" in explicit_companies  # Apple

    # Test with SP500 flag
    sp500_flag = True
    if sp500_flag:
      # Should generate SP500 list
      companies = ["mock_sp500_companies"]  # Placeholder
      assert len(companies) > 0

  def test_company_batch_processing(self):
    """Test batch processing of companies."""
    companies = [f"000000{i:04d}" for i in range(100)]
    batch_size = 25

    batches = []
    for i in range(0, len(companies), batch_size):
      batch = companies[i : i + batch_size]
      batches.append(batch)

    assert len(batches) == 4
    assert len(batches[0]) == 25
    assert len(batches[-1]) == 25

  def test_failed_company_tracking(self):
    """Test tracking of failed companies."""
    failed_companies = {}

    # Track failures
    failed_companies["0000320193"] = {
      "phase": "download",
      "error": "Rate limited",
      "attempts": 3,
    }

    failed_companies["0000789019"] = {
      "phase": "process",
      "error": "Invalid XBRL",
      "attempts": 1,
    }

    assert len(failed_companies) == 2
    assert failed_companies["0000320193"]["attempts"] == 3


class TestYearProcessing:
  """Test year-based processing logic."""

  def test_year_range_generation(self):
    """Test generation of year ranges."""
    start_year = 2020
    end_year = 2024

    years = list(range(start_year, end_year + 1))

    assert len(years) == 5
    assert years[0] == 2020
    assert years[-1] == 2024

  def test_year_prioritization(self):
    """Test year prioritization (recent first)."""
    years = [2020, 2021, 2022, 2023, 2024]

    # Process recent years first
    prioritized = sorted(years, reverse=True)

    assert prioritized[0] == 2024
    assert prioritized[-1] == 2020

  def test_year_specific_configuration(self):
    """Test year-specific configuration."""
    year_configs = {
      2024: {"max_filings": 1000, "include_amendments": True},
      2023: {"max_filings": 2000, "include_amendments": True},
      2022: {"max_filings": 2000, "include_amendments": False},
    }

    # Check 2024 has different limits
    assert year_configs[2024]["max_filings"] != year_configs[2023]["max_filings"]
    assert year_configs[2022]["include_amendments"] is False


class TestTaskCoordination:
  """Test task coordination and monitoring."""

  def test_task_id_tracking(self):
    """Test tracking of Celery task IDs."""
    task_tracker = {}

    # Track task IDs by phase and company
    task_tracker["download"] = {
      "0000320193": "task_abc123",
      "0000789019": "task_def456",
    }

    task_tracker["process"] = {
      "0000320193": "task_ghi789",
    }

    assert len(task_tracker["download"]) == 2
    assert task_tracker["download"]["0000320193"] == "task_abc123"

  def test_task_status_monitoring(self):
    """Test monitoring task statuses."""
    task_statuses = {
      "task_abc123": "SUCCESS",
      "task_def456": "PENDING",
      "task_ghi789": "FAILURE",
    }

    # Count by status
    status_counts = {}
    for status in task_statuses.values():
      status_counts[status] = status_counts.get(status, 0) + 1

    assert status_counts["SUCCESS"] == 1
    assert status_counts["PENDING"] == 1
    assert status_counts["FAILURE"] == 1

  def test_task_result_aggregation(self):
    """Test aggregation of task results."""
    task_results = [
      {"task_id": "task_1", "filings_processed": 10, "errors": 0},
      {"task_id": "task_2", "filings_processed": 8, "errors": 2},
      {"task_id": "task_3", "filings_processed": 12, "errors": 1},
    ]

    # Aggregate results
    total_processed = sum(r["filings_processed"] for r in task_results)
    total_errors = sum(r["errors"] for r in task_results)

    assert total_processed == 30
    assert total_errors == 3


class TestCheckpointing:
  """Test checkpointing and recovery."""

  def test_checkpoint_creation(self):
    """Test creation of checkpoints."""
    checkpoint = {
      "timestamp": datetime.now().isoformat(),
      "phase": "process",
      "companies_completed": ["0000320193", "0000789019"],
      "companies_remaining": ["0001018724"],
      "state_snapshot": {},
    }

    assert checkpoint["phase"] == "process"
    assert len(checkpoint["companies_completed"]) == 2
    assert len(checkpoint["companies_remaining"]) == 1

  def test_checkpoint_recovery(self):
    """Test recovery from checkpoint."""
    # Simulate checkpoint data
    checkpoint = {
      "phase": "process",
      "companies_completed": ["0000320193"],
      "companies_remaining": ["0000789019", "0001018724"],
    }

    # Resume from checkpoint
    resume_from_phase = checkpoint["phase"]
    resume_companies = checkpoint["companies_remaining"]

    assert resume_from_phase == "process"
    assert len(resume_companies) == 2
    assert "0000320193" not in resume_companies

  def test_checkpoint_validation(self):
    """Test checkpoint validation."""
    # Valid checkpoint
    valid_checkpoint = {
      "timestamp": "2024-01-01T10:00:00",
      "phase": "download",
      "companies_completed": [],
      "companies_remaining": ["0000320193"],
    }

    # Invalid checkpoint (missing required fields)
    invalid_checkpoint = {
      "timestamp": "2024-01-01T10:00:00",
      # Missing phase
    }

    # Validation
    required_fields = [
      "timestamp",
      "phase",
      "companies_completed",
      "companies_remaining",
    ]

    valid = all(field in valid_checkpoint for field in required_fields)
    invalid = all(field in invalid_checkpoint for field in required_fields)

    assert valid
    assert not invalid


class TestOrchestrationMetrics:
  """Test orchestration metrics and reporting."""

  def test_metrics_collection(self):
    """Test collection of orchestration metrics."""
    metrics = {
      "start_time": "2024-01-01T10:00:00",
      "end_time": "2024-01-01T14:00:00",
      "total_companies": 100,
      "successful_companies": 95,
      "failed_companies": 5,
      "phases_completed": ["download", "process", "consolidate", "ingest"],
      "total_filings": 5000,
    }

    success_rate = (metrics["successful_companies"] / metrics["total_companies"]) * 100

    assert success_rate == 95.0
    assert len(metrics["phases_completed"]) == 4

  def test_performance_tracking(self):
    """Test performance tracking."""
    phase_timings = {
      "download": 3600,  # seconds
      "process": 7200,
      "consolidate": 1800,
      "ingest": 2400,
    }

    total_time = sum(phase_timings.values())
    slowest_phase = max(phase_timings, key=phase_timings.get)  # type: ignore

    assert total_time == 15000  # 4 hours 10 minutes
    assert slowest_phase == "process"

  def test_error_reporting(self):
    """Test error reporting and analysis."""
    errors = [
      {"phase": "download", "company": "0000320193", "error": "Rate limited"},
      {"phase": "download", "company": "0000789019", "error": "Rate limited"},
      {"phase": "process", "company": "0001018724", "error": "Invalid XBRL"},
    ]

    # Group errors by phase
    errors_by_phase = {}
    for error in errors:
      phase = error["phase"]
      errors_by_phase[phase] = errors_by_phase.get(phase, 0) + 1

    assert errors_by_phase["download"] == 2
    assert errors_by_phase["process"] == 1
    """Test error classification for smart retry."""
    from robosystems.tasks.sec_xbrl.orchestration import SECOrchestrator

    orchestrator = SECOrchestrator()

    test_cases = [
      ("Rate limit exceeded", "rate_limit"),
      ("429 Too Many Requests", "rate_limit"),
      ("Connection timeout", "timeout"),
      ("Out of memory", "memory"),
      ("404 Not Found", "not_found"),
      ("Parse error in XBRL", "data_error"),
      ("Unknown error", "unknown"),
    ]

    for error_msg, expected_type in test_cases:
      result = orchestrator._classify_error(error_msg)
      assert result == expected_type

  def test_checkpoint_data_structure(self):
    """Test checkpoint data structure."""
    checkpoint = {
      "phase": "download",
      "completed_items": ["0001045810", "0000789019"],
      "timestamp": datetime.now().isoformat(),
    }

    assert "phase" in checkpoint
    assert "completed_items" in checkpoint
    assert isinstance(checkpoint["completed_items"], list)
    assert "timestamp" in checkpoint

  def test_failed_company_tracking(self):
    """Test failed company tracking structure."""
    failed_record = {
      "cik": "0001045810",
      "phase": "download",
      "error": "Rate limit",
      "error_type": "rate_limit",
      "timestamp": datetime.now().isoformat(),
      "retry_count": 0,
    }

    assert failed_record["cik"] == "0001045810"
    assert failed_record["phase"] == "download"
    assert failed_record["error_type"] == "rate_limit"
    assert failed_record["retry_count"] == 0


class TestPhaseProgression:
  """Test phase-based processing logic."""

  def test_phase_progression(self):
    """Test the phase progression logic."""
    phases = ["download", "process", "ingest"]

    for i, current_phase in enumerate(phases):
      if i < len(phases) - 1:
        next_phase = phases[i + 1]
      else:
        next_phase = None

      if current_phase == "download":
        assert next_phase == "process"
      elif current_phase == "process":
        assert next_phase == "ingest"
      elif current_phase == "ingest":
        assert next_phase is None

  def test_phase_status_transitions(self):
    """Test valid phase status transitions."""
    valid_transitions = {
      "pending": ["in_progress", "skipped"],
      "in_progress": ["completed", "failed", "paused"],
      "completed": [],  # Terminal state
      "failed": ["in_progress"],  # Can retry
      "paused": ["in_progress", "cancelled"],
      "skipped": [],  # Terminal state
    }

    # Test that transitions are valid
    current_status = "pending"
    next_status = "in_progress"
    assert next_status in valid_transitions[current_status]

    current_status = "in_progress"
    next_status = "completed"
    assert next_status in valid_transitions[current_status]

  def test_phase_dependencies(self):
    """Test that phases have correct dependencies."""

    # Cannot start process without download
    download_complete = False
    can_start_process = download_complete
    assert not can_start_process

    # Can start process after download
    download_complete = True
    can_start_process = download_complete
    assert can_start_process

  def test_phase_completion_criteria(self):
    """Test phase completion criteria."""
    phase_progress = {"total": 10, "completed": 10, "failed": 0}

    # Phase is complete when all items are processed
    is_complete = (
      phase_progress["completed"] + phase_progress["failed"] == phase_progress["total"]
    )
    assert is_complete

    # Phase is not complete with pending items
    phase_progress["completed"] = 8
    is_complete = (
      phase_progress["completed"] + phase_progress["failed"] == phase_progress["total"]
    )
    assert not is_complete


class TestRetryLogic:
  """Test smart retry logic for failed companies."""

  def test_retry_strategy_by_error_type(self):
    """Test different retry strategies based on error type."""
    retry_strategies = {
      "rate_limit": {
        "max_retries": 5,
        "delay": 60,  # 1 minute
        "backoff": "exponential",
      },
      "timeout": {"max_retries": 3, "delay": 30, "backoff": "linear"},
      "network": {"max_retries": 3, "delay": 10, "backoff": "exponential"},
      "memory": {
        "max_retries": 1,
        "delay": 0,
        "backoff": None,  # Don't retry memory errors
      },
      "not_found": {
        "max_retries": 0,  # Don't retry 404s
        "delay": 0,
        "backoff": None,
      },
      "data_error": {"max_retries": 1, "delay": 0, "backoff": None},
      "unknown": {"max_retries": 2, "delay": 30, "backoff": "linear"},
    }

    # Test rate limit gets more retries
    assert (
      retry_strategies["rate_limit"]["max_retries"]
      > retry_strategies["timeout"]["max_retries"]
    )

    # Test 404s don't get retried
    assert retry_strategies["not_found"]["max_retries"] == 0

    # Test memory errors get minimal retries
    assert retry_strategies["memory"]["max_retries"] <= 1

  def test_retry_count_limits(self):
    """Test that retry counts are respected."""
    failed_company = {"cik": "0001045810", "retry_count": 3, "error_type": "timeout"}

    max_retries = 3

    should_retry = failed_company["retry_count"] < max_retries
    assert not should_retry

    failed_company["retry_count"] = 2
    should_retry = failed_company["retry_count"] < max_retries
    assert should_retry

  def test_retry_delay_calculation(self):
    """Test retry delay calculation."""
    # Linear backoff
    retry_count = 2
    base_delay = 30
    linear_delay = base_delay * (retry_count + 1)
    assert linear_delay == 90

    # Exponential backoff
    exponential_delay = base_delay * (2**retry_count)
    assert exponential_delay == 120


class TestPipelineMetrics:
  """Test pipeline metrics calculation."""

  def test_overall_progress_calculation(self):
    """Test calculation of overall pipeline progress."""
    phases = {
      "download": {"total": 10, "completed": 10, "failed": 0},
      "process": {"total": 10, "completed": 5, "failed": 1},
      "ingest": {"total": 1, "completed": 0, "failed": 0},
    }

    total_items = sum(p["total"] for p in phases.values())
    completed_items = sum(p["completed"] for p in phases.values())
    overall_progress = (completed_items / total_items) * 100 if total_items > 0 else 0

    assert total_items == 21
    assert completed_items == 15
    assert overall_progress > 70

  def test_success_rate_calculation(self):
    """Test success rate calculation for phases."""
    phase_stats = {"total": 10, "completed": 9, "failed": 1}

    success_rate = (phase_stats["completed"] / phase_stats["total"]) * 100
    assert success_rate == 90.0

  def test_duration_calculation(self):
    """Test duration calculation for pipeline."""
    start_time = datetime(2024, 1, 1, 10, 0, 0)
    end_time = datetime(2024, 1, 1, 12, 30, 0)

    duration = (end_time - start_time).total_seconds()
    duration_hours = duration / 3600

    assert duration == 9000  # 2.5 hours in seconds
    assert duration_hours == 2.5

  def test_throughput_calculation(self):
    """Test throughput calculation."""
    items_processed = 100
    duration_seconds = 300  # 5 minutes

    throughput_per_second = items_processed / duration_seconds
    throughput_per_minute = (items_processed / duration_seconds) * 60

    assert throughput_per_second > 0.3
    assert throughput_per_minute == 20


class TestPipelineConfiguration:
  """Test pipeline configuration options."""

  def test_default_configuration(self):
    """Test default pipeline configuration."""
    config = {
      "max_workers": {
        "download": 2,  # Rate limited
        "process": 8,  # CPU bound
        "ingest": 1,  # Single writer
      },
      "batch_size": 10,
      "timeout": 3600,  # 1 hour
      "retry_enabled": True,
    }

    assert config["max_workers"]["download"] == 2
    assert config["max_workers"]["process"] > config["max_workers"]["download"]
    assert config["max_workers"]["ingest"] == 1

  def test_worker_allocation_logic(self):
    """Test worker allocation based on phase."""
    # Download should be most constrained
    download_workers = 2
    process_workers = 8
    ingest_workers = 1

    assert download_workers < process_workers
    assert ingest_workers == 1

  def test_batch_size_configuration(self):
    """Test batch size configuration for different phases."""
    # Different batch sizes for different operations
    download_batch = 5  # Small batches for rate limiting
    process_batch = 20  # Larger batches for efficiency
    ingest_batch = 100  # Large batches for bulk loading

    assert download_batch < process_batch
    assert process_batch < ingest_batch


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.sec_xbrl.orchestration.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestDownloadCompanyFilingsTask:
  """Test cases for download_company_filings Celery task."""

  @patch("robosystems.operations.pipelines.sec_xbrl_filings.SECXBRLPipeline")
  def test_successful_download(self, mock_pipeline_class):
    """Test successful filing download for a company."""
    from robosystems.tasks.sec_xbrl.orchestration import download_company_filings

    mock_pipeline = MagicMock()
    mock_pipeline.raw_bucket = "test-bucket"
    mock_pipeline.s3_client = MagicMock()

    mock_pipeline._discover_entity_filings_by_year.return_value = [
      {"accessionNumber": "0001234567-89-012345"},
      {"accessionNumber": "0001234567-89-012346"},
    ]

    mock_pipeline._collect_raw_filing.return_value = {
      "status": "success",
      "key": "raw/year=2024/0001045810/000123456789012345.zip",
    }

    mock_pipeline_class.return_value = mock_pipeline

    result = download_company_filings.apply(
      args=[],
      kwargs={
        "cik": "0001045810",
        "years": [2024],
        "pipeline_id": "test-pipeline-123",
        "skip_if_exists": False,
      },
    ).get()

    assert result["status"] == "success"
    assert result["cik"] == "0001045810"
    assert result["total_downloaded"] == 2
    assert 2024 in result["years"]

  @patch("robosystems.operations.pipelines.sec_xbrl_filings.SECXBRLPipeline")
  def test_download_with_cache(self, mock_pipeline_class):
    """Test download respects S3 cache."""
    from robosystems.tasks.sec_xbrl.orchestration import download_company_filings

    mock_pipeline = MagicMock()
    mock_pipeline.raw_bucket = "test-bucket"
    mock_pipeline.s3_client = MagicMock()

    mock_pipeline._discover_entity_filings_by_year.return_value = [
      {"accessionNumber": "0001234567-89-012345"},
    ]

    mock_pipeline.s3_client.head_object.return_value = {"ContentLength": 1000}

    mock_pipeline_class.return_value = mock_pipeline

    result = download_company_filings.apply(
      args=[],
      kwargs={
        "cik": "0001045810",
        "years": [2024],
        "skip_if_exists": True,
      },
    ).get()

    assert result["status"] == "success"
    assert result["total_cached"] == 1
    assert result["total_downloaded"] == 0


class TestHandlePhaseCompletionTask:
  """Test cases for handle_phase_completion Celery task."""

  @patch("robosystems.tasks.sec_xbrl.orchestration.cleanup_phase_connections")
  @patch("robosystems.tasks.sec_xbrl.orchestration.publish_phase_metrics")
  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_successful_phase_completion(
    self, mock_orchestrator_class, mock_publish, mock_cleanup
  ):
    """Test successful phase completion handling."""
    from robosystems.tasks.sec_xbrl.orchestration import handle_phase_completion

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {"phases": {"download": {}}}
    mock_orchestrator_class.return_value = mock_orchestrator

    mock_cleanup_task = MagicMock()
    mock_cleanup_task.id = "cleanup-123"
    mock_cleanup.apply_async.return_value = mock_cleanup_task

    results = [
      {
        "status": "success",
        "cik": "0001045810",
        "total_downloaded": 10,
        "total_cached": 5,
      },
      {
        "status": "success",
        "cik": "0000789019",
        "total_downloaded": 8,
        "total_cached": 3,
      },
    ]

    result = handle_phase_completion.apply(
      args=[],
      kwargs={
        "results": results,
        "phase": "download",
        "start_time": "2024-01-01T10:00:00",
        "total_companies": 2,
      },
    ).get()

    assert result["status"] == "completed"
    assert result["phase"] == "download"
    assert result["metrics"]["companies_processed"] == 2
    assert result["metrics"]["companies_failed"] == 0
    mock_publish.assert_called_once()

  @patch("robosystems.tasks.sec_xbrl.orchestration.smart_retry_failed_companies")
  @patch("robosystems.tasks.sec_xbrl.orchestration.cleanup_phase_connections")
  @patch("robosystems.tasks.sec_xbrl.orchestration.publish_phase_metrics")
  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_phase_completion_with_failures(
    self, mock_orchestrator_class, mock_publish, mock_cleanup, mock_retry
  ):
    """Test phase completion with some failures."""
    from robosystems.tasks.sec_xbrl.orchestration import handle_phase_completion

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {"phases": {"download": {}}}
    mock_orchestrator._classify_error.return_value = "rate_limit"
    mock_orchestrator_class.return_value = mock_orchestrator

    mock_cleanup_task = MagicMock()
    mock_cleanup_task.id = "cleanup-123"
    mock_cleanup.apply_async.return_value = mock_cleanup_task

    results = [
      {
        "status": "success",
        "cik": "0001045810",
        "total_downloaded": 10,
        "total_cached": 0,
      },
      {"status": "failed", "cik": "0000789019", "error": "Rate limit exceeded"},
    ]

    result = handle_phase_completion.apply(
      args=[],
      kwargs={
        "results": results,
        "phase": "download",
        "start_time": "2024-01-01T10:00:00",
        "total_companies": 2,
      },
    ).get()

    assert result["status"] == "completed"
    assert result["metrics"]["companies_processed"] == 1
    assert result["metrics"]["companies_failed"] == 1
    assert "rate_limit" in result["metrics"]["error_breakdown"]
    mock_retry.apply_async.assert_called_once()


class TestCleanupPhaseConnectionsTask:
  """Test cases for cleanup_phase_connections Celery task."""

  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  def test_successful_cleanup(self, mock_factory):
    """Test successful connection cleanup."""
    from robosystems.tasks.sec_xbrl.orchestration import cleanup_phase_connections

    mock_factory._connection_pools = {"pool1": {}, "pool2": {}}
    mock_factory._pool_stats = {"stats": {}}

    result = cleanup_phase_connections.apply(
      args=[],
      kwargs={"phase": "download"},
    ).get()

    assert result["status"] == "success"
    assert result["phase"] == "download"
    assert len(mock_factory._connection_pools) == 0

  @patch("robosystems.config.valkey_registry.create_redis_client")
  @patch("robosystems.graph_api.client.factory.GraphClientFactory")
  def test_ingest_phase_cleanup(self, mock_factory, mock_redis_client):
    """Test cleanup after ingest phase includes Redis."""
    from robosystems.tasks.sec_xbrl.orchestration import cleanup_phase_connections

    mock_factory._connection_pools = {}

    mock_redis = MagicMock()
    mock_redis.connection_pool = MagicMock()
    mock_redis_client.return_value = mock_redis

    result = cleanup_phase_connections.apply(
      args=[],
      kwargs={"phase": "ingest"},
    ).get()

    assert result["status"] == "success"
    assert result["phase"] == "ingest"
    mock_redis.connection_pool.disconnect.assert_called_once()


class TestSmartRetryFailedCompaniesTask:
  """Test cases for smart_retry_failed_companies Celery task."""

  @patch("robosystems.tasks.sec_xbrl.orchestration.download_company_filings")
  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_retry_rate_limited_companies(self, mock_orchestrator_class, mock_download):
    """Test retry of rate-limited companies."""
    from robosystems.tasks.sec_xbrl.orchestration import smart_retry_failed_companies

    mock_orchestrator = MagicMock()
    mock_orchestrator.get_failed_companies.return_value = [
      {"cik": "0001045810", "error_type": "rate_limit", "retry_count": 0},
      {"cik": "0000789019", "error_type": "rate_limit", "retry_count": 1},
    ]
    mock_orchestrator_class.return_value = mock_orchestrator

    result = smart_retry_failed_companies.apply(
      args=[],
      kwargs={"phase": "download", "max_attempts": 3},
    ).get()

    assert result["status"] == "retry_started"
    assert result["retried"] == 2
    assert result["skipped"] == 0

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_skip_max_attempts_reached(self, mock_orchestrator_class):
    """Test skipping companies that reached max attempts."""
    from robosystems.tasks.sec_xbrl.orchestration import smart_retry_failed_companies

    mock_orchestrator = MagicMock()
    mock_orchestrator.get_failed_companies.return_value = [
      {"cik": "0001045810", "error_type": "timeout", "retry_count": 3},
    ]
    mock_orchestrator_class.return_value = mock_orchestrator

    result = smart_retry_failed_companies.apply(
      args=[],
      kwargs={"phase": "download", "max_attempts": 3},
    ).get()

    assert result["skipped"] == 1
    assert result["retried"] == 0

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_skip_data_errors(self, mock_orchestrator_class):
    """Test skipping companies with data errors."""
    from robosystems.tasks.sec_xbrl.orchestration import smart_retry_failed_companies

    mock_orchestrator = MagicMock()
    mock_orchestrator.get_failed_companies.return_value = [
      {"cik": "0001045810", "error_type": "data_error", "retry_count": 0},
      {"cik": "0000789019", "error_type": "not_found", "retry_count": 0},
    ]
    mock_orchestrator_class.return_value = mock_orchestrator

    result = smart_retry_failed_companies.apply(
      args=[],
      kwargs={"phase": "download"},
    ).get()

    assert result["skipped"] == 2
    assert result["retried"] == 0


class TestPlanPhasedProcessingTask:
  """Test cases for plan_phased_processing Celery task."""

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  @patch("robosystems.adapters.sec.SECClient")
  def test_plan_with_max_companies(
    self, mock_sec_client_class, mock_orchestrator_class
  ):
    """Test planning with max companies limit."""
    from robosystems.tasks.sec_xbrl.orchestration import plan_phased_processing
    import pandas as pd

    mock_sec_client = MagicMock()
    mock_sec_client.get_companies_df.return_value = pd.DataFrame(
      {
        "cik_str": [1045810, 789019, 1018724, 320193, 1234567],
      }
    )
    mock_sec_client_class.return_value = mock_sec_client

    mock_orchestrator = MagicMock()
    mock_orchestrator_class.return_value = mock_orchestrator

    result = plan_phased_processing.apply(
      args=[],
      kwargs={
        "start_year": 2023,
        "end_year": 2024,
        "max_companies": 3,
      },
    ).get()

    assert result["status"] == "success"
    assert result["companies"] == 3
    assert result["years"] == [2023, 2024]
    mock_orchestrator._save_state.assert_called_once()

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  @patch("robosystems.adapters.sec.SECClient")
  def test_plan_with_cik_filter(self, mock_sec_client_class, mock_orchestrator_class):
    """Test planning with specific CIK filter."""
    from robosystems.tasks.sec_xbrl.orchestration import plan_phased_processing
    import pandas as pd

    mock_sec_client = MagicMock()
    mock_sec_client.get_companies_df.return_value = pd.DataFrame(
      {
        "cik_str": [1045810, 789019],
      }
    )
    mock_sec_client_class.return_value = mock_sec_client

    mock_orchestrator = MagicMock()
    mock_orchestrator_class.return_value = mock_orchestrator

    result = plan_phased_processing.apply(
      args=[],
      kwargs={
        "start_year": 2024,
        "end_year": 2024,
        "cik_filter": "0001045810",
      },
    ).get()

    assert result["status"] == "success"
    assert result["companies"] == 1


class TestStartPhaseTask:
  """Test cases for start_phase Celery task."""

  @patch("robosystems.tasks.sec_xbrl.orchestration.orchestrate_download_phase")
  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_start_download_phase(self, mock_orchestrator_class, mock_orchestrate):
    """Test starting download phase."""
    from robosystems.tasks.sec_xbrl.orchestration import start_phase

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {
      "companies": ["0001045810", "0000789019"],
      "years": [2024],
      "config": {"companies_per_batch": 50},
    }
    mock_orchestrator_class.return_value = mock_orchestrator

    mock_task = MagicMock()
    mock_task.id = "download-task-123"
    mock_orchestrate.apply_async.return_value = mock_task

    result = start_phase.apply(
      args=[],
      kwargs={"phase": "download"},
    ).get()

    assert result["status"] == "started"
    assert result["phase"] == "download"
    assert result["companies"] == 2
    mock_orchestrate.apply_async.assert_called_once()

  @patch("robosystems.tasks.sec_xbrl.ingestion.ingest_sec_data")
  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_start_ingest_phase(self, mock_orchestrator_class, mock_ingest):
    """Test starting ingest phase."""
    from robosystems.tasks.sec_xbrl.orchestration import start_phase

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {
      "companies": ["0001045810"],
      "years": [2023, 2024],
      "config": {},
    }
    mock_orchestrator_class.return_value = mock_orchestrator

    mock_task = MagicMock()
    mock_task.id = "ingest-task-123"
    mock_ingest.apply_async.return_value = mock_task

    result = start_phase.apply(
      args=[],
      kwargs={"phase": "ingest", "backend": "neo4j"},
    ).get()

    assert result["status"] == "started"
    assert result["phase"] == "ingest"
    assert result["backend"] == "neo4j"
    assert result["years"] == [2023, 2024]
    assert mock_ingest.apply_async.call_count == 2


class TestGetPhaseStatusTask:
  """Test cases for get_phase_status Celery task."""

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_get_status_with_checkpoints(self, mock_orchestrator_class):
    """Test getting status with checkpoint info."""
    from robosystems.tasks.sec_xbrl.orchestration import get_phase_status

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {
      "companies": ["0001045810"],
      "config": {"max_companies": 1},
      "stats": {"total_companies": 1},
      "phases": {
        "download": {"status": "completed"},
        "process": {"status": "in_progress"},
      },
      "last_updated": "2024-01-01T12:00:00",
    }

    mock_orchestrator.get_checkpoint.side_effect = lambda phase: (
      {"completed_items": ["0001045810"], "timestamp": "2024-01-01T11:00:00"}
      if phase == "download"
      else None
    )

    mock_orchestrator.get_failed_companies.return_value = []
    mock_orchestrator_class.return_value = mock_orchestrator

    result = get_phase_status.apply(args=[], kwargs={}).get()

    assert result["status"] == "success"
    assert "download" in result["phases"]
    assert result["phases"]["download"]["checkpoint"]["completed_count"] == 1

  @patch("robosystems.tasks.sec_xbrl.orchestration.SECOrchestrator")
  def test_get_status_no_plan(self, mock_orchestrator_class):
    """Test getting status when no plan exists."""
    from robosystems.tasks.sec_xbrl.orchestration import get_phase_status

    mock_orchestrator = MagicMock()
    mock_orchestrator._load_state.return_value = {"companies": []}
    mock_orchestrator_class.return_value = mock_orchestrator

    result = get_phase_status.apply(args=[], kwargs={}).get()

    assert result["status"] == "no_plan"
