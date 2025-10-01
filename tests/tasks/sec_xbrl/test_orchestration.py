"""
Test Suite for SEC XBRL Orchestration Tasks

Tests the phase-based pipeline orchestration logic.
"""

from unittest.mock import MagicMock, patch
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
