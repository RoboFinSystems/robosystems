"""
Test Suite for SEC XBRL Maintenance Tasks

Tests database reset and cleanup operations.
"""

from datetime import datetime


class TestDatabaseResetLogic:
  """Test database reset logic."""

  def test_reset_requires_confirmation(self):
    """Test that reset requires explicit confirmation."""
    confirm = False

    # Should not proceed without confirmation
    if not confirm:
      result = {"status": "cancelled", "message": "Reset not confirmed"}
    else:
      result = {"status": "success"}

    assert result["status"] == "cancelled"
    assert "not confirmed" in result["message"]

  def test_reset_workflow(self):
    """Test the reset workflow steps."""
    workflow = [
      "check_database_exists",
      "delete_database_if_exists",
      "create_database_with_schema",
      "verify_schema_applied",
    ]

    # Simulate workflow
    completed_steps = []

    for step in workflow:
      # Simulate step execution
      completed_steps.append(step)

    assert len(completed_steps) == len(workflow)
    assert completed_steps[0] == "check_database_exists"
    assert completed_steps[-1] == "verify_schema_applied"

  def test_database_configuration(self):
    """Test database configuration for reset."""
    config = {
      "db_name": "sec",
      "graph_id": "sec",
      "schema_type": "shared",
      "repository_name": "sec",
    }

    assert config["db_name"] == "sec"
    assert config["schema_type"] == "shared"
    assert config["repository_name"] == "sec"

  def test_schema_verification_logic(self):
    """Test schema verification after reset."""
    # Simulate schema queries
    node_types = ["Entity", "Report", "Fact", "Element", "Unit"]
    relationship_types = ["ENTITY_HAS_REPORT", "REPORT_HAS_FACT"]

    schema_info = {
      "node_types": len(node_types),
      "relationship_types": len(relationship_types),
    }

    assert schema_info["node_types"] == 5
    assert schema_info["relationship_types"] == 2

  def test_reset_error_handling(self):
    """Test error handling during reset."""
    errors = []

    # Simulate deletion failure
    try:
      # Mock deletion that fails
      raise Exception("Permission denied")
    except Exception as e:
      errors.append({"step": "delete_database", "error": str(e), "recoverable": True})

    assert len(errors) == 1
    assert errors[0]["step"] == "delete_database"
    assert "Permission denied" in errors[0]["error"]
    assert errors[0]["recoverable"]


class TestYearBasedCleanup:
  """Test year-based cleanup logic."""

  def test_s3_path_patterns_for_year(self):
    """Test S3 path pattern generation for a specific year."""
    year = 2024

    paths = {
      "processed": f"processed/year={year}/",
      "raw": f"raw/year={year}/",
      "consolidated": f"consolidated/year={year}/",
    }

    assert "year=2024" in paths["processed"]
    assert "year=2024" in paths["raw"]
    assert "year=2024" in paths["consolidated"]

  def test_file_filtering_by_year(self):
    """Test filtering files by year."""
    files = [
      "processed/year=2023/nodes/Entity/file1.parquet",
      "processed/year=2024/nodes/Entity/file2.parquet",
      "processed/year=2024/nodes/Report/file3.parquet",
      "processed/year=2025/nodes/Entity/file4.parquet",
    ]

    target_year = 2024

    filtered = [f for f in files if f"year={target_year}" in f]

    assert len(filtered) == 2
    assert all(f"year={target_year}" in f for f in filtered)

  def test_batch_deletion_logic(self):
    """Test batch deletion for large file sets."""
    # S3 has a limit of 1000 objects per delete request
    batch_size = 1000
    total_files = 2500

    batches = []
    for i in range(0, total_files, batch_size):
      batch_end = min(i + batch_size, total_files)
      batch_count = batch_end - i
      batches.append(batch_count)

    assert len(batches) == 3
    assert batches[0] == 1000
    assert batches[1] == 1000
    assert batches[2] == 500

  def test_dry_run_mode(self):
    """Test dry run mode doesn't delete files."""
    dry_run = True
    files_to_delete = ["file1.parquet", "file2.parquet"]

    if dry_run:
      result = {
        "status": "dry_run",
        "would_delete": len(files_to_delete),
        "files": files_to_delete,
      }
      actual_deletions = 0
    else:
      actual_deletions = len(files_to_delete)
      result = {"status": "success", "deleted": actual_deletions}

    assert result["status"] == "dry_run"
    assert actual_deletions == 0
    assert result["would_delete"] == 2

  def test_cleanup_statistics(self):
    """Test cleanup statistics calculation."""
    deleted_counts = {"processed_files": 150, "raw_files": 75, "consolidated_files": 10}

    total_deleted = sum(deleted_counts.values())

    stats = {
      "total": total_deleted,
      "by_type": deleted_counts,
      "largest_category": max(deleted_counts, key=lambda k: deleted_counts[k])
      if deleted_counts
      else None,
    }

    assert stats["total"] == 235
    assert stats["largest_category"] == "processed_files"


class TestMaintenanceConfiguration:
  """Test maintenance configuration options."""

  def test_default_settings(self):
    """Test default maintenance settings."""
    defaults = {
      "require_confirmation": True,
      "dry_run": False,
      "batch_size": 1000,
      "timeout": 3600,
    }

    assert defaults["require_confirmation"]
    assert not defaults["dry_run"]
    assert defaults["batch_size"] == 1000

  def test_safety_checks(self):
    """Test safety checks before destructive operations."""
    safety_checks = [
      "confirm_parameter",
      "database_backup_exists",
      "no_active_connections",
      "sufficient_permissions",
    ]

    # All checks must pass
    checks_passed = []

    for check in safety_checks:
      # Simulate check
      if check == "confirm_parameter":
        passed = True  # Assume confirmed
      else:
        passed = True  # Simulate other checks passing

      if passed:
        checks_passed.append(check)

    all_passed = len(checks_passed) == len(safety_checks)
    assert all_passed

  def test_cleanup_scope_validation(self):
    """Test validation of cleanup scope."""
    valid_years = list(range(2020, 2025))
    requested_year = 2024

    is_valid = requested_year in valid_years
    assert is_valid

    # Test invalid year
    requested_year = 2019
    is_valid = requested_year in valid_years
    assert not is_valid


class TestMaintenanceMonitoring:
  """Test maintenance operation monitoring."""

  def test_progress_tracking(self):
    """Test progress tracking during maintenance."""
    total_items = 1000
    processed = 0

    progress_updates = []

    for i in range(0, total_items, 100):
      processed = i
      progress = (processed / total_items) * 100
      progress_updates.append(
        {"processed": processed, "total": total_items, "percentage": progress}
      )

    assert len(progress_updates) == 10
    assert progress_updates[-1]["percentage"] == 90.0

  def test_timing_metrics(self):
    """Test timing metrics for maintenance operations."""
    start_time = datetime(2024, 1, 1, 10, 0, 0)
    end_time = datetime(2024, 1, 1, 10, 30, 0)

    duration = (end_time - start_time).total_seconds()

    metrics = {
      "start_time": start_time.isoformat(),
      "end_time": end_time.isoformat(),
      "duration_seconds": duration,
      "duration_minutes": duration / 60,
    }

    assert metrics["duration_seconds"] == 1800
    assert metrics["duration_minutes"] == 30

  def test_error_collection(self):
    """Test error collection during maintenance."""
    errors = []

    # Simulate some errors during processing
    operations = ["delete_file_1", "delete_file_2", "delete_file_3"]

    for op in operations:
      if "2" in op:  # Simulate error on file 2
        errors.append(
          {
            "operation": op,
            "error": "Access denied",
            "timestamp": datetime.now().isoformat(),
          }
        )

    assert len(errors) == 1
    assert errors[0]["operation"] == "delete_file_2"


class TestResetSecDatabaseTask:
  """Integration tests for reset_sec_database Celery task."""

  def test_reset_cancelled_without_confirmation(self):
    """Test that reset is cancelled without confirmation."""
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    result = reset_sec_database.apply(kwargs={"confirm": False}).get()

    assert result["status"] == "cancelled"
    assert "not confirmed" in result["message"]

  def test_successful_database_reset_existing_db(self):
    """Test successful database reset with existing database."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with (
      patch(
        "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService"
      ) as mock_service_class,
    ):
      # Mock graph client
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(
        return_value={"database": "sec", "exists": True}
      )
      mock_client.delete_database = AsyncMock(return_value={"status": "success"})
      mock_client.create_database = AsyncMock(
        return_value={"status": "success", "graph_id": "sec"}
      )
      mock_client.get_schema = AsyncMock(
        return_value={
          "node_tables": ["Entity", "Report", "Fact"],
          "rel_tables": ["FILED", "HAS_FACT"],
        }
      )
      mock_factory.return_value = mock_client

      # Mock repository service
      mock_service = MagicMock()
      mock_service.create_shared_repository = AsyncMock(
        return_value={
          "graph_id": "sec",
          "database_info": {"tables": ["entities", "reports", "facts"]},
        }
      )
      mock_service_class.return_value = mock_service

      result = reset_sec_database.apply(kwargs={"confirm": True}).get()

      assert result["status"] == "success"
      assert result["database"] == "sec"
      assert result["graph_id"] == "sec"
      assert result["node_types"] == 3
      assert result["relationship_types"] == 2
      assert "duration_seconds" in result

      mock_client.delete_database.assert_called_once_with("sec")
      mock_client.create_database.assert_called_once()

  def test_successful_database_reset_no_existing_db(self):
    """Test successful database reset when database doesn't exist."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with (
      patch(
        "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService"
      ) as mock_service_class,
    ):
      # Mock graph client - database doesn't exist
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=Exception("Database not found")
      )
      mock_client.create_database = AsyncMock(
        return_value={"status": "success", "graph_id": "sec"}
      )
      mock_client.get_schema = AsyncMock(
        return_value=[
          {"type": "NODE", "name": "Entity"},
          {"type": "NODE", "name": "Report"},
          {"type": "REL", "name": "FILED"},
        ]
      )
      mock_factory.return_value = mock_client

      # Mock repository service
      mock_service = MagicMock()
      mock_service.create_shared_repository = AsyncMock(
        return_value={"graph_id": "sec", "database_info": {"tables": []}}
      )
      mock_service_class.return_value = mock_service

      result = reset_sec_database.apply(kwargs={"confirm": True}).get()

      assert result["status"] == "success"
      assert result["node_types"] == 2
      assert result["relationship_types"] == 1

      # Should NOT call delete if database doesn't exist
      mock_client.delete_database.assert_not_called()
      mock_client.create_database.assert_called_once()

  def test_reset_handles_delete_error_gracefully(self):
    """Test that reset continues if database deletion fails."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with (
      patch(
        "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService"
      ) as mock_service_class,
    ):
      # Mock graph client
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(return_value={"exists": True})
      mock_client.delete_database = AsyncMock(
        side_effect=Exception("Delete permission denied")
      )
      mock_client.create_database = AsyncMock(
        return_value={"status": "success", "graph_id": "sec"}
      )
      mock_client.get_schema = AsyncMock(
        return_value={"node_tables": [], "rel_tables": []}
      )
      mock_factory.return_value = mock_client

      # Mock repository service
      mock_service = MagicMock()
      mock_service.create_shared_repository = AsyncMock(
        return_value={"graph_id": "sec", "database_info": {"tables": []}}
      )
      mock_service_class.return_value = mock_service

      result = reset_sec_database.apply(kwargs={"confirm": True}).get()

      assert result["status"] == "success"
      mock_client.create_database.assert_called_once()

  def test_reset_fails_on_create_database_error(self):
    """Test reset fails when database creation fails."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with patch(
      "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
    ) as mock_factory:
      # Mock graph client
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=Exception("Database not found")
      )
      mock_client.create_database = AsyncMock(
        return_value={"status": "failed", "error": "Creation failed"}
      )
      mock_factory.return_value = mock_client

      result = reset_sec_database.apply(kwargs={"confirm": True}).get()

      assert result["status"] == "failed"
      assert "error" in result
      assert "duration_seconds" in result

  def test_reset_with_different_backend_parameter(self):
    """Test reset accepts different backend parameter."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with (
      patch(
        "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService"
      ) as mock_service_class,
    ):
      # Mock graph client
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=Exception("Database not found")
      )
      mock_client.create_database = AsyncMock(
        return_value={"status": "success", "graph_id": "sec"}
      )
      mock_client.get_schema = AsyncMock(
        return_value={"node_tables": [], "rel_tables": []}
      )
      mock_factory.return_value = mock_client

      # Mock repository service
      mock_service = MagicMock()
      mock_service.create_shared_repository = AsyncMock(
        return_value={"graph_id": "sec", "database_info": {"tables": []}}
      )
      mock_service_class.return_value = mock_service

      result = reset_sec_database.apply(
        kwargs={"confirm": True, "backend": "neo4j"}
      ).get()

      assert result["status"] == "success"
      assert result["backend"] == "neo4j"

  def test_reset_handles_schema_verification_error(self):
    """Test reset continues if schema verification fails."""
    from unittest.mock import MagicMock, AsyncMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import reset_sec_database

    with (
      patch(
        "robosystems.tasks.sec_xbrl.maintenance.GraphClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.operations.graph.shared_repository_service.SharedRepositoryService"
      ) as mock_service_class,
    ):
      # Mock graph client
      mock_client = MagicMock()
      mock_client.get_database_info = AsyncMock(
        side_effect=Exception("Database not found")
      )
      mock_client.create_database = AsyncMock(
        return_value={"status": "success", "graph_id": "sec"}
      )
      mock_client.get_schema = AsyncMock(side_effect=Exception("Schema query failed"))
      mock_factory.return_value = mock_client

      # Mock repository service
      mock_service = MagicMock()
      mock_service.create_shared_repository = AsyncMock(
        return_value={"graph_id": "sec", "database_info": {"tables": []}}
      )
      mock_service_class.return_value = mock_service

      result = reset_sec_database.apply(kwargs={"confirm": True}).get()

      assert result["status"] == "success"
      assert result["node_types"] == 0
      assert result["relationship_types"] == 0


class TestFullResetForYearTask:
  """Integration tests for full_reset_for_year Celery task."""

  def test_full_reset_cancelled_without_confirmation(self):
    """Test that full reset is cancelled without confirmation."""
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": False}).get()

    assert result["status"] == "cancelled"
    assert "not confirmed" in result["message"]

  def test_successful_full_reset_with_s3_files(self):
    """Test successful full reset with S3 files to delete."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      mock_paginator.paginate.return_value = [
        {
          "Contents": [
            {"Key": "processed/year=2024/file1.parquet"},
            {"Key": "processed/year=2024/file2.parquet"},
          ]
        }
      ]
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset
      mock_reset.return_value = {
        "status": "success",
        "node_types": 5,
        "relationship_types": 3,
      }

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "completed"
      assert result["year"] == 2024
      assert result["s3_clear"]["status"] == "success"
      assert result["s3_clear"]["files_deleted"] == 2
      assert result["database_reset"]["status"] == "success"
      assert "duration_seconds" in result

      mock_s3.delete_objects.assert_called_once()
      mock_reset.assert_called_once_with(confirm=True, backend="ladybug")

  def test_successful_full_reset_no_s3_files(self):
    """Test successful full reset when no S3 files exist."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client - no files
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      mock_paginator.paginate.return_value = [{}]  # No Contents key
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset
      mock_reset.return_value = {"status": "success"}

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "completed"
      assert result["s3_clear"]["status"] == "success"
      assert result["s3_clear"]["files_deleted"] == 0

      mock_s3.delete_objects.assert_not_called()

  def test_full_reset_s3_error_continues_with_db_reset(self):
    """Test full reset continues with DB reset even if S3 fails."""
    from unittest.mock import patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client failure
      mock_boto3_client.side_effect = Exception("S3 connection failed")

      # Mock database reset success
      mock_reset.return_value = {"status": "success"}

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "partial_failure"
      assert result["s3_clear"]["status"] == "failed"
      assert "S3 connection failed" in result["s3_clear"]["error"]
      assert result["database_reset"]["status"] == "success"

  def test_full_reset_db_error_after_s3_success(self):
    """Test full reset handles DB error after S3 success."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client success
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "processed/year=2024/file1.parquet"}]}
      ]
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset failure
      mock_reset.return_value = {"status": "failed", "error": "DB reset failed"}

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "partial_failure"
      assert result["s3_clear"]["status"] == "success"
      assert result["database_reset"]["status"] == "failed"
      assert "DB reset failed" in result["database_reset"]["error"]

  def test_full_reset_with_large_s3_file_batch(self):
    """Test full reset handles large S3 file batches correctly."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client with 2500 files (requires 3 batches)
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      files = [{"Key": f"processed/year=2024/file{i}.parquet"} for i in range(2500)]
      mock_paginator.paginate.return_value = [{"Contents": files}]
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset
      mock_reset.return_value = {"status": "success"}

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "completed"
      assert result["s3_clear"]["files_deleted"] == 2500

      # Should call delete_objects 3 times (1000, 1000, 500)
      assert mock_s3.delete_objects.call_count == 3

  def test_full_reset_with_different_backend(self):
    """Test full reset with different backend parameter."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client - no files
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      mock_paginator.paginate.return_value = [{}]
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset
      mock_reset.return_value = {"status": "success"}

      result = full_reset_for_year.apply(
        kwargs={"year": 2024, "confirm": True, "backend": "neo4j"}
      ).get()

      assert result["status"] == "completed"
      assert result["backend"] == "neo4j"

      mock_reset.assert_called_once_with(confirm=True, backend="neo4j")

  def test_full_reset_db_exception_handling(self):
    """Test full reset handles database reset exceptions."""
    from unittest.mock import MagicMock, patch
    from robosystems.tasks.sec_xbrl.maintenance import full_reset_for_year

    with (
      patch("boto3.client") as mock_boto3_client,
      patch("robosystems.tasks.sec_xbrl.maintenance.reset_sec_database") as mock_reset,
    ):
      # Mock S3 client - no files
      mock_s3 = MagicMock()
      mock_paginator = MagicMock()
      mock_paginator.paginate.return_value = [{}]
      mock_s3.get_paginator.return_value = mock_paginator
      mock_boto3_client.return_value = mock_s3

      # Mock database reset throws exception
      mock_reset.side_effect = Exception("Database connection lost")

      result = full_reset_for_year.apply(kwargs={"year": 2024, "confirm": True}).get()

      assert result["status"] == "partial_failure"
      assert result["database_reset"]["status"] == "failed"
      assert "Database connection lost" in result["database_reset"]["error"]
