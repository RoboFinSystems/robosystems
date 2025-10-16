"""
Copy strategies for different data sources.

This module implements the strategy pattern for handling different
copy sources (S3, URL, DataFrame, etc.) with a unified interface.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any
import uuid

from robosystems.graph_api.client import KuzuClient
from robosystems.security import SecurityAuditLogger, SecurityEventType
from robosystems.logger import logger
from robosystems.middleware.sse.event_storage import (
  get_event_storage,
  EventType,
  OperationStatus,
)
from .models import BaseCopyRequest


class CopyStrategy(ABC):
  """Abstract base class for copy strategies."""

  @abstractmethod
  async def validate(
    self, request: BaseCopyRequest, user_id: str, graph_id: str, client_ip: str
  ) -> None:
    """Validate the copy request."""
    pass

  @abstractmethod
  async def execute(
    self,
    request: BaseCopyRequest,
    kuzu_client: KuzuClient,
    graph_id: str,
    user_id: str,
    timeout_seconds: int,
  ) -> Dict[str, Any]:
    """Execute the copy operation."""
    pass

  @abstractmethod
  def get_source_type(self) -> str:
    """Get the source type identifier."""
    pass


class S3CopyStrategy(CopyStrategy):
  """Strategy for copying data from S3."""

  def get_source_type(self) -> str:
    return "s3"

  async def validate(
    self, request: BaseCopyRequest, user_id: str, graph_id: str, client_ip: str
  ) -> None:
    """Validate S3 copy request."""

    # Cast to S3CopyRequest for type safety
    s3_request = request

    # Validate credentials are provided
    if not s3_request.s3_access_key_id or not s3_request.s3_secret_access_key:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.INVALID_INPUT,
        details={
          "reason": "Missing S3 credentials",
          "user_id": user_id,
          "graph_id": graph_id,
          "client_ip": client_ip,
        },
        risk_level="medium",
      )
      raise ValueError("AWS credentials required for S3 access")

    # Basic validation of access key format
    if not s3_request.s3_access_key_id.startswith(("AKIA", "ASIA", "AIDA")):
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.INVALID_INPUT,
        details={
          "reason": "Invalid AWS access key format",
          "user_id": user_id,
          "graph_id": graph_id,
          "client_ip": client_ip,
        },
        risk_level="high",
      )
      raise ValueError("Invalid AWS access key ID format")

    # Log the validation
    logger.info(
      f"Validated S3 copy request for user {user_id} to {graph_id}.{s3_request.table_name} "
      f"from {s3_request.s3_path}"
    )

  async def execute(
    self,
    request: BaseCopyRequest,
    kuzu_client: KuzuClient,
    graph_id: str,
    user_id: str,
    timeout_seconds: int,
  ) -> Dict[str, Any]:
    """Execute S3 copy operation via Kuzu API with SSE monitoring."""

    # Cast to S3CopyRequest for type safety
    s3_request = request

    # Generate operation ID for SSE tracking
    operation_id = str(uuid.uuid4())

    # Create operation in SSE event storage
    event_storage = get_event_storage()
    await event_storage.create_operation(
      operation_id=operation_id,
      user_id=user_id,
      operation_type="copy",
      graph_id=graph_id,
      ttl=7200,  # 2 hours TTL for copy operations
    )

    # Store operation started event
    await event_storage.store_event(
      operation_id=operation_id,
      event_type=EventType.OPERATION_STARTED,
      data={
        "status": OperationStatus.RUNNING.value,
        "message": f"Starting S3 copy from {s3_request.s3_path} to {s3_request.table_name}",
        "source_type": "s3",
        "table_name": s3_request.table_name,
        "s3_path": s3_request.s3_path,
      },
    )

    try:
      # Use SSE-enabled ingestion for long-running operations
      logger.info(
        f"Starting SSE-monitored S3 COPY for user {user_id}: {s3_request.s3_path} -> {graph_id}.{s3_request.table_name}"
      )

      # Build S3 credentials for Kuzu
      s3_credentials = {
        "aws_access_key_id": s3_request.s3_access_key_id,
        "aws_secret_access_key": s3_request.s3_secret_access_key,
        "region": s3_request.s3_region,
      }

      # Add optional S3 configuration
      if s3_request.s3_session_token:
        s3_credentials["aws_session_token"] = s3_request.s3_session_token
      if s3_request.s3_endpoint:
        s3_credentials["endpoint_url"] = s3_request.s3_endpoint
      if s3_request.s3_url_style:
        s3_credentials["url_style"] = s3_request.s3_url_style

      # Note: We can't easily forward real-time progress from Kuzu's SSE to our SSE
      # because ingest_with_sse is a blocking call that monitors the entire operation.
      # Instead, we'll publish periodic progress updates if we enhance this later.

      # Store progress event before starting the blocking call
      await event_storage.store_event(
        operation_id=operation_id,
        event_type=EventType.OPERATION_PROGRESS,
        data={
          "status": OperationStatus.RUNNING.value,
          "message": "Submitting ingestion request to Kuzu database...",
          "progress_percent": 0,
        },
      )

      # Start the SSE-based ingestion (this blocks until complete)
      response = await kuzu_client.ingest_with_sse(
        graph_id=graph_id,
        table_name=s3_request.table_name,
        s3_pattern=s3_request.s3_path,
        s3_credentials=s3_credentials,
        ignore_errors=s3_request.ignore_errors,
        timeout=timeout_seconds,
      )

      # Extract results from completed ingestion
      status = response.get("status", "failed")
      records_loaded = response.get("records_loaded", 0)
      error_msg = response.get("error")
      duration_seconds = response.get("duration_seconds", 0)

      # Store completion event
      if status == "completed":
        # When using IGNORE_ERRORS=TRUE, Kuzu doesn't report row count accurately
        # We'll indicate completion without a specific count if it's 0
        if records_loaded == 0 and s3_request.ignore_errors:
          completion_message = f"Copy operation completed for {s3_request.table_name} in {duration_seconds:.1f}s"
          import_message = f"Data imported to {s3_request.table_name} (row count not available with ignore_errors option)"
        else:
          completion_message = f"Successfully loaded {records_loaded:,} records into {s3_request.table_name} in {duration_seconds:.1f}s"
          import_message = f"Successfully imported {records_loaded:,} records in {duration_seconds:.1f}s"

        await event_storage.store_event(
          operation_id=operation_id,
          event_type=EventType.OPERATION_COMPLETED,
          data={
            "status": OperationStatus.COMPLETED.value,
            "message": completion_message,
            "rows_imported": records_loaded if records_loaded > 0 else None,
            "table_name": s3_request.table_name,
            "duration_seconds": duration_seconds,
            "note": "Row count may not be accurate when using ignore_errors option"
            if records_loaded == 0 and s3_request.ignore_errors
            else None,
          },
        )

        return {
          "status": "completed",
          "operation_id": operation_id,
          "rows_imported": records_loaded if records_loaded > 0 else None,
          "message": import_message,
        }
      else:
        # Operation failed
        error_message = error_msg or "Copy operation failed"
        await event_storage.store_event(
          operation_id=operation_id,
          event_type=EventType.OPERATION_ERROR,
          data={
            "status": OperationStatus.FAILED.value,
            "error": error_message,
            "message": f"Failed to load data into {s3_request.table_name}",
          },
        )

        return {
          "status": "failed",
          "operation_id": operation_id,
          "message": error_message,
        }

    except Exception as e:
      logger.error(f"S3 copy operation failed: {e}")

      # Store error event
      await event_storage.store_event(
        operation_id=operation_id,
        event_type=EventType.OPERATION_ERROR,
        data={
          "status": OperationStatus.FAILED.value,
          "error": str(e),
          "message": f"Copy operation failed: {e}",
        },
      )

      return {
        "status": "failed",
        "operation_id": operation_id,
        "message": str(e),
      }


class URLCopyStrategy(CopyStrategy):
  """Strategy for copying data from URLs (future implementation)."""

  def get_source_type(self) -> str:
    return "url"

  async def validate(
    self, request: BaseCopyRequest, user_id: str, graph_id: str, client_ip: str
  ) -> None:
    """Validate URL copy request."""
    # TODO: Implement URL validation
    raise NotImplementedError("URL copy not yet implemented")

  async def execute(
    self,
    request: BaseCopyRequest,
    kuzu_client: KuzuClient,
    graph_id: str,
    user_id: str,
    timeout_seconds: int,
  ) -> Dict[str, Any]:
    """Execute URL copy operation."""
    # TODO: Implement URL copy
    # This would use Kuzu's httpfs extension to load from URLs
    raise NotImplementedError("URL copy not yet implemented")


class DataFrameCopyStrategy(CopyStrategy):
  """Strategy for copying data from DataFrames (future implementation)."""

  def get_source_type(self) -> str:
    return "dataframe"

  async def validate(
    self, request: BaseCopyRequest, user_id: str, graph_id: str, client_ip: str
  ) -> None:
    """Validate DataFrame copy request."""
    # TODO: Implement DataFrame validation
    raise NotImplementedError("DataFrame copy not yet implemented")

  async def execute(
    self,
    request: BaseCopyRequest,
    kuzu_client: KuzuClient,
    graph_id: str,
    user_id: str,
    timeout_seconds: int,
  ) -> Dict[str, Any]:
    """Execute DataFrame copy operation."""
    # TODO: Implement DataFrame copy
    # This would upload the DataFrame to a temporary location
    # then use COPY FROM to load it
    raise NotImplementedError("DataFrame copy not yet implemented")


class CopyStrategyFactory:
  """Factory for creating copy strategies based on source type."""

  _strategies = {
    "s3": S3CopyStrategy,
    "url": URLCopyStrategy,
    "dataframe": DataFrameCopyStrategy,
  }

  @classmethod
  def create_strategy(cls, source_type: str) -> CopyStrategy:
    """Create a copy strategy for the given source type."""
    strategy_class = cls._strategies.get(source_type)
    if not strategy_class:
      raise ValueError(f"Unknown source type: {source_type}")
    return strategy_class()

  @classmethod
  def register_strategy(cls, source_type: str, strategy_class: type[CopyStrategy]):
    """Register a new copy strategy."""
    cls._strategies[source_type] = strategy_class
