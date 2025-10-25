"""
Graph API Pydantic models for request/response validation.
"""

from .cluster import (
  ClusterHealthResponse,
  ClusterInfoResponse,
)
from .database import (
  QueryRequest,
  QueryResponse,
  SchemaInstallRequest,
  SchemaInstallResponse,
  BackupRequest,
  BackupResponse,
  DatabaseInfo,
  DatabaseListResponse,
  DatabaseCreateRequest,
  DatabaseCreateResponse,
)
from .ingestion import (
  CopyIngestRequest,
  IngestResponse,
  TaskStatusResponse,
)

__all__ = [
  # Cluster models
  "ClusterHealthResponse",
  "ClusterInfoResponse",
  # Database models
  "QueryRequest",
  "QueryResponse",
  "SchemaInstallRequest",
  "SchemaInstallResponse",
  "BackupRequest",
  "BackupResponse",
  "DatabaseInfo",
  "DatabaseListResponse",
  "DatabaseCreateRequest",
  "DatabaseCreateResponse",
  # Ingestion models
  "CopyIngestRequest",
  "IngestResponse",
  "TaskStatusResponse",
]
