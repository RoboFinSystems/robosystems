"""Graph backup tracking model for PostgreSQL."""

from collections.abc import Sequence
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Optional

from sqlalchemy import (
  JSON,
  Boolean,
  Column,
  DateTime,
  Float,
  ForeignKey,
  Integer,
  String,
  Text,
  desc,
)
from sqlalchemy.orm import Session, relationship

from ...database import Model
from ...utils.ulid import generate_prefixed_ulid


class BackupStatus(str, Enum):
  """Backup status enumeration."""

  PENDING = "pending"
  IN_PROGRESS = "in_progress"
  COMPLETED = "completed"
  FAILED = "failed"
  EXPIRED = "expired"


class BackupType(str, Enum):
  """Backup type enumeration."""

  FULL = "full"
  INCREMENTAL = "incremental"


class GraphBackup(Model):
  """Model for tracking graph database backups."""

  __tablename__ = "graph_backups"

  id = Column(
    String, primary_key=True, default=lambda: generate_prefixed_ulid("backup")
  )

  # Graph identification
  graph_id = Column(String, nullable=False, index=True)
  database_name = Column(String, nullable=False, index=True)

  # Backup metadata
  backup_type = Column(String, nullable=False, default=BackupType.FULL.value)
  status = Column(
    String, nullable=False, default=BackupStatus.PENDING.value, index=True
  )

  # S3 storage information
  s3_bucket = Column(String, nullable=False)
  s3_key = Column(String, nullable=False)  # S3 object key
  s3_metadata_key = Column(String, nullable=True)  # S3 metadata object key

  # Size and compression metrics
  original_size_bytes = Column(Integer, nullable=False, default=0)
  compressed_size_bytes = Column(Integer, nullable=False, default=0)
  encrypted_size_bytes = Column(Integer, nullable=False, default=0)
  compression_ratio = Column(Float, nullable=False, default=0.0)

  # Database statistics at backup time
  node_count = Column(Integer, nullable=False, default=0)
  relationship_count = Column(Integer, nullable=False, default=0)
  database_version = Column(String, nullable=True)

  # Backup process metrics
  backup_duration_seconds = Column(Float, nullable=False, default=0.0)

  # Security and integrity
  checksum = Column(
    String, nullable=True
  )  # SHA-256 checksum (calculated after backup completion)
  encryption_enabled = Column(Boolean, nullable=False, default=True)
  compression_enabled = Column(Boolean, nullable=False, default=True)

  # Error handling
  error_message = Column(Text, nullable=True)
  retry_count = Column(Integer, nullable=False, default=0)

  # Additional metadata as JSON
  backup_metadata = Column(JSON, nullable=True)

  # Timestamps
  started_at = Column(DateTime, nullable=True)
  completed_at = Column(DateTime, nullable=True)
  expires_at = Column(DateTime, nullable=True)  # For retention management
  created_at = Column(
    DateTime, default=lambda: datetime.now(UTC), nullable=False
  )
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  # User tracking (optional)
  created_by_user_id = Column(String, ForeignKey("users.id"), nullable=True, index=True)

  # Relationships
  created_by_user = relationship("User", foreign_keys=[created_by_user_id])

  def __repr__(self) -> str:
    """String representation of the graph backup."""
    return (
      f"<GraphBackup {self.id} graph={self.graph_id} "
      f"type={self.backup_type} status={self.status}>"
    )

  @classmethod
  def create(
    cls,
    graph_id: str,
    database_name: str,
    backup_type: str,
    s3_bucket: str,
    s3_key: str,
    session: Session,
    **kwargs,
  ) -> "GraphBackup":
    """Create a new graph backup record."""
    backup = cls(
      graph_id=graph_id,
      database_name=database_name,
      backup_type=backup_type,
      s3_bucket=s3_bucket,
      s3_key=s3_key,
      **kwargs,
    )

    session.add(backup)
    session.commit()
    session.refresh(backup)
    return backup

  @classmethod
  def get_by_id(cls, backup_id: str, session: Session) -> Optional["GraphBackup"]:
    """Get a backup by ID."""
    return session.query(cls).filter(cls.id == backup_id).first()

  @classmethod
  def get_by_graph_id(
    cls,
    graph_id: str,
    session: Session,
    backup_type: str | None = None,
    status: str | None = None,
    limit: int | None = None,
    include_expired: bool = False,
  ) -> Sequence["GraphBackup"]:
    """Get backups for a specific graph."""
    query = session.query(cls).filter(cls.graph_id == graph_id)

    # Exclude expired backups by default
    if not include_expired:
      query = query.filter(cls.status != BackupStatus.EXPIRED)

    if backup_type:
      query = query.filter(cls.backup_type == backup_type)

    if status:
      query = query.filter(cls.status == status)

    query = query.order_by(cls.created_at.desc())

    if limit:
      query = query.limit(limit)

    return query.all()

  @classmethod
  def get_latest_successful(
    cls, graph_id: str, backup_type: str, session: Session
  ) -> Optional["GraphBackup"]:
    """Get the latest successful backup for a graph."""
    return (
      session.query(cls)
      .filter(
        cls.graph_id == graph_id,
        cls.backup_type == backup_type,
        cls.status == BackupStatus.COMPLETED,
      )
      .order_by(desc(cls.completed_at))
      .first()
    )

  @classmethod
  def get_pending_backups(cls, session: Session) -> list["GraphBackup"]:
    """Get all pending backups."""
    return (
      session.query(cls)
      .filter(cls.status == BackupStatus.PENDING)
      .order_by(cls.created_at.asc())
      .all()
    )

  @classmethod
  def get_expired_backups(cls, session: Session) -> Sequence["GraphBackup"]:
    """Get all expired backups."""
    current_time = datetime.now(UTC)
    return (
      session.query(cls)
      .filter(cls.expires_at.isnot(None), cls.expires_at < current_time)
      .all()
    )

  @classmethod
  def get_backup_stats(cls, graph_id: str, session: Session) -> dict[str, Any]:
    """Get backup statistics for a graph."""
    from sqlalchemy import func

    # Basic counts (exclude expired backups)
    total_backups = (
      session.query(func.count(cls.id))
      .filter(cls.graph_id == graph_id, cls.status != BackupStatus.EXPIRED)
      .scalar()
    )

    successful_backups = (
      session.query(func.count(cls.id))
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.COMPLETED)
      .scalar()
    )

    failed_backups = (
      session.query(func.count(cls.id))
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.FAILED)
      .scalar()
    )

    # Size statistics
    total_original_size = (
      session.query(func.sum(cls.original_size_bytes))
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.COMPLETED)
      .scalar()
      or 0
    )

    total_compressed_size = (
      session.query(func.sum(cls.compressed_size_bytes))
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.COMPLETED)
      .scalar()
      or 0
    )

    avg_compression_ratio = (
      session.query(func.avg(cls.compression_ratio))
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.COMPLETED)
      .scalar()
      or 0
    )

    # Latest backup info
    latest_backup = (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.status == BackupStatus.COMPLETED)
      .order_by(desc(cls.completed_at))
      .first()
    )

    return {
      "graph_id": graph_id,
      "total_backups": total_backups,
      "successful_backups": successful_backups,
      "failed_backups": failed_backups,
      "success_rate": successful_backups / total_backups if total_backups > 0 else 0,
      "total_original_size_bytes": total_original_size,
      "total_compressed_size_bytes": total_compressed_size,
      "storage_saved_bytes": total_original_size - total_compressed_size,
      "average_compression_ratio": float(avg_compression_ratio)
      if avg_compression_ratio
      else 0,
      "latest_backup_date": latest_backup.completed_at.isoformat()
      if latest_backup and latest_backup.completed_at is not None
      else None,
    }

  def start_backup(self, session: Session) -> None:
    """Mark backup as started."""
    self.status = BackupStatus.IN_PROGRESS.value
    self.started_at = datetime.now(UTC)
    self.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def complete_backup(
    self,
    session: Session,
    original_size: int,
    compressed_size: int,
    encrypted_size: int,
    checksum: str,
    node_count: int = 0,
    relationship_count: int = 0,
    backup_duration: float = 0.0,
    metadata: dict[str, Any] | None = None,
  ) -> None:
    """Mark backup as completed with metrics."""
    self.status = BackupStatus.COMPLETED.value
    self.completed_at = datetime.now(UTC)
    self.original_size_bytes = original_size
    self.compressed_size_bytes = compressed_size
    self.encrypted_size_bytes = encrypted_size
    self.compression_ratio = (
      (original_size - compressed_size) / original_size if original_size > 0 else 0
    )
    self.checksum = checksum
    self.node_count = node_count
    self.relationship_count = relationship_count
    self.backup_duration_seconds = backup_duration

    if metadata:
      self.backup_metadata = metadata

    self.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def fail_backup(self, session: Session, error_message: str) -> None:
    """Mark backup as failed with error message."""
    self.status = BackupStatus.FAILED.value
    self.error_message = error_message
    self.retry_count += 1
    self.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def expire_backup(self, session: Session) -> None:
    """Mark backup as expired."""
    self.status = BackupStatus.EXPIRED.value
    self.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def update_metadata(self, session: Session, metadata: dict[str, Any]) -> None:
    """Update backup metadata."""
    from sqlalchemy.orm.attributes import flag_modified

    if self.backup_metadata is not None:
      self.backup_metadata.update(metadata)
      # Flag the JSON column as modified so SQLAlchemy detects the change
      flag_modified(self, "backup_metadata")
    else:
      self.backup_metadata = metadata

    self.updated_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def delete(self, session: Session) -> None:
    """Delete the backup record."""
    session.delete(self)
    session.commit()

  def to_dict(self) -> dict[str, Any]:
    """Convert backup record to dictionary."""
    return {
      "id": self.id,
      "graph_id": self.graph_id,
      "database_name": self.database_name,
      "backup_type": self.backup_type,
      "status": self.status,
      "s3_bucket": self.s3_bucket,
      "s3_key": self.s3_key,
      "s3_metadata_key": self.s3_metadata_key,
      "original_size_bytes": self.original_size_bytes,
      "compressed_size_bytes": self.compressed_size_bytes,
      "encrypted_size_bytes": self.encrypted_size_bytes,
      "compression_ratio": self.compression_ratio,
      "node_count": self.node_count,
      "relationship_count": self.relationship_count,
      "database_version": self.database_version,
      "backup_duration_seconds": self.backup_duration_seconds,
      "checksum": self.checksum[:16] + "..."
      if self.checksum is not None
      else None,  # Truncate for display
      "encryption_enabled": self.encryption_enabled,
      "compression_enabled": self.compression_enabled,
      "error_message": self.error_message,
      "retry_count": self.retry_count,
      "metadata": self.backup_metadata,
      "started_at": self.started_at.isoformat()
      if self.started_at is not None
      else None,
      "completed_at": self.completed_at.isoformat()
      if self.completed_at is not None
      else None,
      "expires_at": self.expires_at.isoformat()
      if self.expires_at is not None
      else None,
      "created_at": self.created_at.isoformat(),
      "updated_at": self.updated_at.isoformat(),
      "created_by_user_id": self.created_by_user_id,
    }

  @property
  def is_completed(self) -> bool:
    """Check if backup is completed."""
    return self.status == BackupStatus.COMPLETED.value

  @property
  def is_failed(self) -> bool:
    """Check if backup is failed."""
    return self.status == BackupStatus.FAILED.value

  @property
  def is_expired(self) -> bool:
    """Check if backup is expired."""
    if self.expires_at is None:
      return False
    return datetime.now(UTC) > self.expires_at

  @property
  def storage_efficiency(self) -> float:
    """Calculate storage efficiency (compression + encryption overhead)."""
    if self.original_size_bytes == 0 or self.original_size_bytes is None:
      return 0.0
    return self.encrypted_size_bytes / self.original_size_bytes
