"""
Source File Model

Tracks raw/source files in the data lake that feed into graph processing pipelines.
These are permanent archival files (e.g., SEC XBRL filings, stock price data) that
can be processed multiple ways over time.

Unlike GraphFile which tracks processed files through staging/ingestion,
SourceFile tracks the original source data and its processing status.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import (
  BigInteger,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  Text,
)
from sqlalchemy.orm import Session, relationship

from robosystems.database import Base
from robosystems.utils.ulid import generate_prefixed_ulid


class SourceFile(Base):
  """Tracks source files in the raw data lake.

  Source files are permanent archival data (SEC filings, stock prices, etc.)
  that get processed into graph data. One source file may be processed
  multiple ways over time (e.g., XBRL extraction now, HTML semantic search later).
  """

  __tablename__ = "source_files"
  __table_args__ = (
    Index("idx_source_files_graph_id", "graph_id"),
    Index("idx_source_files_status", "status"),
    Index("idx_source_files_file_type", "file_type"),
    Index("idx_source_files_partition_key", "partition_key"),
    # Composite index for retry queries
    Index("idx_source_files_status_attempts", "status", "attempts"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("sf"))
  graph_id = Column(
    String,
    ForeignKey("graphs.graph_id", ondelete="CASCADE"),
    nullable=False,
    # Index defined in __table_args__ as idx_source_files_graph_id
  )

  # S3 location (unique per file)
  storage_key = Column(String, nullable=False, unique=True)

  # File metadata
  file_type = Column(String, nullable=False)  # xbrl_filing, stock_price, etc.
  file_size_bytes = Column(BigInteger, nullable=True)

  # Source identification (flexible for different source types)
  source_id = Column(String, nullable=True)  # accession_number, ticker, etc.

  # Processing status
  status = Column(String, nullable=False, default="pending")
  # pending: downloaded, awaiting processing
  # processing: currently being processed
  # success: processing completed successfully
  # error: processing failed

  error_reason = Column(Text, nullable=True)
  attempts = Column(Integer, nullable=False, default=0)

  # Dagster partition alignment (e.g., 2025_0001373715_0001373715-25-000309)
  partition_key = Column(String, nullable=True)

  # Timestamps
  discovered_at = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(UTC),
    nullable=False,
  )
  processed_at = Column(DateTime(timezone=True), nullable=True)
  last_attempt_at = Column(DateTime(timezone=True), nullable=True)

  # Relationships
  graph = relationship("Graph", backref="source_files")

  def __repr__(self) -> str:
    return (
      f"<SourceFile {self.id} graph_id={self.graph_id} "
      f"key={self.storage_key} status={self.status}>"
    )

  @classmethod
  def create(
    cls,
    graph_id: str,
    storage_key: str,
    file_type: str,
    session: Session,
    file_size_bytes: int | None = None,
    source_id: str | None = None,
    partition_key: str | None = None,
    status: str = "pending",
    commit: bool = True,
  ) -> "SourceFile":
    """Create a new source file record."""
    source_file = cls(
      graph_id=graph_id,
      storage_key=storage_key,
      file_type=file_type,
      file_size_bytes=file_size_bytes,
      source_id=source_id,
      partition_key=partition_key,
      status=status,
    )

    session.add(source_file)
    if commit:
      session.commit()
    else:
      session.flush()
    session.refresh(source_file)
    return source_file

  @classmethod
  def get_by_storage_key(
    cls, storage_key: str, session: Session
  ) -> Optional["SourceFile"]:
    """Get source file by S3 storage key."""
    return session.query(cls).filter(cls.storage_key == storage_key).first()

  @classmethod
  def get_or_create(
    cls,
    graph_id: str,
    storage_key: str,
    file_type: str,
    session: Session,
    file_size_bytes: int | None = None,
    source_id: str | None = None,
    partition_key: str | None = None,
    commit: bool = True,
  ) -> tuple["SourceFile", bool]:
    """Get existing source file or create new one.

    Returns:
        Tuple of (source_file, created) where created is True if new.
    """
    existing = cls.get_by_storage_key(storage_key, session)
    if existing:
      return existing, False

    source_file = cls.create(
      graph_id=graph_id,
      storage_key=storage_key,
      file_type=file_type,
      session=session,
      file_size_bytes=file_size_bytes,
      source_id=source_id,
      partition_key=partition_key,
      commit=commit,
    )
    return source_file, True

  @classmethod
  def get_by_graph_id(
    cls, graph_id: str, session: Session, status: str | None = None
  ) -> Sequence["SourceFile"]:
    """Get all source files for a graph, optionally filtered by status."""
    query = session.query(cls).filter(cls.graph_id == graph_id)
    if status:
      query = query.filter(cls.status == status)
    return query.order_by(cls.discovered_at.desc()).all()

  @classmethod
  def get_pending(
    cls,
    graph_id: str,
    session: Session,
    limit: int | None = None,
    max_attempts: int | None = None,
  ) -> Sequence["SourceFile"]:
    """Get pending source files for processing.

    Args:
        graph_id: Graph to query
        session: Database session
        limit: Maximum number of files to return
        max_attempts: Skip files with >= this many attempts (for retry limiting)
    """
    query = (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.status == "pending")
      .order_by(cls.discovered_at.asc())
    )
    if max_attempts:
      query = query.filter(cls.attempts < max_attempts)
    if limit:
      query = query.limit(limit)
    return query.all()

  @classmethod
  def get_failed(
    cls,
    graph_id: str,
    session: Session,
    max_attempts: int | None = None,
    limit: int | None = None,
  ) -> Sequence["SourceFile"]:
    """Get failed source files for retry.

    Args:
        graph_id: Graph to query
        session: Database session
        max_attempts: Only return files with fewer than this many attempts
        limit: Maximum number of files to return
    """
    query = session.query(cls).filter(cls.graph_id == graph_id, cls.status == "error")
    if max_attempts:
      query = query.filter(cls.attempts < max_attempts)
    query = query.order_by(cls.last_attempt_at.asc())
    if limit:
      query = query.limit(limit)
    return query.all()

  def mark_processing(self, session: Session) -> None:
    """Mark file as currently being processed."""
    self.status = "processing"
    self.attempts += 1
    self.last_attempt_at = datetime.now(UTC)
    session.commit()
    session.refresh(self)

  def mark_success(self, session: Session) -> None:
    """Mark file as successfully processed."""
    self.status = "success"
    self.processed_at = datetime.now(UTC)
    self.error_reason = None
    session.commit()
    session.refresh(self)

  def mark_error(self, session: Session, error_reason: str) -> None:
    """Mark file as failed with error reason."""
    self.status = "error"
    self.error_reason = error_reason
    session.commit()
    session.refresh(self)

  def reset_for_retry(self, session: Session) -> None:
    """Reset file status to pending for retry."""
    self.status = "pending"
    self.error_reason = None
    session.commit()
    session.refresh(self)
