from datetime import datetime, timezone
from typing import Optional, Sequence

from sqlalchemy import (
  Column,
  String,
  DateTime,
  ForeignKey,
  Integer,
  Index,
)
from sqlalchemy.orm import relationship, Session

from ...database import Base
from ...utils.ulid import generate_prefixed_ulid


class GraphFile(Base):
  __tablename__ = "graph_files"
  __table_args__ = (
    Index("idx_graph_files_graph_id", "graph_id"),
    Index("idx_graph_files_table_id", "table_id"),
    Index("idx_graph_files_status", "upload_status"),
    Index("idx_graph_files_duckdb_status", "duckdb_status"),
    Index("idx_graph_files_graph_status", "graph_status"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("gf"))
  graph_id = Column(
    String,
    ForeignKey("graphs.graph_id", ondelete="CASCADE"),
    nullable=False,
    index=True,
  )
  table_id = Column(
    String,
    ForeignKey("graph_tables.id", ondelete="CASCADE"),
    nullable=False,
    index=True,
  )

  file_name = Column(String, nullable=False)
  s3_key = Column(String, nullable=False)
  file_format = Column(String, nullable=False)

  file_size_bytes = Column(Integer, nullable=False)
  row_count = Column(Integer, nullable=True)

  upload_status = Column(String, nullable=False, default="pending")
  upload_method = Column(String, nullable=False)

  # Incremental ingestion: Multi-layer status tracking
  duckdb_status = Column(String, nullable=False, default="pending")
  duckdb_row_count = Column(Integer, nullable=True)
  duckdb_staged_at = Column(DateTime(timezone=True), nullable=True)

  graph_status = Column(String, nullable=False, default="pending")
  graph_ingested_at = Column(DateTime(timezone=True), nullable=True)

  # Dagster operation ID for tracking async processing (SSE streaming)
  operation_id = Column(String, nullable=True)

  created_at = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(timezone.utc),
    nullable=False,
  )
  uploaded_at = Column(DateTime(timezone=True), nullable=True)

  graph = relationship("Graph", backref="files")
  table = relationship("GraphTable", back_populates="files")

  def __repr__(self) -> str:
    return f"<GraphFile {self.id} graph_id={self.graph_id} table_id={self.table_id} name={self.file_name}>"

  @classmethod
  def create(
    cls,
    graph_id: str,
    table_id: str,
    file_name: str,
    s3_key: str,
    file_format: str,
    file_size_bytes: int,
    upload_method: str,
    session: Session,
    row_count: Optional[int] = None,
    upload_status: str = "pending",
    commit: bool = True,
  ) -> "GraphFile":
    file = cls(
      graph_id=graph_id,
      table_id=table_id,
      file_name=file_name,
      s3_key=s3_key,
      file_format=file_format,
      file_size_bytes=file_size_bytes,
      upload_method=upload_method,
      row_count=row_count,
      upload_status=upload_status,
    )

    session.add(file)
    if commit:
      session.commit()
    else:
      session.flush()
    session.refresh(file)
    return file

  @classmethod
  def get_by_id(cls, file_id: str, session: Session) -> Optional["GraphFile"]:
    return session.query(cls).filter(cls.id == file_id).first()

  @classmethod
  def get_all_for_table(cls, table_id: str, session: Session) -> Sequence["GraphFile"]:
    return session.query(cls).filter(cls.table_id == table_id).all()

  @classmethod
  def get_by_graph_id(cls, graph_id: str, session: Session) -> Sequence["GraphFile"]:
    return session.query(cls).filter(cls.graph_id == graph_id).all()

  def mark_uploaded(self, session: Session) -> None:
    self.upload_status = "completed"
    self.uploaded_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(self)

  def mark_failed(self, session: Session) -> None:
    self.upload_status = "failed"
    session.commit()
    session.refresh(self)

  def mark_duckdb_staged(
    self, session: Session, row_count: Optional[int] = None
  ) -> None:
    """Mark file as successfully staged in DuckDB."""
    self.duckdb_status = "staged"
    self.duckdb_staged_at = datetime.now(timezone.utc)
    if row_count is not None:
      self.duckdb_row_count = row_count
    session.commit()
    session.refresh(self)

  def mark_duckdb_failed(self, session: Session) -> None:
    """Mark DuckDB staging as failed."""
    self.duckdb_status = "failed"
    session.commit()
    session.refresh(self)

  def mark_graph_ingested(self, session: Session) -> None:
    """Mark file as successfully ingested to graph database."""
    self.graph_status = "ingested"
    self.graph_ingested_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(self)

  def mark_graph_failed(self, session: Session) -> None:
    """Mark graph ingestion as failed."""
    self.graph_status = "failed"
    session.commit()
    session.refresh(self)
