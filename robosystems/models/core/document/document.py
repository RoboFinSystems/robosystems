"""Document model for user graph document management.

Documents represent user-uploaded or connection-synced text content stored
in PostgreSQL as the source of truth. Content is synced to OpenSearch for
full-text and semantic search.
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Optional

from sqlalchemy import (
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
  Text,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from robosystems.database import Model
from robosystems.utils.ulid import generate_prefixed_ulid


class Document(Model):
  """User graph document with raw markdown content."""

  __tablename__ = "documents"
  __table_args__ = (
    Index("idx_documents_graph", "graph_id"),
    Index("idx_documents_user", "user_id"),
    Index("idx_documents_graph_source", "graph_id", "source_type"),
    UniqueConstraint(
      "graph_id",
      "external_id",
      name="uq_documents_graph_external_id",
    ),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("doc"))
  graph_id = Column(String, ForeignKey("graphs.graph_id"), nullable=False)
  user_id = Column(String, ForeignKey("users.id"), nullable=False)
  title = Column(String(500), nullable=False)
  content = Column(Text, nullable=False)
  tags = Column(JSONB, nullable=True)
  folder = Column(String, nullable=True)
  external_id = Column(String, nullable=True)
  source_type = Column(String, nullable=False, default="uploaded_doc")
  source_provider = Column(String, nullable=True)
  sections_indexed = Column(Integer, nullable=False, default=0)
  created_at = Column(DateTime, default=lambda: datetime.now(UTC), nullable=False)
  updated_at = Column(
    DateTime,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
    nullable=False,
  )

  def __repr__(self) -> str:
    return f"<Document {self.id} title={self.title!r} graph={self.graph_id}>"

  @classmethod
  def create(
    cls,
    graph_id: str,
    user_id: str,
    title: str,
    content: str,
    session: Session,
    tags: list[str] | None = None,
    folder: str | None = None,
    external_id: str | None = None,
    source_type: str = "uploaded_doc",
    source_provider: str | None = None,
    sections_indexed: int = 0,
  ) -> "Document":
    """Create a new document."""
    doc = cls(
      graph_id=graph_id,
      user_id=user_id,
      title=title,
      content=content,
      tags=tags,
      folder=folder,
      external_id=external_id,
      source_type=source_type,
      source_provider=source_provider,
      sections_indexed=sections_indexed,
    )
    session.add(doc)
    try:
      session.commit()
      session.refresh(doc)
    except SQLAlchemyError:
      session.rollback()
      raise
    return doc

  @classmethod
  def get_by_id(cls, document_id: str, session: Session) -> Optional["Document"]:
    """Get document by ID."""
    return session.query(cls).filter(cls.id == document_id).first()

  @classmethod
  def get_by_id_and_graph(
    cls, document_id: str, graph_id: str, session: Session
  ) -> Optional["Document"]:
    """Get document by ID with graph_id verification."""
    return (
      session.query(cls).filter(cls.id == document_id, cls.graph_id == graph_id).first()
    )

  @classmethod
  def get_by_external_id(
    cls, graph_id: str, external_id: str, session: Session
  ) -> Optional["Document"]:
    """Get document by external ID within a graph."""
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.external_id == external_id)
      .first()
    )

  @classmethod
  def get_by_graph(
    cls,
    graph_id: str,
    session: Session,
    source_type: str | None = None,
  ) -> Sequence["Document"]:
    """Get all documents for a graph, optionally filtered by source type."""
    query = session.query(cls).filter(cls.graph_id == graph_id)
    if source_type:
      query = query.filter(cls.source_type == source_type)
    return query.order_by(cls.updated_at.desc()).all()

  @classmethod
  def count_by_graph(
    cls,
    graph_id: str,
    session: Session,
    source_type: str | None = None,
  ) -> int:
    """Count documents for a graph, optionally filtered by source type."""
    query = session.query(cls).filter(cls.graph_id == graph_id)
    if source_type:
      query = query.filter(cls.source_type == source_type)
    return query.count()

  def update(
    self,
    session: Session,
    title: str | None = None,
    content: str | None = None,
    tags: list[str] | None = ...,  # type: ignore[assignment]
    folder: str | None = ...,  # type: ignore[assignment]
    sections_indexed: int | None = None,
  ) -> None:
    """Update document fields. Use sentinel (...) to distinguish None from unset."""
    if title is not None:
      self.title = title
    if content is not None:
      self.content = content
    if tags is not ...:
      self.tags = tags
    if folder is not ...:
      self.folder = folder
    if sections_indexed is not None:
      self.sections_indexed = sections_indexed
    self.updated_at = datetime.now(UTC)
    try:
      session.commit()
      session.refresh(self)
    except SQLAlchemyError:
      session.rollback()
      raise

  def delete(self, session: Session) -> None:
    """Delete the document."""
    try:
      session.delete(self)
      session.commit()
    except SQLAlchemyError:
      session.rollback()
      raise
