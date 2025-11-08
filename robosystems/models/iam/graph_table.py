from datetime import datetime, timezone
from typing import Optional, Dict, Any, Sequence

from sqlalchemy import (
  Column,
  String,
  DateTime,
  ForeignKey,
  Integer,
  Index,
  UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, Session

from ...database import Base
from ...utils.ulid import generate_prefixed_ulid


class GraphTable(Base):
  __tablename__ = "graph_tables"
  __table_args__ = (
    Index("idx_graph_tables_graph_id", "graph_id"),
    Index("idx_graph_tables_type", "table_type"),
    UniqueConstraint("graph_id", "table_name", name="unique_graph_table"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("gt"))
  graph_id = Column(
    String,
    ForeignKey("graphs.graph_id", ondelete="CASCADE"),
    nullable=False,
    index=True,
  )

  table_name = Column(String, nullable=False)
  table_type = Column(String, nullable=False)

  schema_json = Column(JSONB, nullable=False)
  target_node_type = Column(String, nullable=True)

  row_count = Column(Integer, default=0)
  file_count = Column(Integer, default=0)
  total_size_bytes = Column(Integer, default=0)

  created_at = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(timezone.utc),
    nullable=False,
  )
  updated_at = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(timezone.utc),
    onupdate=lambda: datetime.now(timezone.utc),
  )

  graph = relationship("Graph", backref="tables")
  files = relationship(
    "GraphFile", back_populates="table", cascade="all, delete-orphan"
  )

  def __repr__(self) -> str:
    return f"<GraphTable {self.id} graph_id={self.graph_id} name={self.table_name} type={self.table_type}>"

  @classmethod
  def create(
    cls,
    graph_id: str,
    table_name: str,
    table_type: str,
    schema_json: Dict[str, Any],
    session: Session,
    target_node_type: Optional[str] = None,
    commit: bool = True,
  ) -> "GraphTable":
    table = cls(
      graph_id=graph_id,
      table_name=table_name,
      table_type=table_type,
      schema_json=schema_json,
      target_node_type=target_node_type,
    )

    session.add(table)
    if commit:
      session.commit()
      session.refresh(table)
    return table

  @classmethod
  def get_by_name(
    cls, graph_id: str, table_name: str, session: Session
  ) -> Optional["GraphTable"]:
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.table_name == table_name)
      .first()
    )

  @classmethod
  def get_all_for_graph(cls, graph_id: str, session: Session) -> Sequence["GraphTable"]:
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id)
      .order_by(cls.table_type, cls.table_name)
      .all()
    )

  @classmethod
  def get_by_id(cls, table_id: str, session: Session) -> Optional["GraphTable"]:
    return session.query(cls).filter(cls.id == table_id).first()

  def update_stats(
    self,
    session: Session,
    row_count: Optional[int] = None,
    file_count: Optional[int] = None,
    total_size_bytes: Optional[int] = None,
  ) -> None:
    if row_count is not None:
      self.row_count = row_count
    if file_count is not None:
      self.file_count = file_count
    if total_size_bytes is not None:
      self.total_size_bytes = total_size_bytes

    self.updated_at = datetime.now(timezone.utc)
    session.commit()
    session.refresh(self)
