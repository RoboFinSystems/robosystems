from datetime import datetime, timezone
from typing import Optional, Dict, Any, Sequence

from sqlalchemy import (
  Column,
  String,
  DateTime,
  ForeignKey,
  Integer,
  Text,
  Boolean,
  Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, Session

from ...database import Base
from ...utils.ulid import generate_prefixed_ulid


class GraphSchema(Base):
  __tablename__ = "graph_schemas"
  __table_args__ = (
    Index("idx_graph_schemas_graph_id", "graph_id"),
    Index("idx_graph_schemas_active", "is_active"),
    Index("idx_graph_schemas_type", "schema_type"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("gs"))
  graph_id = Column(
    String,
    ForeignKey("graphs.graph_id", ondelete="CASCADE"),
    nullable=False,
    index=True,
  )

  schema_type = Column(String, nullable=False)
  schema_version = Column(Integer, nullable=False, default=1)

  schema_ddl = Column(Text, nullable=False)
  schema_json = Column(JSONB, nullable=True)

  custom_schema_name = Column(String, nullable=True)
  custom_schema_version = Column(String, nullable=True)

  created_at = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(timezone.utc),
    nullable=False,
  )
  is_active = Column(Boolean, default=True, nullable=False)

  graph = relationship("Graph", backref="schemas")

  def __repr__(self) -> str:
    return f"<GraphSchema {self.id} graph_id={self.graph_id} type={self.schema_type} version={self.schema_version}>"

  @classmethod
  def create(
    cls,
    graph_id: str,
    schema_type: str,
    schema_ddl: str,
    session: Session,
    schema_json: Optional[Dict[str, Any]] = None,
    schema_version: int = 1,
    custom_schema_name: Optional[str] = None,
    custom_schema_version: Optional[str] = None,
    is_active: bool = True,
    commit: bool = True,
  ) -> "GraphSchema":
    schema = cls(
      graph_id=graph_id,
      schema_type=schema_type,
      schema_ddl=schema_ddl,
      schema_json=schema_json,
      schema_version=schema_version,
      custom_schema_name=custom_schema_name,
      custom_schema_version=custom_schema_version,
      is_active=is_active,
    )

    session.add(schema)
    if commit:
      session.commit()
      session.refresh(schema)
    return schema

  @classmethod
  def get_active_schema(
    cls, graph_id: str, session: Session
  ) -> Optional["GraphSchema"]:
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id, cls.is_active.is_(True))
      .order_by(cls.schema_version.desc())
      .first()
    )

  @classmethod
  def get_all_versions(cls, graph_id: str, session: Session) -> Sequence["GraphSchema"]:
    return (
      session.query(cls)
      .filter(cls.graph_id == graph_id)
      .order_by(cls.schema_version.desc())
      .all()
    )
