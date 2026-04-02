"""PublishList and PublishListMember models — named distribution lists for report sharing.

A PublishList is a named group of subscriber graph IDs that a company can share
reports to. When a ledger user shares a report, they select a publish list and
all members receive a copy.

These tables live in each tenant's extensions schema alongside report_shares,
report_definitions, etc.
"""

from datetime import UTC, datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, String, UniqueConstraint

from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class PublishList(ExtensionsBase):
  __tablename__ = "publish_lists"
  __table_args__ = (
    UniqueConstraint("name", name="uq_publish_lists_name"),
    Index("idx_publish_lists_name", "name"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("plist"))
  name = Column(String, nullable=False)
  description = Column(String, nullable=True)
  created_by = Column(String, nullable=False)
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )

  def __repr__(self) -> str:
    return f"<PublishList {self.name}>"


class PublishListMember(ExtensionsBase):
  __tablename__ = "publish_list_members"
  __table_args__ = (
    UniqueConstraint(
      "publish_list_id", "target_graph_id", name="uq_publish_list_members_pair"
    ),
    Index("idx_plm_list", "publish_list_id"),
    Index("idx_plm_target", "target_graph_id"),
  )

  id = Column(String, primary_key=True, default=lambda: generate_prefixed_ulid("plm"))
  publish_list_id = Column(
    String, ForeignKey("publish_lists.id", ondelete="CASCADE"), nullable=False
  )
  target_graph_id = Column(String, nullable=False)
  added_by = Column(String, nullable=False)
  added_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))

  def __repr__(self) -> str:
    return (
      f"<PublishListMember list={self.publish_list_id} target={self.target_graph_id}>"
    )
