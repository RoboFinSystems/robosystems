"""Business entities; one per graph today, with ``parent_entity_id`` for a
future hierarchy. Connector-sourced rows are populated from CompanyInfo;
native rows are user-edited."""

from datetime import UTC, datetime

from sqlalchemy import (
  Boolean,
  Column,
  DateTime,
  ForeignKey,
  Index,
  Integer,
  String,
)
from sqlalchemy.dialects.postgresql import JSONB

from robosystems.config.constants import ReportingStyleConstants
from robosystems.db.extensions import ExtensionsBase
from robosystems.utils.ulid import generate_prefixed_ulid


class Entity(ExtensionsBase):
  __tablename__ = "entities"
  __table_args__ = (
    Index("idx_entities_source", "source"),
    Index("idx_entities_status", "status"),
    Index("idx_entities_parent", "parent_entity_id"),
  )

  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("ent"),
  )
  name = Column(String, nullable=False)
  legal_name = Column(String)
  uri = Column(String)

  cik = Column(String)
  ticker = Column(String)
  exchange = Column(String)
  sic = Column(String)
  sic_description = Column(String)
  category = Column(String)
  state_of_incorporation = Column(String)
  fiscal_year_end = Column(String)
  tax_id = Column(String)
  lei = Column(String)

  industry = Column(String)
  entity_type = Column(String)  # corporation, llc, partnership, subsidiary
  phone = Column(String)

  # The Reporting Style Structure this entity presents under (equity form
  # etc.). Per entity, not per graph, so subsidiaries can differ. Defaulted
  # from ``entity_type`` at creation.
  reporting_style_id = Column(
    String,
    nullable=False,
    default=ReportingStyleConstants.DEFAULT_STYLE_ID,
  )
  website = Column(String)
  status = Column(String, nullable=False, default="active")

  is_parent = Column(Boolean, nullable=False, default=True)
  parent_entity_id = Column(String, ForeignKey("entities.id"), nullable=True)

  source = Column(String, nullable=False, default="native")  # native, quickbooks, xero
  source_id = Column(String)  # realm_id for QB, org_id for Xero
  connection_id = Column(String)

  address_line1 = Column(String)
  address_city = Column(String)
  address_state = Column(String)
  address_postal_code = Column(String)
  address_country = Column(String, default="US")

  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)
