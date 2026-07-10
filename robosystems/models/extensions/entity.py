"""Entity model — business entities (companies, organizations).

Maps to Entity nodes in the graph. Each entity row materializes to an
Entity node during the postgres_scanner → DuckDB → LadybugDB pipeline.

Single entity per graph for now, designed for multi-entity (parent/child)
in the future via parent_entity_id self-FK.

Dual-mode:
- OLAP (source='quickbooks'/'xero'): Populated from connector CompanyInfo
- OLTP (source='native'): User-editable entity details
"""

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

  # Identity
  id = Column(
    String,
    primary_key=True,
    default=lambda: generate_prefixed_ulid("ent"),
  )
  name = Column(String, nullable=False)
  legal_name = Column(String)
  uri = Column(String)

  # SEC/regulatory identifiers
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

  # Business info
  industry = Column(String)
  entity_type = Column(String)  # corporation, llc, partnership, subsidiary
  phone = Column(String)

  # Reporting Style — the leaf-level presentation vector (equity-form etc.)
  # this entity reports under. A Structure id in the same tenant schema
  # (``structures`` / ``reporting_style_networks``). Lives on the entity,
  # not the graph, so heterogeneous subsidiaries in a future multi-entity
  # hierarchy can each carry their own style while resolving to the same
  # canonical calc-DAG subtotals. Defaulted from ``entity_type`` at
  # creation (corporation→Default, partnership→PART, llc→LLC).
  reporting_style_id = Column(
    String,
    nullable=False,
    default=ReportingStyleConstants.DEFAULT_STYLE_ID,
  )
  website = Column(String)
  status = Column(String, nullable=False, default="active")

  # Hierarchy (future: multi-entity per graph)
  is_parent = Column(Boolean, nullable=False, default=True)
  parent_entity_id = Column(String, ForeignKey("entities.id"), nullable=True)

  # Source provenance (same pattern as accounts/transactions)
  source = Column(String, nullable=False, default="native")  # native, quickbooks, xero
  source_id = Column(String)  # realm_id for QB, org_id for Xero
  connection_id = Column(String)

  # Address (populated from connector CompanyInfo)
  address_line1 = Column(String)
  address_city = Column(String)
  address_state = Column(String)
  address_postal_code = Column(String)
  address_country = Column(String, default="US")

  # Metadata
  metadata_ = Column("metadata", JSONB, nullable=False, default=dict)
  version = Column(Integer, nullable=False, default=1)

  # Timestamps
  created_at = Column(DateTime, nullable=False, default=lambda: datetime.now(UTC))
  updated_at = Column(
    DateTime,
    nullable=False,
    default=lambda: datetime.now(UTC),
    onupdate=lambda: datetime.now(UTC),
  )
  created_by = Column(String, nullable=False)
