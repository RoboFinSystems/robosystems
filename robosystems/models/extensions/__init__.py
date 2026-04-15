"""Extensions OLTP models.

These models use ExtensionsBase (not the platform Base/Model) and live
in the separate 'extensions' database with schema-per-graph-id tenancy.

Top-level files mirror schemas/base.py — they are base ontology concepts
universally applicable regardless of which extension consumes them
(Entity, Taxonomy, Element, Dimension, Association, ClassificationRule,
Structure, plus the EntityTaxonomy join table). Extension-specific
models live in subfolders (roboledger/, roboinvestor/).

See schemas/base.py for the two invariants governing the base-vs-extension
split and the aspects-only-on-events rule.
"""

# Base ontology concepts
from .association import Association
from .classification_rule import ClassificationRule
from .dimension import Dimension
from .element import Account, Element
from .entity import Entity
from .entity_taxonomy import EntityTaxonomy

# RoboInvestor extension
from .roboinvestor import (
  Portfolio,
  Position,
  Security,
)

# RoboLedger extension (imports COA_SOURCES + ledger-specific models)
from .roboledger import (
  COA_SOURCES,
  Entry,
  Fact,
  FiscalCalendar,
  FiscalCalendarEvent,
  FiscalPeriod,
  LineItem,
  PublishList,
  PublishListMember,
  Report,
  ReportShare,
  Transaction,
  entry_dimensions,
  line_item_dimensions,
  transaction_dimensions,
)
from .structure import Structure
from .taxonomy import Taxonomy

__all__ = [
  # RoboLedger
  "COA_SOURCES",
  # Base ontology
  "Account",
  "Association",
  "ClassificationRule",
  "Dimension",
  "Element",
  "Entity",
  "EntityTaxonomy",
  "Entry",
  "Fact",
  "FiscalCalendar",
  "FiscalCalendarEvent",
  "FiscalPeriod",
  "LineItem",
  # RoboInvestor
  "Portfolio",
  "Position",
  "PublishList",
  "PublishListMember",
  "Report",
  "ReportShare",
  "Security",
  "Structure",
  "Taxonomy",
  "Transaction",
  "entry_dimensions",
  "line_item_dimensions",
  "transaction_dimensions",
]
