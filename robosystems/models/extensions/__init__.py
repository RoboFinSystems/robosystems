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
from .association_classification import AssociationClassification
from .classification import Classification
from .classification_rule import ClassificationRule
from .dimension import Dimension
from .element import Account, Element
from .element_classification import ElementClassification
from .element_label import ElementLabel
from .element_reference import ElementReference
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
  FactSet,
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
from .rule import Rule
from .structure import Structure
from .structure_template import StructureTemplate
from .taxonomy import Taxonomy
from .verification_result import VerificationResult

__all__ = [
  # RoboLedger
  "COA_SOURCES",
  # Base ontology
  "Account",
  "Association",
  "AssociationClassification",
  "Classification",
  "ClassificationRule",
  "Dimension",
  "Element",
  "ElementClassification",
  "ElementLabel",
  "ElementReference",
  "Entity",
  "EntityTaxonomy",
  "Entry",
  "Fact",
  "FactSet",
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
  "Rule",
  "Security",
  "Structure",
  "StructureTemplate",
  "Taxonomy",
  "Transaction",
  "VerificationResult",
  "entry_dimensions",
  "line_item_dimensions",
  "transaction_dimensions",
]
