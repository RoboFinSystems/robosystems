"""RoboLedger-specific OLTP models (accounting domain).

Re-exports base ontology concepts (Taxonomy, Element, Account, Dimension,
Association, ClassificationRule, Structure) that live at the extensions
top-level but are heavily used by roboledger code. The re-exports are
permanent — they're not migration shims, they're an affirmation that
roboledger depends on base ontology concepts.

See schemas/base.py for the authoritative list of base ontology concepts
and the two invariants governing the base-vs-extension split.
"""

# Base ontology concepts (live in models/extensions/ root)
from ..association import Association
from ..classification_rule import ClassificationRule
from ..dimension import Dimension
from ..element import Account, Element
from ..structure import Structure
from ..taxonomy import Taxonomy

# RoboLedger-specific concepts
from .dimension_junctions import (
  entry_dimensions,
  line_item_dimensions,
  transaction_dimensions,
)
from .entry import Entry
from .fact import Fact
from .fiscal_calendar import FiscalCalendar, FiscalCalendarEvent
from .fiscal_period import FiscalPeriod
from .line_item import LineItem
from .publish_list import PublishList, PublishListMember
from .report import Report
from .report_share import ReportShare
from .transaction import Transaction

# Source types that represent company Chart of Accounts elements
# (as opposed to seed taxonomy elements like us-gaap, sfac6)
COA_SOURCES = ("quickbooks", "xero", "plaid", "native", "import")

__all__ = [
  "COA_SOURCES",
  "Account",
  "Association",
  "ClassificationRule",
  "Dimension",
  "Element",
  "Entry",
  "Fact",
  "FiscalCalendar",
  "FiscalCalendarEvent",
  "FiscalPeriod",
  "LineItem",
  "PublishList",
  "PublishListMember",
  "Report",
  "ReportShare",
  "Structure",
  "Taxonomy",
  "Transaction",
  "entry_dimensions",
  "line_item_dimensions",
  "transaction_dimensions",
]
