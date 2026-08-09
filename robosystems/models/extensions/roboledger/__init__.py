"""RoboLedger-specific OLTP models (accounting domain).

Report, Fact, FactSet, Transaction, Entry, LineItem, FiscalCalendar,
FiscalPeriod, PublishList, PublishListMember, ReportShare, and the three
dimensional junction tables (transaction_dimensions, entry_dimensions,
line_item_dimensions) are declared here.

Base ontology concepts (Taxonomy, Element, Account, Dimension, Association,
Structure) are declared at `robosystems.models.extensions.*` and re-exported
here so roboledger code can reach them as neighbours. Prefer the top-level
path — `from robosystems.models.extensions import X` — in new code; both
work.

See schemas/base.py for the authoritative list of base ontology concepts
and the two invariants governing the base-vs-extension split.
"""

# Base ontology concepts (live in models/extensions/ root)
from ..association import Association
from ..dimension import Dimension
from ..element import Account, Element
from ..structure import Structure
from ..taxonomy import Taxonomy

# RoboLedger-specific concepts
from .agent import Agent
from .blocked_source_graph import BlockedSourceGraph
from .dimension_junctions import (
  entry_dimensions,
  event_dimensions,
  line_item_dimensions,
  transaction_dimensions,
)
from .entry import Entry
from .event import Event
from .event_handler import EventHandler
from .fact import Fact
from .fact_set import FactSet
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
  "Agent",
  "Association",
  "BlockedSourceGraph",
  "Dimension",
  "Element",
  "Entry",
  "Event",
  "EventHandler",
  "Fact",
  "FactSet",
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
  "event_dimensions",
  "line_item_dimensions",
  "transaction_dimensions",
]
