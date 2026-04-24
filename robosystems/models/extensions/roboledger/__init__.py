"""RoboLedger-specific OLTP models (accounting domain).

## Canonical import paths

**Base ontology concepts** (Taxonomy, Element, Account, Dimension,
Association, ClassificationRule, Structure) have their canonical home at
`robosystems.models.extensions.*` — that is the single source of truth
for the declaration, inheritance chain, and test coverage.

This module **re-exports** those symbols as a convenience for roboledger
code that lives under `models.extensions.roboledger.*` and naturally
reaches for neighbor imports. The re-exports are permanent: they are not
migration shims, and they will not be removed when the refactor settles.
They exist because roboledger depends heavily on base ontology concepts
and the alternative (force every roboledger file to reach back up to the
parent namespace) adds noise without value.

**New code should prefer `from robosystems.models.extensions import X`**
for base concepts. Both import paths work and will continue to work, but
the top-level path is the canonical one. Code inside the roboledger/
subpackage may use either, but should lean toward the top-level path for
files that touch base concepts and only incidentally live in roboledger.

## RoboLedger-specific models

Report, Fact, Transaction, Entry, LineItem, FiscalCalendar, FiscalPeriod,
PublishList, PublishListMember, ReportShare, and the three dimensional
junction tables (transaction_dimensions, entry_dimensions,
line_item_dimensions) are genuinely roboledger-specific and have their
canonical home here.

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
  event_dimensions,
  line_item_dimensions,
  transaction_dimensions,
)
from .entry import Entry
from .event import Event
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
  "Association",
  "ClassificationRule",
  "Dimension",
  "Element",
  "Entry",
  "Event",
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
