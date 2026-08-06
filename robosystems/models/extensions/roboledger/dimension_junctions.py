"""Dimension junction tables — roboledger-specific bindings.

These many-to-many junction tables bind the base `Dimension` model (in
`models/extensions/dimension.py`) to roboledger-specific tables
(transactions, entries, line_items). They live under roboledger/ because
they reference ledger-side tables; the Dimension class itself is a base
ontology concept and lives at the extensions top level.

Each level in the three-level ledger model can carry its own dimensional
qualifiers:
- Transaction: source system, provenance dimensions
- Entry: fund, trust account, product channel dimensions
- LineItem: department, class, location, project dimensions
- Fact: report-layer aspects — scenario first (which world a number
  belongs to); segment/geography/product members as the dimensional
  spine grows
"""

from sqlalchemy import Column, ForeignKey, String, Table

from robosystems.db.extensions import ExtensionsBase

# Junction FK delete semantics (same in all three tables):
# - Parent side (transaction_id / entry_id / line_item_id): CASCADE — when
#   the owning ledger row is deleted, its dimensional tags are discarded
#   with it (the tag is meaningless without its parent).
# - Dimension side: RESTRICT — dimensions are shared reference data and
#   deleting one that is still tagged on ledger rows should be an explicit
#   operator decision, not a silent cascade that leaves orphaned ledger
#   rows without their tags.

transaction_dimensions = Table(
  "transaction_dimensions",
  ExtensionsBase.metadata,
  Column(
    "transaction_id",
    String,
    ForeignKey("transactions.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id", ondelete="RESTRICT"),
    primary_key=True,
  ),
)

entry_dimensions = Table(
  "entry_dimensions",
  ExtensionsBase.metadata,
  Column(
    "entry_id",
    String,
    ForeignKey("entries.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id", ondelete="RESTRICT"),
    primary_key=True,
  ),
)

line_item_dimensions = Table(
  "line_item_dimensions",
  ExtensionsBase.metadata,
  Column(
    "line_item_id",
    String,
    ForeignKey("line_items.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id", ondelete="RESTRICT"),
    primary_key=True,
  ),
)

event_dimensions = Table(
  "event_dimensions",
  ExtensionsBase.metadata,
  Column(
    "event_id",
    String,
    ForeignKey("events.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id", ondelete="RESTRICT"),
    primary_key=True,
  ),
)

# Fact-level aspects. Actuals stay undimensioned (the default member, in
# XBRL terms) — a row here marks a fact as belonging to an explicit member
# on some axis. Scenario is the first: forecast facts carry a `scenario`
# Dimension, so every reader honoring the documented consolidated-totals
# contract (`has_dimensions = false`) excludes them by construction.
fact_dimensions = Table(
  "fact_dimensions",
  ExtensionsBase.metadata,
  Column(
    "fact_id",
    String,
    ForeignKey("facts.id", ondelete="CASCADE"),
    primary_key=True,
  ),
  Column(
    "dimension_id",
    String,
    ForeignKey("dimensions.id", ondelete="RESTRICT"),
    primary_key=True,
  ),
)
