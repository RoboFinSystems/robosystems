"""Junctions binding the base `Dimension` model to ledger rows and facts.

Each level carries its own qualifiers: transactions (source, provenance),
entries (fund, channel), line items (department, class, location, project),
facts (report-layer aspects such as scenario).
"""

from sqlalchemy import Column, ForeignKey, String, Table

from robosystems.db.extensions import ExtensionsBase

# Parent side CASCADEs (a tag is meaningless without its row); dimension side
# RESTRICTs (dimensions are shared, so deleting a used one must be deliberate).

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

# Actuals stay undimensioned (the XBRL default member). Forecast facts carry a
# `scenario` dimension, so `has_dimensions = false` readers exclude them.
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
