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
"""

from sqlalchemy import Column, ForeignKey, String, Table

from robosystems.db.extensions import ExtensionsBase

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
    ForeignKey("dimensions.id"),
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
    ForeignKey("dimensions.id"),
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
    ForeignKey("dimensions.id"),
    primary_key=True,
  ),
)
