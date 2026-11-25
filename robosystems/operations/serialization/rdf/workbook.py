"""The workbook: a report's holon projected into an Excel file.

One sheet per section of the report behind a contents sheet that names the
holon by IRI and digest. A row is a line, a column a period, and a line's
breakdowns by the section's own axes sit beneath it. Every number is the
reported fact and no cell is a formula. xbrlkit writes it from the holon
alone, so it is derived from the stored artifact and never from the rows.
"""

from __future__ import annotations

from xbrlkit.holon.workbook import CONTENT_TYPE, EXTENSION, workbook_from_holon

WORKBOOK_EXTENSION = EXTENSION
WORKBOOK_CONTENT_TYPE = CONTENT_TYPE

# What the holon carries that the workbook does not. It draws every section
# with its breakdowns, so the facts it leaves out are only those dated on a
# period too sparse to get a column; its contents sheet counts them.
WORKBOOK_OMITTED_CONTENT: tuple[str, ...] = (
  "sparse_period_facts",
  "presentation_links",
  "calculation_links",
  "definition_links",
  "ib_envelopes",
  "framework_pins",
  "filing_lifecycle",
)


def serialize_holon_to_workbook(holon: str, *, holon_filename: str) -> bytes:
  """The workbook for ``holon``, naming ``holon_filename`` as where to get it."""
  return workbook_from_holon(holon, href=holon_filename)
