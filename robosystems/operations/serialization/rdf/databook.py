"""The DataBook: a report's holon projected into a document a person reads.

To the holon what inline XBRL is to XBRL: a rendering with the facts in it.
Each section is a table whose values link to their facts in the holon, with
the same facts beneath it as RDF; the arrangement, the roll-ups, the
breakdowns by axis and the ledger stay in the holon, which the document names
as its source by IRI and digest. xbrlkit writes it from the holon alone, so it
is derived from the stored artifact and never from the rows.
"""

from __future__ import annotations

from xbrlkit import __version__ as XBRLKIT_VERSION
from xbrlkit.holon.databook import databook_from_holon

DATABOOK_EXTENSION = ".databook.md"
# Where a built DataBook is kept. The writer's version is in the name, so a
# file an earlier xbrlkit wrote is not served after an upgrade: the next
# download writes a new one.
DATABOOK_CACHE_EXTENSION = f".xbrlkit-{XBRLKIT_VERSION}{DATABOOK_EXTENSION}"
DATABOOK_CONTENT_TYPE = "text/markdown; charset=utf-8"

# What the holon carries that the DataBook only points at.
DATABOOK_OMITTED_CONTENT: tuple[str, ...] = (
  "dimensional_facts",
  "presentation_links",
  "calculation_links",
  "definition_links",
  "ib_envelopes",
  "framework_pins",
  "filing_lifecycle",
)


def serialize_holon_to_databook(holon: str, *, holon_filename: str) -> str:
  """The DataBook for ``holon``, naming ``holon_filename`` as where to get it."""
  return databook_from_holon(holon, href=holon_filename)
