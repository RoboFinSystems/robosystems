"""Holon serialization — the report as scene/boundary/projection named graphs.

A *holon* is a report expressed as an RDF **dataset**: three named graphs over
one report IRI —

* ``<report>#scene`` — the instance facts (values this report reports) plus the
  Information Block that groups them and the element/period/unit/entity/factSet
  those facts reference.
* ``<report>#boundary`` — the calculation network: the roll-up rules the facts
  must obey.
* ``<report>#projection`` — the presentation network: order, indentation,
  subtotals (structures + presentation arcs), with the elements those arcs
  reach so every row carries its label whether or not it reports a value.

The fourth graph, ``#lineage`` (the ledger behind the facts), is intentionally
*absent* from the published holon — a report is an aggregation of the books, not
the books. The access boundary holds by graph omission, no filter code.

The holon is a **shape, not a file format**. JSON-LD's data model *is* an RDF
dataset, so dataset-form JSON-LD carries these named graphs natively — the
single canonical, API-native holon.

The partition lives in xbrlkit (``xbrlkit.serialize.holon.to_holon``), the
writer behind the SEC pipeline's per-filing holons. This module is the
platform's flavor entry: it re-expresses the ``StatementBundle`` as an
``XbrlModel`` through the bridge in ``model.py`` — the waist the Tavi flavor
already reads through — and hands it to that writer. A platform-side copy of
the partition used to live here; it fell behind the kernel it was forked from
and seeded the scene from facts alone, so every element reached only by an arc
(an empty statement line, an abstract header) left the holon without its
declaration and rendered unlabelled. One partition, one repo: the demo
DataBook converter (``examples/_common/databook.py``) reads the flat JSON-LD
back through xbrlkit's reader and calls the same writer.
"""

from __future__ import annotations

from xbrlkit.serialize import to_holon

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.model import (
  bundle_to_xbrl_model,
  report_identifier,
)


def serialize_to_holon_jsonld(bundle: StatementBundle) -> str:
  """Serialize a ``StatementBundle`` to the canonical dataset-form JSON-LD holon.

  The report IRI the named graphs hang off is
  ``https://robosystems.ai/report/{report_id}`` — the root the flat JSON-LD
  stamps for the same report — so a DataBook's ``graph:`` map and the holon
  viewer address ``#scene`` / ``#boundary`` / ``#projection`` identically
  whichever path produced the file.
  """
  return to_holon(bundle_to_xbrl_model(bundle), report_id=report_identifier(bundle))
