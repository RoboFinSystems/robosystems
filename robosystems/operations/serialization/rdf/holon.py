"""Holon serialization: the report as an RDF dataset of named graphs over one
report IRI — ``#scene`` (facts), ``#boundary`` (calculation network) and
``#projection`` (presentation network).

``#lineage`` (the ledger behind the facts) is deliberately omitted: a report is
an aggregation of the books, not the books. The partition itself is xbrlkit's
``to_holon``, fed through the ``model.py`` bridge.
"""

from __future__ import annotations

from xbrlkit.serialize import to_holon

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.model import (
  bundle_to_xbrl_model,
  report_identifier,
)


def serialize_to_holon_jsonld(bundle: StatementBundle) -> str:
  """Serialize to dataset-form JSON-LD, rooted at the same report IRI as the
  flat JSON-LD."""
  return to_holon(bundle_to_xbrl_model(bundle), report_id=report_identifier(bundle))
