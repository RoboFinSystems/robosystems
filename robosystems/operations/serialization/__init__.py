"""Information Block + Report serialization — interop export side.

Two encoder families share one envelope (``StatementBundle``):

* ``serialize_to_rdf(bundle, flavor=RdfFlavor.JSONLD)`` — RDF-family
  serializer. JSON-LD is the Phase 1 default; Turtle, N-Quads, RDF/XML
  slot in as additional ``RdfFlavor`` values without API change.
* ``serialize_to_xbrl(bundle, flavor=XbrlFlavor.XBRL_2_1)`` — XBRL-family
  serializer. XBRL 2.1 is the Phase 1 default; OIM flavors (xBRL-CSV,
  xBRL-JSON) slot in later.

Producers populate the bundle:

* ``build_report_bundle(session, report_id)`` — assembles a published
  Report (FactSets + Facts + framework slice + IB envelopes) into a
  mode='report' ``StatementBundle``. Stamped at publish, stored in S3.
* ``build_live_bundle`` is the future Phase-3 producer for mode='live'
  ephemeral snapshots.

See ``local/docs/specs/serialization.md`` §3a for the API contract and
``§3`` for the envelope shape.
"""

from robosystems.operations.serialization.bundle import (
  StatementBundle,
  build_report_bundle,
)
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor
from robosystems.operations.serialization.rdf import serialize_to_rdf
from robosystems.operations.serialization.xbrl import serialize_to_xbrl

__all__ = [
  "RdfFlavor",
  "StatementBundle",
  "XbrlFlavor",
  "build_report_bundle",
  "serialize_to_rdf",
  "serialize_to_xbrl",
]
