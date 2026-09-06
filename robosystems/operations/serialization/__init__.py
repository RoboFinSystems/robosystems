"""Information Block + Report serialization — interop export side.

Two encoder families share one envelope (``StatementBundle``):

* ``serialize_to_rdf(bundle, flavor=RdfFlavor.JSONLD)`` — RDF-family
  serializer. JSON-LD is the default; the holon is the dataset-form
  flavor; Turtle, N-Quads, RDF/XML slot in as additional ``RdfFlavor``
  values without API change.
* ``serialize_to_xbrl(bundle, flavor=XbrlFlavor.XBRL_2_1)`` — XBRL-family
  serializer. XBRL 2.1 is the default; ``TAVI`` is the Project Tavi
  compiled model, emitted by xbrlkit through the bundle → ``XbrlModel``
  bridge (``model.py``) — the waist the remaining encoders move behind;
  further OIM flavors (xBRL-CSV, xBRL-JSON) slot in later.

Producers populate the bundle:

* ``build_report_bundle(session, report_id)`` — assembles a published
  Report (FactSets + Facts + framework slice + IB envelopes) into a
  mode='report' ``StatementBundle``. Stamped at publish, stored in S3.
* ``mode='live'`` ephemeral snapshots have no producer — the bundle
  shape supports the mode, nothing builds one.
"""

from robosystems.operations.serialization.bundle import (
  StatementBundle,
  build_report_bundle,
)
from robosystems.operations.serialization.flavors import RdfFlavor, XbrlFlavor
from robosystems.operations.serialization.model import bundle_to_xbrl_model
from robosystems.operations.serialization.rdf import serialize_to_rdf
from robosystems.operations.serialization.rdf.holon import serialize_to_holon_jsonld
from robosystems.operations.serialization.xbrl import serialize_to_xbrl
from robosystems.operations.serialization.xbrl.tavi import serialize_to_tavi

__all__ = [
  "RdfFlavor",
  "StatementBundle",
  "XbrlFlavor",
  "build_report_bundle",
  "bundle_to_xbrl_model",
  "serialize_to_holon_jsonld",
  "serialize_to_rdf",
  "serialize_to_tavi",
  "serialize_to_xbrl",
]
