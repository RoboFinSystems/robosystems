"""RDF-family encoder dispatch.

Phase 1 ships JSON-LD; additional flavors slot in by adding a module
and an ``elif`` arm here. The public ``serialize_to_rdf`` entry point
keeps a stable signature regardless of which flavor is requested.
"""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.flavors import RdfFlavor


def serialize_to_rdf(
  bundle: StatementBundle,
  flavor: RdfFlavor = RdfFlavor.JSONLD,
) -> str:
  """Serialize a ``StatementBundle`` to an RDF-family format.

  Returns a string for textual flavors (JSON-LD, Turtle, RDF/XML);
  binary flavors (none today) would return bytes via a future overload.
  """
  if flavor is RdfFlavor.JSONLD:
    from robosystems.operations.serialization.rdf.jsonld import (
      serialize_to_jsonld,
    )

    return serialize_to_jsonld(bundle)
  raise ValueError(f"Unsupported RDF flavor: {flavor!r}")
