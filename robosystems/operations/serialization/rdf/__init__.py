"""RDF-family encoder dispatch."""

from __future__ import annotations

from robosystems.operations.serialization.bundle import StatementBundle
from robosystems.operations.serialization.flavors import RdfFlavor


def serialize_to_rdf(
  bundle: StatementBundle,
  flavor: RdfFlavor = RdfFlavor.JSONLD,
) -> str:
  if flavor is RdfFlavor.JSONLD:
    from robosystems.operations.serialization.rdf.jsonld import (
      serialize_to_jsonld,
    )

    return serialize_to_jsonld(bundle)
  if flavor is RdfFlavor.HOLON_JSONLD:
    from robosystems.operations.serialization.rdf.holon import (
      serialize_to_holon_jsonld,
    )

    return serialize_to_holon_jsonld(bundle)
  raise ValueError(f"Unsupported RDF flavor: {flavor!r}")
